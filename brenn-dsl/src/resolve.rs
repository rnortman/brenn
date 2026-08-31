//! The resolver: from a root document to a [`ResolvedConfig`].
//!
//! Five passes, in a fixed order, all pure — state is threaded, never global:
//!
//! 1. **Load** — follow `use` statements from the root file, parsing each module
//!    once. A missing module, an import cycle or any parse failure ends the
//!    compile here: resolution needs every model present.
//! 2. **Index** — per file, the top-level symbol table, then imports applied
//!    into it. No shadowing anywhere: a name means one thing in a file, and a
//!    collision is a two-site error.
//! 3. **Constants** — constants are leaves, so they resolve before anything can
//!    reference them. Escapes decode here.
//! 4. **Expand** — instantiate classes and assemblies.
//! 5. **Check** — everything that needs the expanded whole.
//!
//! Diagnostics accumulate rather than stopping at the first: independent errors
//! in one document are all reported.

use brenn_envelope::addressing::is_unreserved_name;
use brenn_envelope::grants::ComponentGrant;

use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::ops::{Deref, DerefMut};
use std::path::{Path, PathBuf};
use std::rc::Rc;

use fltk_cst_core::Span;
use fltk_serde_core::Spanned;

use crate::derived::DerivedConfig;
use crate::diag::{Diagnostic, check_unique, duplicate_statement, or_list, two_site};
use crate::model::{
    AclStmt, AgentBlock, AgentClass, Arg, ArgList, AssemblyDef, AssemblyItem,
    AttachmentTargetAttrs, Attr, AttrBlock, AttrMap, Binding as BindStmt, ChanAddr, ChanRef,
    ChannelAttrs, ChannelDef, ComponentClass, ConstDef, FStrPart, File, GrantStmt, InTail,
    InlineTable, InstBody, IntOrWord, IoTail, Item, LinkStmt, MapValues, Matcher, MatcherVal,
    McpServerStmt, MountStmt, MountTail, NamedAttrDef, NewStmt, OpenAttrs, OutTail, Param,
    ParamList, PathRef, PathSeg, PortDir as DeclDir, RateLimitAttrs, SectionNode, StrLike, StrLit,
    StrPart, SubscribeStmt, SubscribeTail, SurfaceDef, ToolBlock, TypedBlock, UseStmt, UuidPin,
    Value, WordList,
};
use crate::resolved::scheme::{spellable_list, split_spellable};
use crate::resolved::{
    Abi, ChanId, ClassRef, HandlePath, LinkId, MatcherKind, PortDir, RAcl, RAgent,
    RAttachmentTarget, RBinding, RChanRef, RChannel, RComponentInst, RConsumer, RGrant, RHooks,
    RLink, RMatcher, RMatcherVal, RMcp, RMount, RNamed, RPin, RPort, RRateLimit, RRemote, RSection,
    RSubscribe, RSurface, RTail, RToolGrant, RTuning, RVal, RValue, RWebhook, RWebhookBlock,
    RWordList, ResolvedConfig, str_value,
};

/// The module key of the root file: the crate root has no path to name it by.
const ROOT_KEY: &str = "";

/// The extension a module file takes.
const MODULE_EXT: &str = "brenn";

/// What leads a packaged module's key, and the sigil that spells it in source.
///
/// A tree module key is `::`-joined path segments, and the grammar admits no
/// `@` in a name, so no tree import can ever produce a key in this namespace.
const PKG_SIGIL: &str = "@";

/// Compile a document tree, starting from its root file.
///
/// The root file's directory is the tree root: `use wiring::deskbar;` reads
/// `<root_dir>/wiring/deskbar.brenn`. `module_root`, where one is supplied, is
/// the flat directory packaged-module imports resolve against:
/// `use @deskbar::*;` reads `<module_root>/deskbar.brenn`. A document with no
/// packaged-module import is unaffected by it.
pub fn compile(root: &Path, module_root: Option<&Path>) -> Result<DerivedConfig, Vec<Diagnostic>> {
    let files = load(root, module_root)?;
    let config = resolve_files(files, ROOT_KEY)?;
    crate::derive::derive(config)
}

/// Resolve an already-loaded set of modules — the testable core, no I/O.
///
/// Each entry is a module key (`""` for the root, `"wiring::deskbar"` for a
/// module) and the file it parsed to. `root` names which key is the root.
///
/// `root` is validated to name one of the modules and does nothing else.
/// Expansion does not walk from it: every loaded module's top-level `new` is
/// instantiated, wherever it was written, because a module is loaded only by
/// being imported and everything a document reaches is part of it.
pub fn resolve_files(
    files: Vec<(String, File)>,
    root: &str,
) -> Result<ResolvedConfig, Vec<Diagnostic>> {
    assert!(
        files.iter().any(|(key, _)| key == root),
        "the root key names one of the files"
    );
    let mut errors = Vec::new();
    check_packaged_discipline(&files, &mut errors);
    let mut index = Index::build(&files, &mut errors);
    index.resolve_constants(&files, &mut errors);
    if !errors.is_empty() {
        return Err(errors);
    }
    let Emitted { config, withheld } = emit_entities(&index, files, &mut errors);
    check_identity(&config, &mut errors);
    check_grants(&config, &withheld, &mut errors);
    check_addresses(&config, &mut errors);
    if !errors.is_empty() {
        return Err(errors);
    }
    Ok(config)
}

// ── the packaged-module discipline ───────────────────────────────────────────

/// What a packaged module is refused with when it carries anything else.
const DISCIPLINE_REFUSAL: &str = "a packaged module declares vocabulary — component classes, \
     assemblies, constants — and instantiates nothing";

/// Refuse anything effectful in a module whose author is not the deployer.
///
/// Loading a module instantiates its top-level `new` statements and effects its
/// top-level channels, wherever it was written. That is right for a module the
/// deployer wrote and wrong for one that shipped inside a component package: an
/// import would otherwise inject instances, channels or grants into a document
/// whose author consented to none of them. So a packaged module may declare
/// component classes, assemblies and constants — vocabulary, which stamps
/// nothing until someone writes `new` against it — and nothing else.
///
/// Its `use` statements are held to the same line: a packaged module may build
/// on other packaged modules, but can know nothing of any deployment's tree.
///
/// The walk stops at the item level on purpose: an assembly body may declare
/// grants and a surface, and this pass does not look inside one. Nothing in an
/// assembly happens until someone writes `new` against it, and that `new` is
/// the consent — a deployer who stamps a packaged assembly accepts the whole
/// arrangement it declares, authority included. Refusing the same forms one
/// level down would ban shipping any arrangement that wires its own parameters.
///
/// Runs in the I/O-free core rather than in the loader so the in-memory path
/// exercises it identically.
fn check_packaged_discipline(files: &[(String, File)], errors: &mut Vec<Diagnostic>) {
    for (key, file) in files {
        if !is_packaged(key) {
            continue;
        }
        for stmt in &file.uses {
            if !stmt.pkg {
                errors.push(Diagnostic::at(
                    "a packaged module imports only packaged modules: `use @<module>::<Item>;`",
                    stmt.path.head.span().clone(),
                ));
            }
        }
        for item in &file.items {
            match item.value() {
                Item::ConstDef(_) | Item::Component(_) | Item::Assembly(_) => {}
                _ => errors.push(Diagnostic::at(DISCIPLINE_REFUSAL, item.span().clone())),
            }
        }
    }
}

// ── pass 1: load ─────────────────────────────────────────────────────────────

/// Parse the root file and, transitively, every module it reaches.
///
/// Returns the modules in the order they were reached.
fn load(root: &Path, module_root: Option<&Path>) -> Result<Vec<(String, File)>, Vec<Diagnostic>> {
    let root_dir = root.parent().unwrap_or(Path::new(".")).to_path_buf();
    // An operator typo must not pass silently, so the module root is checked
    // for what it claims to be whether or not the document reaches for it. The
    // probe is a read, not a stat: a directory nothing may list answers every
    // import with an absence, and "no packaged module" sends its reader hunting
    // the release staging for a file that is sitting right there.
    if let Some(dir) = module_root
        && let Err(error) = std::fs::read_dir(dir)
    {
        return Err(vec![Diagnostic::unpositioned(
            format!(
                "--modules {}: not a readable directory: {error}",
                dir.display()
            ),
            root.display().to_string(),
        )]);
    }
    let mut loader = Loader {
        root_dir,
        module_root: module_root.map(Path::to_path_buf),
        reported_missing_module_root: false,
        files: Vec::new(),
        seen: HashMap::new(),
        loaded: HashMap::new(),
        errors: Vec::new(),
    };
    loader.visit(ROOT_KEY.to_string(), root.to_path_buf(), &mut Vec::new());
    if !loader.errors.is_empty() {
        return Err(loader.errors);
    }
    Ok(loader.files)
}

struct Loader {
    root_dir: PathBuf,
    /// Where `@` imports resolve, where the caller supplied one. Absent is not
    /// a default: a document that reaches for a packaged module without one is
    /// refused naming the flag.
    module_root: Option<PathBuf>,
    /// Whether the absent module root has already been reported. It is one
    /// fact about the invocation, not about any module, so a document importing
    /// nine packaged modules gets one sentence naming the flag.
    reported_missing_module_root: bool,
    /// Modules in the order they were reached, root first.
    files: Vec<(String, File)>,
    /// Module key to the path it was read from — also the "already visited" set.
    seen: HashMap<String, PathBuf>,
    /// Canonical path to the module key it was first loaded as. One file is one
    /// module: the same file reached under a second key would index every
    /// declaration in it twice.
    loaded: HashMap<PathBuf, String>,
    errors: Vec<Diagnostic>,
}

impl Loader {
    /// Parse one module and descend into what it imports.
    ///
    /// `stack` is the chain of module keys currently being visited, which is
    /// what makes a cycle nameable when the closing edge is found.
    fn visit(&mut self, key: String, path: PathBuf, stack: &mut Vec<String>) {
        let file = match crate::parse_file(&path) {
            Ok(file) => file,
            Err(error) => {
                self.errors.push(error);
                // Record it anyway: a second `use` of a file that failed to
                // parse should not report the same failure twice.
                self.seen.insert(key, path);
                return;
            }
        };
        if let Ok(canonical) = path.canonicalize() {
            self.loaded.insert(canonical, key.clone());
        }
        self.seen.insert(key.clone(), path);
        // A packaged module's tree imports are refused by the discipline pass;
        // following one here would report a missing module in front of the
        // refusal that is the real answer.
        let packaged = is_packaged(&key);
        let imports: Vec<(String, Span)> = file
            .uses
            .iter()
            .filter(|stmt| !packaged || stmt.pkg)
            .filter_map(|stmt| use_target(stmt).and_then(Result::ok))
            .map(|target| (target.module, target.span))
            .collect();
        self.files.push((key.clone(), file));

        stack.push(key);
        for (module, span) in imports {
            if let Some(position) = stack.iter().position(|member| *member == module) {
                self.errors
                    .push(cycle_error(&module, &stack[position..], span));
                continue;
            }
            if self.seen.contains_key(&module) {
                continue;
            }
            let path = match self.module_path(&module) {
                Some(path) => path,
                None => {
                    if !self.reported_missing_module_root {
                        self.reported_missing_module_root = true;
                        self.errors.push(Diagnostic::at(
                            "this document imports packaged modules; pass `--modules <dir>`",
                            span,
                        ));
                    }
                    // Poison it under a path that cannot exist, so a second
                    // `use` of this module is not a second walk of it.
                    self.seen.insert(module, PathBuf::new());
                    continue;
                }
            };
            if !path.is_file() {
                self.errors
                    .push(Diagnostic::at(self.missing_module(&module, &path), span));
                // Poison it so a second `use` of the same missing module is not
                // a second report of the same absence.
                self.seen.insert(module, path);
                continue;
            }
            // The same file under a second module key would be parsed and
            // indexed twice, and every declaration in it would exist twice in
            // the resolved config. Reaching the root file by name is the way
            // this happens.
            if let Ok(canonical) = path.canonicalize()
                && let Some(first) = self.loaded.get(&canonical)
            {
                self.errors.push(Diagnostic::at(
                    format!(
                        "`{}` is already loaded as {}: one file is one module",
                        module_label(&module),
                        module_label(first)
                    ),
                    span,
                ));
                self.seen.insert(module, path);
                continue;
            }
            self.visit(module, path, stack);
        }
        stack.pop();
    }

    /// Where a module key reads from, or nothing where it names a packaged
    /// module and no module root was supplied.
    ///
    /// A tree key is its segments under the root directory, plus the extension.
    /// The grammar admits neither `..` nor an absolute head, so a tree module
    /// path cannot escape the root by construction. A packaged key is its one
    /// name directly under the module root, which is flat.
    fn module_path(&self, key: &str) -> Option<PathBuf> {
        let mut path = if is_packaged(key) {
            self.module_root.clone()?
        } else {
            self.root_dir.clone()
        };
        for segment in module_name(key).split("::") {
            path.push(segment);
        }
        path.set_extension(MODULE_EXT);
        Some(path)
    }

    /// What a module that is not on disk is reported as.
    ///
    /// A tree module is named against the root file's directory, which the
    /// reader has in front of them. A packaged module's root is an environment
    /// fact the document does not state, so the message spells it out whole:
    /// "under which root" is the question the reader is actually asking.
    fn missing_module(&self, key: &str, path: &Path) -> String {
        if is_packaged(key) {
            format!(
                "no packaged module `{}`: expected `{}`",
                module_name(key),
                path.display()
            )
        } else {
            format!(
                "no module `{key}`: expected `{}`",
                display_relative(path, &self.root_dir)
            )
        }
    }
}

fn is_packaged(key: &str) -> bool {
    key.starts_with(PKG_SIGIL)
}

fn module_name(key: &str) -> &str {
    key.strip_prefix(PKG_SIGIL).unwrap_or(key)
}

struct UseTarget {
    module: String,
    span: Span,
    item: Option<String>,
}

/// What a `use` names, or why it names nothing.
///
/// `None` is a path that names no module at all, and `Err` is a packaged import
/// written deeper than the one level a module root has. Both are reported by the
/// index pass, which is the one pass every entry point runs; the loader reads
/// this too and stays silent about them so neither is reported twice.
fn use_target(stmt: &UseStmt) -> Option<Result<UseTarget, Diagnostic>> {
    let span = stmt.path.head.span().clone();
    let mut segments = module_segments(stmt)?;
    let item = if stmt.glob {
        None
    } else {
        Some(segments.pop().expect("a named use has two segments"))
    };
    if stmt.pkg && segments.len() != 1 {
        return Some(Err(Diagnostic::at(
            "a packaged module is one level: `use @<module>::<Item>;`",
            span,
        )));
    }
    let module = if stmt.pkg {
        format!("{PKG_SIGIL}{}", segments.join("::"))
    } else {
        segments.join("::")
    };
    Some(Ok(UseTarget { module, span, item }))
}

/// A path as written against the root directory, for a message a reader can act
/// on without knowing where the tree lives.
fn display_relative(path: &Path, root_dir: &Path) -> String {
    path.strip_prefix(root_dir)
        .unwrap_or(path)
        .display()
        .to_string()
}

/// The closing edge of an import cycle, with every member named.
fn cycle_error(module: &str, members: &[String], span: Span) -> Diagnostic {
    let chain: Vec<&str> = members
        .iter()
        .map(|member| module_label(member))
        .chain(std::iter::once(module_label(module)))
        .collect();
    Diagnostic::at(format!("import cycle: {}", chain.join(" -> ")), span)
}

/// What a module key is called in a message; the root has no path to name.
fn module_label(key: &str) -> &str {
    if key.is_empty() { "<root>" } else { key }
}

// ── pass 2: index ────────────────────────────────────────────────────────────

/// What a top-level name declares.
///
/// The kind is what a diagnostic says when a name is used where its kind is not
/// legal, and what the expand pass dispatches an instantiation on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SymKind {
    Const,
    ComponentClass,
    AgentClass,
    Assembly,
    Channel,
    Link,
    Surface,
    Instance,
    Remote,
    Webhook,
    Repo,
    MqttClient,
    McpServer,
}

impl SymKind {
    /// What this is called in a diagnostic.
    pub fn describe(self) -> &'static str {
        match self {
            SymKind::Const => "a constant",
            SymKind::ComponentClass => "a component class",
            SymKind::AgentClass => "an agent class",
            SymKind::Assembly => "an assembly",
            SymKind::Channel => "a channel",
            SymKind::Link => "a link",
            SymKind::Surface => "a surface",
            SymKind::Instance => "an instance",
            SymKind::Remote => "a remote",
            SymKind::Webhook => "a webhook",
            SymKind::Repo => "a repo",
            SymKind::MqttClient => "an mqtt client",
            SymKind::McpServer => "an mcp server",
        }
    }
}

/// One top-level declaration, found by name.
#[derive(Debug, Clone)]
struct Symbol {
    kind: SymKind,
    /// Which file declared it, and which item of that file it is.
    file: usize,
    item: usize,
    /// Where the declaration's name was written.
    span: Span,
    /// The class a `new` names, for the kinds of instance only its class tells
    /// apart. `None` on everything that is not an instantiation.
    class: Option<PathRef>,
}

/// A name in a file's scope, and where it entered that scope: its own
/// declaration, or the `use` that imported it.
#[derive(Debug, Clone)]
struct Binding {
    symbol: Symbol,
    site: Span,
    imported: bool,
}

/// One file's top-level scope, plus what its constants resolved to.
struct FileIndex {
    /// The module key, for a diagnostic that has to name the file a symbol was
    /// looked for in.
    key: String,
    /// Names declared here, before imports. A glob exports these and only
    /// these: an import does not re-export.
    locals: HashMap<String, Symbol>,
    /// Everything the file can name: its locals plus its imports.
    scope: HashMap<String, Binding>,
    consts: HashMap<String, RVal>,
}

/// Every file's scope, addressable by module key.
struct Index {
    files: Vec<FileIndex>,
    by_key: HashMap<String, usize>,
}

impl Index {
    /// Build every file's local table, then apply imports into it.
    fn build(files: &[(String, File)], errors: &mut Vec<Diagnostic>) -> Index {
        let mut index = Index {
            files: Vec::new(),
            by_key: HashMap::new(),
        };
        for (position, (key, file)) in files.iter().enumerate() {
            index.by_key.insert(key.clone(), position);
            index.files.push(FileIndex {
                key: key.clone(),
                locals: locals_of(file, position, errors),
                scope: HashMap::new(),
                consts: HashMap::new(),
            });
        }
        for (position, (_, file)) in files.iter().enumerate() {
            let mut scope: HashMap<String, Binding> = index.files[position]
                .locals
                .iter()
                .map(|(name, symbol)| {
                    (
                        name.clone(),
                        Binding {
                            site: symbol.span.clone(),
                            symbol: symbol.clone(),
                            imported: false,
                        },
                    )
                })
                .collect();
            index.apply_imports(position, file, &mut scope, errors);
            index.files[position].scope = scope;
        }
        for (position, (_, file)) in files.iter().enumerate() {
            index.check_params(position, file, errors);
            check_bodies(file, errors);
        }
        index
    }

    /// Bring each `use` statement's names into one file's scope.
    fn apply_imports(
        &self,
        position: usize,
        file: &File,
        scope: &mut HashMap<String, Binding>,
        errors: &mut Vec<Diagnostic>,
    ) {
        for stmt in &file.uses {
            let target = match use_target(stmt) {
                None => {
                    errors.push(use_shape_error(stmt));
                    continue;
                }
                Some(Err(error)) => {
                    errors.push(error);
                    continue;
                }
                Some(Ok(target)) => target,
            };
            let UseTarget { module, span, item } = target;
            let Some(&source) = self.by_key.get(&module) else {
                // The load pass already reported why this module is absent.
                continue;
            };
            if source == position {
                errors.push(Diagnostic::at("a module cannot import itself", span));
                continue;
            }
            match item {
                Some(name) => {
                    let Some(symbol) = self.files[source].locals.get(&name) else {
                        errors.push(Diagnostic::at(
                            format!("module `{}` declares no `{name}`", module_label(&module)),
                            span,
                        ));
                        continue;
                    };
                    bind(scope, &name, symbol.clone(), span.clone(), true, errors);
                }
                None => {
                    let mut names: Vec<&String> = self.files[source].locals.keys().collect();
                    names.sort();
                    for name in names {
                        let symbol = self.files[source].locals[name].clone();
                        bind(scope, name, symbol, span.clone(), true, errors);
                    }
                }
            }
        }
    }

    /// The class-level checks that need no expansion.
    ///
    /// They are definition-site checks on purpose: a parameter colliding with a
    /// top-level name is refused where the parameter is written, so that adding
    /// a declaration to a file can never silently change what a class body
    /// means.
    fn check_params(&self, position: usize, file: &File, errors: &mut Vec<Diagnostic>) {
        for item in &file.items {
            let params = match item.value() {
                Item::Agent(class) => class.params.as_ref(),
                Item::Assembly(assembly) => Some(&assembly.params),
                _ => None,
            };
            let Some(params) = params else { continue };
            let mut seen: HashMap<&str, Span> = HashMap::new();
            for param in &params.params {
                let name = param.name.value().as_str();
                let span = param.name.span().clone();
                if let Some(first) = seen.get(name) {
                    errors.push(two_site(
                        format!("parameter `{name}` is declared twice"),
                        span.clone(),
                        "the first declaration",
                        first.clone(),
                    ));
                } else if let Some(binding) = self.files[position].scope.get(name) {
                    errors.push(two_site(
                        format!(
                            "parameter `{name}` collides with {} of the same name; nothing shadows here",
                            binding.symbol.kind.describe()
                        ),
                        span.clone(),
                        "the declaration it collides with",
                        binding.site.clone(),
                    ));
                }
                seen.insert(name, span);
                let ty = check_param_type(&param.ty, errors);
                if let Some(default) = &param.default
                    && ty.is_some_and(ParamType::is_entity)
                {
                    errors.push(two_site(
                        format!(
                            "a `{}` parameter names an entity, and a default is a literal; \
                             every instantiation states this one",
                            param.ty.value()
                        ),
                        default.span().clone(),
                        "the parameter",
                        param.name.span().clone(),
                    ));
                }
                if let Some(default) = &param.default {
                    // A default is a leaf for the same reason a constant is:
                    // it is read at the definition site, where the caller's
                    // scope does not exist.
                    check_leaf_only(default, "a parameter default", errors);
                }
            }
        }
    }

    /// Resolve every constant in every file.
    fn resolve_constants(&mut self, files: &[(String, File)], errors: &mut Vec<Diagnostic>) {
        for (position, (_, file)) in files.iter().enumerate() {
            let mut resolved = HashMap::new();
            for constant in file.consts() {
                let ConstDef { name, value, .. } = constant;
                let mut refusals = Vec::new();
                check_leaf_only(value, "a constant", &mut refusals);
                if !refusals.is_empty() {
                    // The walk under a scope that has nothing in it would only
                    // say the same thing again, worse.
                    errors.append(&mut refusals);
                    continue;
                }
                match resolve_value(value, &LeafScope) {
                    Ok(value) => {
                        resolved.insert(name.value().clone(), value);
                    }
                    Err(error) => errors.push(error),
                }
            }
            self.files[position].consts = resolved;
        }
    }
}

/// Every top-level name one file declares, with duplicates refused.
fn locals_of(
    file: &File,
    position: usize,
    errors: &mut Vec<Diagnostic>,
) -> HashMap<String, Symbol> {
    let mut locals: HashMap<String, Symbol> = HashMap::new();
    for (item_index, item) in file.items.iter().enumerate() {
        let Some((kind, name)) = declared_name(item.value()) else {
            continue;
        };
        let symbol = Symbol {
            kind,
            file: position,
            item: item_index,
            span: name.span().clone(),
            class: match item.value() {
                Item::Inst(stmt) => Some(stmt.cls.clone()),
                _ => None,
            },
        };
        if let Some(first) = locals.get(name.value()) {
            errors.push(two_site(
                format!("`{}` is declared twice in this file", name.value()),
                symbol.span.clone(),
                "the first declaration",
                first.span.clone(),
            ));
            continue;
        }
        locals.insert(name.value().clone(), symbol);
    }
    locals
}

/// Refuse two entities declared under one name inside an assembly body.
///
/// A file's top level gets this from [`locals_of`]; an assembly body has no
/// symbol table of its own, and without the check the second declaration would
/// simply take the handle — both entities emitted, every reference resolving to
/// the later one and the earlier wired to nothing.
///
/// Definition-site, like the parameter checks: once per assembly rather than
/// once per instantiation.
fn check_bodies(file: &File, errors: &mut Vec<Diagnostic>) {
    for def in file.assemblies() {
        let mut seen: HashMap<&str, &Span> = HashMap::new();
        for item in &def.items {
            let Some(name) = stamped_name(item.value()) else {
                continue;
            };
            if let Some(first) = seen.get(name.value().as_str()) {
                errors.push(two_site(
                    format!(
                        "`{}` is declared twice in assembly `{}`",
                        name.value(),
                        def.name.value()
                    ),
                    name.span().clone(),
                    "the first declaration",
                    (*first).clone(),
                ));
                continue;
            }
            seen.insert(name.value().as_str(), name.span());
        }
        // A parameter and a handle the body stamps under the same name: the
        // parameter answers every reference and the stamped entity is
        // unreachable from inside the body it belongs to. Nothing shadows here.
        for param in &def.params.params {
            if let Some(stamped) = seen.get(param.name.value().as_str()) {
                errors.push(two_site(
                    format!(
                        "parameter `{}` collides with a handle assembly `{}` stamps; \
                         nothing shadows here",
                        param.name.value(),
                        def.name.value()
                    ),
                    param.name.span().clone(),
                    "the handle it collides with",
                    (*stamped).clone(),
                ));
            }
        }
    }
}

/// The handle an assembly body's item is stamped under, where it stamps one.
fn stamped_name(item: &AssemblyItem) -> Option<&Spanned<String>> {
    match item {
        AssemblyItem::Channel(def) => match &**def {
            crate::model::ChannelDef::Decl(decl) => Some(&decl.handle),
            // A tuning block has no handle: it is a matcher, not an identity.
            crate::model::ChannelDef::Tuning(_) => None,
        },
        AssemblyItem::Link(stmt) => Some(&stmt.handle),
        AssemblyItem::Surface(def) => Some(&def.name),
        AssemblyItem::Inst(inst) => Some(&inst.handle),
        // A grant declares nothing; it names two things already declared.
        AssemblyItem::Grant(_) => None,
    }
}

/// What a top-level item declares, where it declares a name at all.
fn declared_name(item: &Item) -> Option<(SymKind, &Spanned<String>)> {
    Some(match item {
        Item::ConstDef(def) => (SymKind::Const, &def.name),
        Item::Component(class) => (SymKind::ComponentClass, &class.name),
        Item::Agent(class) => (SymKind::AgentClass, &class.name),
        Item::Assembly(def) => (SymKind::Assembly, &def.name),
        Item::Channel(def) => match &**def {
            crate::model::ChannelDef::Decl(decl) => (SymKind::Channel, &decl.handle),
            // A tuning block has no handle: it is a matcher, not an identity.
            crate::model::ChannelDef::Tuning(_) => return None,
        },
        Item::Link(stmt) => (SymKind::Link, &stmt.handle),
        Item::Surface(def) => (SymKind::Surface, &def.name),
        Item::Inst(stmt) => (SymKind::Instance, &stmt.handle),
        Item::Remote(def) => (SymKind::Remote, &def.name),
        Item::Webhook(def) => (SymKind::Webhook, &def.name),
        Item::Repo(def) => (SymKind::Repo, &def.name),
        Item::MqttClient(def) => (SymKind::MqttClient, &def.name),
        Item::McpServer(def) => (SymKind::McpServer, &def.name),
        Item::UuidPins(_) | Item::Acl(_) | Item::Grant(_) | Item::Section(_) => return None,
    })
}

/// Put a name in a scope, refusing a collision with whatever is already there.
fn bind(
    scope: &mut HashMap<String, Binding>,
    name: &str,
    symbol: Symbol,
    site: Span,
    imported: bool,
    errors: &mut Vec<Diagnostic>,
) {
    if let Some(existing) = scope.get(name) {
        // Two `use` statements reaching the same declaration through the same
        // module are one import written twice, not a collision.
        if existing.symbol.file == symbol.file && existing.symbol.item == symbol.item {
            return;
        }
        let what = if existing.imported {
            "another import"
        } else {
            "a declaration in this file"
        };
        errors.push(two_site(
            format!("importing `{name}` collides with {what}"),
            site,
            "the name it collides with",
            existing.site.clone(),
        ));
        return;
    }
    scope.insert(
        name.to_string(),
        Binding {
            symbol,
            site,
            imported,
        },
    );
}

/// The `::`-separated segments of a `use` path, or `None` where the path names
/// no module at all.
fn module_segments(stmt: &UseStmt) -> Option<Vec<String>> {
    let mut segments = vec![stmt.path.head.value().clone()];
    for segment in &stmt.path.segs {
        match segment {
            PathSeg::Module(seg) => segments.push(seg.name.value().clone()),
            PathSeg::Inst(_) => return None,
        }
    }
    // Without a glob the last segment is the item, so a one-segment path has
    // named a module and nothing in it.
    if !stmt.glob && segments.len() < 2 {
        return None;
    }
    Some(segments)
}

/// Why a `use` path names no module, positioned at what it wrote instead.
fn use_shape_error(stmt: &UseStmt) -> Diagnostic {
    for segment in &stmt.path.segs {
        if let PathSeg::Inst(seg) = segment {
            return Diagnostic::at(
                "a module path is written with `::`, not `.`",
                seg.name.span().clone(),
            );
        }
    }
    Diagnostic::at(
        "a `use` names an item: `use module::Item;`, or `use module::*;` for all of them",
        stmt.path.head.span().clone(),
    )
}

/// The parameter types the language has.
///
/// Parsed once, at the parameter that declares it, and carried as this enum
/// everywhere after: a type the declaration accepted and no binding rule
/// matches would be a parameter that can never be satisfied.
#[derive(Clone, Copy, PartialEq, Eq)]
enum ParamType {
    String,
    Int,
    Bool,
    Table,
    Channel,
    Agent,
    Repo,
}

/// The parameter types the language has, in the order a message lists them.
const PARAM_TYPES: [ParamType; 7] = [
    ParamType::String,
    ParamType::Int,
    ParamType::Bool,
    ParamType::Table,
    ParamType::Channel,
    ParamType::Agent,
    ParamType::Repo,
];

impl ParamType {
    /// The type a word names, or nothing where it names none.
    fn parse(word: &str) -> Option<ParamType> {
        Some(match word {
            "String" => ParamType::String,
            "Int" => ParamType::Int,
            "Bool" => ParamType::Bool,
            "Table" => ParamType::Table,
            "Channel" => ParamType::Channel,
            "Agent" => ParamType::Agent,
            "Repo" => ParamType::Repo,
            _ => return None,
        })
    }

    /// The word this type is written with.
    fn as_str(self) -> &'static str {
        match self {
            ParamType::String => "String",
            ParamType::Int => "Int",
            ParamType::Bool => "Bool",
            ParamType::Table => "Table",
            ParamType::Channel => "Channel",
            ParamType::Agent => "Agent",
            ParamType::Repo => "Repo",
        }
    }

    /// Whether an argument of this type names an entity rather than carrying a
    /// value. An entity is checked before the value walk, and cannot default.
    fn is_entity(self) -> bool {
        match self {
            ParamType::Channel | ParamType::Agent | ParamType::Repo => true,
            ParamType::String | ParamType::Int | ParamType::Bool | ParamType::Table => false,
        }
    }
}

/// Refuse a parameter type the language does not have, at the parameter.
fn check_param_type(ty: &Spanned<String>, errors: &mut Vec<Diagnostic>) -> Option<ParamType> {
    let parsed = ParamType::parse(ty.value());
    if parsed.is_none() {
        errors.push(Diagnostic::at(
            format!(
                "`{}` is not a parameter type; expected one of {}",
                ty.value(),
                PARAM_TYPES
                    .iter()
                    .map(|ty| ty.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            ty.span().clone(),
        ));
    }
    parsed
}

/// The refusal for a `.`-segment written after a name that is not an instance.
///
/// The resolver's most-repeated error path: every reference form that admits a
/// dotted tail reaches an instance through it, and nothing else. `owner_kind`
/// leads the message where the name came from somewhere other than the
/// document's own declarations — a class parameter.
fn no_such_segment(owner: &str, owner_kind: Option<&str>, segment: &Spanned<String>) -> Diagnostic {
    let lead = match owner_kind {
        Some(kind) => format!("{kind} `{owner}`"),
        None => format!("`{owner}`"),
    };
    Diagnostic::at(
        format!(
            "{lead} is not an instance, so `.{}` names nothing",
            segment.value()
        ),
        segment.span().clone(),
    )
}

// ── pass 3: constants, and the value walk ────────────────────────────────────

/// Refuse anything but a literal, at any depth.
///
/// Constants and parameter defaults are leaves: they are read where they are
/// written, and a reference or an f-string there would mean the value depends
/// on a scope the reader is not looking at.
fn check_leaf_only(value: &Spanned<Value>, what: &str, errors: &mut Vec<Diagnostic>) {
    let offender = match value.value() {
        Value::Ref(_) => Some("a reference"),
        Value::Fstr(_) => Some("an f-string"),
        Value::M(matcher) => match &matcher.val {
            MatcherVal::Chan(_) => Some("a reference"),
            MatcherVal::Lit(text) => match text.value() {
                StrLike::Fstr(_) => Some("an f-string"),
                StrLike::Str(_) => None,
            },
        },
        _ => None,
    };
    if let Some(offender) = offender {
        errors.push(Diagnostic::at(
            format!("{what} is a literal; {offender} is not one"),
            value.span().clone(),
        ));
        return;
    }
    match value.value() {
        Value::List(list) => {
            for item in &list.items {
                check_leaf_only(item, what, errors);
            }
        }
        Value::Table(table) => {
            for (_, entry) in table.entries.entries() {
                check_leaf_only(entry, what, errors);
            }
        }
        Value::M(matcher) => {
            if let Some(tail) = &matcher.tail {
                for (_, entry) in tail.entries.entries() {
                    check_leaf_only(entry, what, errors);
                }
            }
        }
        _ => {}
    }
}

/// What a value position can reach: names, and the channels a matcher names.
///
/// The value walk is one function over every value position in the document;
/// what differs between positions is which scope they see, which is this.
pub trait ValueScope {
    /// The value a path names.
    fn lookup(&self, path: &PathRef, span: &Span) -> Result<RVal, Diagnostic>;

    /// The declared channel a path names, for an `exact` matcher.
    fn lookup_channel(&self, path: &PathRef, span: &Span) -> Result<ChanId, Diagnostic>;

    /// What a binding's handle names: a declared channel, or a link.
    ///
    /// One method rather than two lookups at the call site, because which of
    /// the two a handle is comes from the symbol it resolves to and nothing
    /// else — asking for a channel first would report "not a channel" about a
    /// link the binding is entitled to name.
    fn lookup_chan_target(&self, path: &PathRef, span: &Span) -> Result<RChanRef, Diagnostic>;
}

/// The scope a leaf position has: none.
///
/// Constants and parameter defaults resolve under it, after the leaves-only
/// check has already refused everything that could reach it. It is still a
/// refusal rather than a panic, because the check and the walk are separate
/// passes and neither should depend on the other having run.
struct LeafScope;

impl ValueScope for LeafScope {
    fn lookup(&self, _path: &PathRef, span: &Span) -> Result<RVal, Diagnostic> {
        Err(Diagnostic::at(
            "a reference is not legal here",
            span.clone(),
        ))
    }

    fn lookup_channel(&self, _path: &PathRef, span: &Span) -> Result<ChanId, Diagnostic> {
        Err(Diagnostic::at(
            "a channel reference is not legal here",
            span.clone(),
        ))
    }

    fn lookup_chan_target(&self, path: &PathRef, span: &Span) -> Result<RChanRef, Diagnostic> {
        self.lookup_channel(path, span).map(RChanRef::Decl)
    }
}

/// Resolve one value position: references followed, strings decoded and
/// interpolated, everything below walked the same way.
pub fn resolve_value(value: &Spanned<Value>, scope: &impl ValueScope) -> Result<RVal, Diagnostic> {
    let span = value.span().clone();
    let resolved = match value.value() {
        Value::Str(literal) => RValue::Str(decode_str(literal)?),
        Value::Fstr(fstr) => RValue::Str(interpolate(fstr, scope)?),
        // A raw string has no escapes by construction: its interior text is the
        // value.
        Value::Raw(text) => RValue::Str(text.value().clone()),
        Value::Int(number) => RValue::Int(*number.value()),
        Value::Flt(number) => RValue::Flt(*number.value()),
        Value::Bool(flag) => RValue::Bool(*flag.value()),
        Value::Ref(path) => return scope.lookup(path, &span),
        Value::List(list) => RValue::List(
            list.items
                .iter()
                .map(|item| resolve_value(item, scope))
                .collect::<Result<Vec<_>, _>>()?,
        ),
        Value::Table(table) => RValue::Table(resolve_table(table, scope)?),
        Value::M(matcher) => RValue::Matcher(resolve_matcher(matcher, scope)?),
    };
    Ok(Spanned::new(resolved, span))
}

/// An inline table's entries, in source order.
fn resolve_table(
    table: &InlineTable,
    scope: &impl ValueScope,
) -> Result<Vec<(String, RVal)>, Diagnostic> {
    table
        .entries
        .entries()
        .iter()
        .map(|(key, value)| Ok((key.clone(), resolve_value(value, scope)?)))
        .collect()
}

/// The matcher kinds, as a diagnostic lists them.
fn matcher_kinds() -> String {
    MatcherKind::ALL
        .iter()
        .map(|kind| format!("`{}`", kind.as_str()))
        .collect::<Vec<_>>()
        .join(", ")
}

/// A matcher: its kind checked, its payload resolved, its tail walked like any
/// other table.
fn resolve_matcher(matcher: &Matcher, scope: &impl ValueScope) -> Result<RMatcher, Diagnostic> {
    let word = matcher.kind.value();
    let Some(kind) = MatcherKind::parse(word) else {
        return Err(Diagnostic::at(
            format!(
                "`{word}` is not a matcher kind; matchers are {}",
                matcher_kinds()
            ),
            matcher.kind.span().clone(),
        ));
    };
    let value = match &matcher.val {
        MatcherVal::Lit(text) => {
            let span = str_like_span(text);
            Spanned::new(
                RMatcherVal::Lit(resolve_str_like(text.value(), scope)?),
                span,
            )
        }
        MatcherVal::Chan(path) => {
            let span = path.head.span().clone();
            let id = scope.lookup_channel(path, &span)?;
            Spanned::new(RMatcherVal::Chan(id), span)
        }
    };
    let tail = match &matcher.tail {
        Some(table) => resolve_table(table, scope)?,
        None => Vec::new(),
    };
    Ok(RMatcher {
        kind: Spanned::new(kind, matcher.kind.span().clone()),
        val: value,
        tail,
    })
}

/// A string or f-string in a position where both are legal, resolved to text.
pub fn resolve_str_like(text: &StrLike, scope: &impl ValueScope) -> Result<String, Diagnostic> {
    match text {
        StrLike::Str(literal) => decode_str(literal),
        StrLike::Fstr(fstr) => interpolate(fstr, scope),
    }
}

/// Where a `str_like` was written: every part it has, merged, falling back to
/// the whole node when it has none.
fn str_like_span(text: &Spanned<StrLike>) -> Span {
    match text.value() {
        StrLike::Str(literal) => merged_span(literal.parts.iter().map(str_part_span), text.span()),
        StrLike::Fstr(fstr) => merged_span(fstr.parts.iter().map(fstr_part_span), text.span()),
    }
}

/// The span covering a string's parts, so a diagnostic underlines the whole
/// string rather than its first fragment. `""` has no parts, so it cites the
/// node span, which encloses the delimiters.
fn merged_span(parts: impl Iterator<Item = Span>, whole: &Span) -> Span {
    parts
        .reduce(|left, right| left.merge(&right).unwrap_or(left))
        .unwrap_or_else(|| whole.clone())
}

fn str_part_span(part: &StrPart) -> Span {
    match part {
        StrPart::Esc(text) | StrPart::Frag(text) => text.span().clone(),
    }
}

fn fstr_part_span(part: &FStrPart) -> Span {
    match part {
        FStrPart::Esc(text) | FStrPart::Frag(text) => text.span().clone(),
        FStrPart::Brace(brace) => brace.span().clone(),
        FStrPart::Interp(path) => path.head.span().clone(),
    }
}

/// The escapes the language has. `\u{…}` is deliberately absent: string content
/// is Unicode already, and adding it later is additive.
fn decode_escape(text: &Spanned<String>) -> Result<char, Diagnostic> {
    let spelling = text.value().as_str();
    let decoded = match spelling {
        "\\" => '\\',
        "\"" => '"',
        "n" => '\n',
        "t" => '\t',
        "r" => '\r',
        "0" => '\0',
        other => {
            return Err(Diagnostic::at(
                format!("unknown escape `\\{other}`; known: \\\\ \\\" \\n \\t \\r \\0"),
                text.span().clone(),
            ));
        }
    };
    Ok(decoded)
}

/// A plain string: fragments as written, escapes decoded.
pub fn decode_str(literal: &StrLit) -> Result<String, Diagnostic> {
    let mut text = String::new();
    for part in &literal.parts {
        match part {
            StrPart::Frag(fragment) => text.push_str(fragment.value()),
            StrPart::Esc(escape) => text.push(decode_escape(escape)?),
        }
    }
    Ok(text)
}

/// An f-string: escapes decoded, `{{`/`}}` as braces, `{path}` spliced.
///
/// Only a string or an integer splices. A float, a boolean, a list, a table, a
/// matcher or an entity handle is refused rather than formatted: implicit
/// formatting of those is a silent-meaning trap, floats above all.
fn interpolate(fstr: &crate::model::FStr, scope: &impl ValueScope) -> Result<String, Diagnostic> {
    let mut text = String::new();
    for part in &fstr.parts {
        match part {
            FStrPart::Frag(fragment) => text.push_str(fragment.value()),
            FStrPart::Esc(escape) => text.push(decode_escape(escape)?),
            FStrPart::Brace(brace) => text.push(match brace.value() {
                crate::model::BraceEscape::Open => '{',
                crate::model::BraceEscape::Close => '}',
            }),
            FStrPart::Interp(path) => {
                let span = path.head.span().clone();
                let spliced = scope.lookup(path, &span)?;
                match spliced.value() {
                    RValue::Str(inner) => text.push_str(inner),
                    RValue::Int(number) => text.push_str(&number.to_string()),
                    other => {
                        return Err(Diagnostic::at(
                            format!(
                                "cannot interpolate {}; only a string or an integer splices",
                                other.kind()
                            ),
                            span,
                        ));
                    }
                }
            }
        }
    }
    Ok(text)
}

// ── the file-scope value scope ───────────────────────────────────────────────

/// A handle path as the tables key it: the head and its `.`-segments joined.
fn dotted_path(head: &str, rest: &[&Spanned<String>]) -> String {
    let mut dotted = head.to_string();
    for segment in rest {
        dotted.push('.');
        dotted.push_str(segment.value());
    }
    dotted
}

/// What a file's dotted handles name.
///
/// One keying rule for every kind of handle a document can reach: the file a
/// reference reaches the handle through, then the dotted handle itself. Each
/// entity kind is an instantiation of this rather than a second copy of the
/// three methods, so the two tables cannot drift apart on how they key.
pub(crate) struct HandleTable<T> {
    by_handle: HashMap<usize, HashMap<String, T>>,
}

// Derived `Default` would demand `T: Default`, which an id or a kind has no
// reason to be: an empty table is empty whatever it holds.
impl<T> Default for HandleTable<T> {
    fn default() -> Self {
        HandleTable {
            by_handle: HashMap::new(),
        }
    }
}

impl<T: Copy> HandleTable<T> {
    /// Record what a handle names, answering with whatever it named before.
    fn record(&mut self, file: usize, handle: String, value: T) -> Option<T> {
        self.by_handle
            .entry(file)
            .or_default()
            .insert(handle, value)
    }

    /// What a file's handle names, where it names anything.
    fn get(&self, file: usize, handle: &str) -> Option<T> {
        self.by_handle.get(&file)?.get(handle).copied()
    }

    /// Record a declaration. The id is the entity's position in the config.
    ///
    /// A handle reaches this once: duplicates at a file's top level are refused
    /// by the index pass and inside an assembly body by [`check_bodies`], so a
    /// second insert under one handle would mean a reference resolves to an
    /// entity no one named. `noun` is what the panic calls the space.
    fn declare(&mut self, noun: &'static str, file: usize, handle: &str, id: T) {
        let prior = self.record(file, handle.to_string(), id);
        assert!(
            prior.is_none(),
            "{noun} handle `{handle}` declared twice in file {file}"
        );
    }
}

impl<T: Copy + Eq + Hash> HandleTable<T> {
    /// Apply an id remapping after renumbering.
    ///
    /// Stamped ids are minted in the order the instantiations completed and
    /// renumbered into source order once expansion is done; the table has to
    /// follow, or a reference resolves to the position some other entity took.
    fn renumber(&mut self, remap: &HashMap<T, T>) {
        for handles in self.by_handle.values_mut() {
            for id in handles.values_mut() {
                if let Some(fresh) = remap.get(id) {
                    *id = *fresh;
                }
            }
        }
    }
}

/// Every declared channel, found the way a document names one.
///
/// Keyed by the declaration rather than by address: two spellings of one
/// address are a refusal, not a lookup, and a reference names a handle.
pub(crate) type ChannelTable = HandleTable<ChanId>;

/// Every declared link, found the way a document names one.
///
/// The [`ChannelTable`] discipline, on the other handle space: a link is
/// referenced by handle and by nothing else, so nothing else can key it.
pub(crate) type LinkTable = HandleTable<LinkId>;

/// What an instantiation stamped, and what kind of entity it is.
///
/// Keyed by the file the top-level instantiation was written in, the way
/// [`ChannelTable`] is: a stamped handle belongs to the file a reference from
/// outside reaches it through.
pub(crate) type StampTable = HandleTable<StampKind>;

/// The kind of entity a stamped handle names.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum StampKind {
    Agent,
    Surface,
    Component,
    Assembly,
}

impl StampKind {
    /// What instantiating a class of this kind stamps, where it stamps an
    /// entity a reference can name.
    fn of_class(kind: SymKind) -> Option<StampKind> {
        match kind {
            SymKind::AgentClass => Some(StampKind::Agent),
            SymKind::ComponentClass => Some(StampKind::Component),
            SymKind::Assembly => Some(StampKind::Assembly),
            _ => None,
        }
    }

    /// What this is, for a diagnostic that has to say what a handle reached.
    fn describe(self) -> &'static str {
        match self {
            StampKind::Agent => "an agent",
            StampKind::Surface => "a surface",
            StampKind::Component => "a component instance",
            StampKind::Assembly => "an assembly instantiation",
        }
    }
}

/// What a value written at a file's top level can name.
///
/// Bare names come from the file's scope — its own declarations and its
/// imports. A `::`-qualified name reaches a module directly, whether or not the
/// file imported anything from it: module paths are absolute from the root, and
/// an import is a convenience for spelling a name short, not a permission.
/// Trailing `.`-segments index into a table constant.
struct FileScope<'a> {
    index: &'a Index,
    file: usize,
    /// The channels declared so far. Empty while the addresses themselves are
    /// being resolved: an address cannot name a channel.
    channels: &'a ChannelTable,
    /// The links declared so far. Never empty for the same reason the channels
    /// are: a link has no address to resolve.
    links: &'a LinkTable,
    /// What the instantiations expanded so far stamped.
    stamps: &'a StampTable,
}

impl<'a> FileScope<'a> {
    /// The scope a name written in one file is resolved through.
    ///
    /// The one place the tables are put together, so that a call site cannot
    /// pass the wrong same-typed table into one of them.
    fn in_file(
        index: &'a Index,
        file: usize,
        channels: &'a ChannelTable,
        links: &'a LinkTable,
        stamps: &'a StampTable,
    ) -> FileScope<'a> {
        FileScope {
            index,
            file,
            channels,
            links,
            stamps,
        }
    }
}

impl ValueScope for FileScope<'_> {
    fn lookup(&self, path: &PathRef, span: &Span) -> Result<RVal, Diagnostic> {
        let (symbol, name, rest) = self.symbol(path, span)?;
        if symbol.kind != SymKind::Const {
            return Err(Diagnostic::at(
                format!(
                    "`{name}` names {}, which is not a value",
                    symbol.kind.describe()
                ),
                span.clone(),
            ));
        }
        let mut value = self.index.files[symbol.file]
            .consts
            .get(&name)
            .ok_or_else(|| {
                Diagnostic::at(format!("`{name}` did not resolve to a value"), span.clone())
            })?
            .clone();
        for segment in rest {
            value = table_field(&value, segment, &name)?;
        }
        Ok(value)
    }

    fn lookup_channel(&self, path: &PathRef, span: &Span) -> Result<ChanId, Diagnostic> {
        let (symbol, name, rest) = self.symbol(path, span)?;
        self.channel_of(&symbol, &name, &rest, span)
    }

    fn lookup_chan_target(&self, path: &PathRef, span: &Span) -> Result<RChanRef, Diagnostic> {
        let (symbol, name, rest) = self.symbol(path, span)?;
        // A stamped handle is reached through the instance that stamped it, and
        // which of the two spaces it lands in is the table's answer, not the
        // symbol's: the symbol is the instantiation either way.
        if !rest.is_empty() || symbol.kind != SymKind::Link {
            if symbol.kind == SymKind::Instance
                && let Some(id) = self.links.get(symbol.file, &dotted_path(&name, &rest))
            {
                return Ok(RChanRef::Link(id));
            }
            return self
                .channel_of(&symbol, &name, &rest, span)
                .map(RChanRef::Decl);
        }
        self.links
            .get(symbol.file, &name)
            .map(RChanRef::Link)
            .ok_or_else(|| Diagnostic::at(format!("link `{name}` did not resolve"), span.clone()))
    }
}

impl FileScope<'_> {
    /// The channel an already-resolved symbol and its `.`-segments name.
    ///
    /// Split out of [`ValueScope::lookup_channel`] so that the channel-target
    /// lookup, which has resolved the symbol to ask about links first, does not
    /// resolve it a second time.
    fn channel_of(
        &self,
        symbol: &Symbol,
        name: &str,
        rest: &[&Spanned<String>],
        span: &Span,
    ) -> Result<ChanId, Diagnostic> {
        if let Some(segment) = rest.first() {
            // A channel an instantiation stamped is named through the handle it
            // was stamped under, and that handle is what the table holds it by.
            if symbol.kind != SymKind::Instance {
                return Err(no_such_segment(name, None, segment));
            }
            let dotted = dotted_path(name, rest);
            return self.channels.get(symbol.file, &dotted).ok_or_else(|| {
                Diagnostic::at(
                    format!("`{name}` stamps no channel `{dotted}`"),
                    span.clone(),
                )
            });
        }
        if symbol.kind != SymKind::Channel {
            return Err(Diagnostic::at(
                format!("`{name}` names {}, not a channel", symbol.kind.describe()),
                span.clone(),
            ));
        }
        self.channels.get(symbol.file, name).ok_or_else(|| {
            Diagnostic::at(
                format!("channel `{name}` did not resolve to an address"),
                span.clone(),
            )
        })
    }
}

impl FileScope<'_> {
    /// The declaration a class path names.
    ///
    /// A class is named directly: there is no instance to reach one through, so
    /// a `.`-segment after the name is a path that names nothing.
    fn class(&self, path: &PathRef, span: &Span) -> Result<(String, Symbol), Diagnostic> {
        let (symbol, name, rest) = self.symbol(path, span)?;
        if let Some(segment) = rest.first() {
            return Err(Diagnostic::at(
                format!(
                    "a class is named directly; `.{}` names nothing in `{name}`",
                    segment.value()
                ),
                segment.span().clone(),
            ));
        }
        Ok((name, symbol))
    }

    /// The declaration a bare name reaches through this file's scope.
    ///
    /// The forms that take a name rather than a path — an `mcp_server`
    /// reference — resolve through this: there is no module to qualify and no
    /// instance to reach through, so the file's scope is the whole question.
    fn named(&self, name: &Spanned<String>) -> Result<Symbol, Diagnostic> {
        match self.index.files[self.file].scope.get(name.value()) {
            Some(binding) => Ok(binding.symbol.clone()),
            None => Err(Diagnostic::at(
                format!(
                    "`{}` is not declared in {}",
                    name.value(),
                    scope_label(&self.index.files[self.file].key)
                ),
                name.span().clone(),
            )),
        }
    }

    /// The declaration a path names, and whatever `.`-segments follow it.
    fn symbol<'p>(
        &self,
        path: &'p PathRef,
        span: &Span,
    ) -> Result<(Symbol, String, Vec<&'p Spanned<String>>), Diagnostic> {
        let (source, name, rest) = self.qualified(path, span)?;
        let file = &self.index.files[source];
        let symbol = match self.index.files[self.file].scope.get(&name) {
            // A bare name is whatever the file's scope says, including an
            // import; a qualified one is the module's own declaration.
            Some(binding) if source == self.file => binding.symbol.clone(),
            _ => match file.locals.get(&name) {
                Some(symbol) => symbol.clone(),
                None => {
                    return Err(Diagnostic::at(
                        format!("`{name}` is not declared in {}", scope_label(&file.key)),
                        span.clone(),
                    ));
                }
            },
        };
        Ok((symbol, name, rest))
    }

    /// Split a path into the module it names, the item in it, and whatever
    /// `.`-segments follow.
    ///
    /// A `::` segment after a `.` segment is refused: module qualification
    /// leads, instance access follows, and the mix in the other order names
    /// nothing.
    fn qualified<'p>(
        &self,
        path: &'p PathRef,
        span: &Span,
    ) -> Result<(usize, String, Vec<&'p Spanned<String>>), Diagnostic> {
        let mut modules = vec![path.head.value().clone()];
        let mut rest: Vec<&Spanned<String>> = Vec::new();
        for segment in &path.segs {
            match segment {
                PathSeg::Module(seg) => {
                    if !rest.is_empty() {
                        return Err(Diagnostic::at(
                            "a `::` module segment cannot follow a `.` segment",
                            seg.name.span().clone(),
                        ));
                    }
                    modules.push(seg.name.value().clone());
                }
                PathSeg::Inst(seg) => rest.push(&seg.name),
            }
        }
        let name = modules.pop().expect("a path has a head");
        if modules.is_empty() {
            return Ok((self.file, name, rest));
        }
        let key = modules.join("::");
        let Some(&source) = self.index.by_key.get(&key) else {
            return Err(Diagnostic::at(format!("no module `{key}`"), span.clone()));
        };
        Ok((source, name, rest))
    }
}

/// One `.`-segment of access into a table value.
fn table_field(value: &RVal, field: &Spanned<String>, owner: &str) -> Result<RVal, Diagnostic> {
    let RValue::Table(entries) = value.value() else {
        return Err(Diagnostic::at(
            format!(
                "`{owner}` is {}, not a table; `.{}` names nothing in it",
                value.value().kind(),
                field.value()
            ),
            field.span().clone(),
        ));
    };
    match entries.iter().find(|(key, _)| key == field.value()) {
        Some((_, found)) => Ok(found.clone()),
        None => {
            let keys: Vec<&str> = entries.iter().map(|(key, _)| key.as_str()).collect();
            Err(Diagnostic::at(
                format!(
                    "`{owner}` has no key `{}`; it has {}",
                    field.value(),
                    if keys.is_empty() {
                        "none".to_string()
                    } else {
                        keys.join(", ")
                    }
                ),
                field.span().clone(),
            ))
        }
    }
}

/// What a file is called in a "not declared in …" message.
fn scope_label(key: &str) -> String {
    if key.is_empty() {
        "this file".to_string()
    } else {
        format!("module `{key}`")
    }
}

/// What a name means where an item is being resolved.
///
/// Three layers, innermost first: the parameters bound by the instantiation
/// being expanded, the channels that instantiation stamped, and the scope of
/// the file the item was written in. The first two are empty for an item
/// written at a file's top level, which is why one type serves both.
///
/// The layers can never both answer for a parameter: a parameter may not
/// collide with any name its file reaches, which the index refuses at the class
/// definition.
struct Scope<'a> {
    outer: FileScope<'a>,
    /// The instantiation's bindings, where there is an instantiation.
    params: Option<&'a ParamBindings>,
    /// The handle every entity of this body is stamped under, where the body is
    /// an assembly's.
    prefix: Option<&'a HandlePath>,
    /// The file the top-level instantiation was written in. Stamped handles
    /// belong to it rather than to the file that declared the assembly, because
    /// that is the file a reference from outside reaches them through.
    root: usize,
}

impl<'a> Scope<'a> {
    /// The scope of a file's own top level: no parameters, nothing stamped.
    fn top(
        index: &'a Index,
        file: usize,
        channels: &'a ChannelTable,
        links: &'a LinkTable,
        stamps: &'a StampTable,
    ) -> Scope<'a> {
        Scope {
            outer: FileScope::in_file(index, file, channels, links, stamps),
            params: None,
            prefix: None,
            root: file,
        }
    }

    /// The parameter a path leads with, where it leads with one.
    ///
    /// A `::`-qualified path names a module, and a module is never a parameter.
    fn param(&self, path: &PathRef) -> Option<&ParamVal> {
        if path
            .segs
            .iter()
            .any(|seg| matches!(seg, PathSeg::Module(_)))
        {
            return None;
        }
        self.params?.get(path.head.value())
    }

    /// The `.`-segments a path carries after its head.
    fn segments(path: &PathRef) -> Vec<&Spanned<String>> {
        path.segs
            .iter()
            .filter_map(|seg| match seg {
                PathSeg::Inst(seg) => Some(&seg.name),
                PathSeg::Module(_) => None,
            })
            .collect()
    }

    /// The handle an entity written here is stamped under.
    fn handle(&self, name: Spanned<String>) -> HandlePath {
        HandlePath::stamp(self.prefix, name)
    }

    /// What this body stamped in one handle space under the name a path spells.
    fn stamped_in<T: Copy>(&self, table: &HandleTable<T>, path: &PathRef) -> Option<T> {
        let prefix = self.prefix?;
        let head = format!("{}.{}", prefix.dotted(), path.head.value());
        table.get(self.root, &dotted_path(&head, &Scope::segments(path)))
    }

    /// The channel this body stamped under the name a path spells, if any.
    fn stamped(&self, path: &PathRef) -> Option<ChanId> {
        self.stamped_in(self.outer.channels, path)
    }

    /// The link this body stamped under the name a path spells, if any.
    fn stamped_link(&self, path: &PathRef) -> Option<LinkId> {
        self.stamped_in(self.outer.links, path)
    }

    /// The declaration a class path names.
    fn class(&self, path: &PathRef, span: &Span) -> Result<(String, Symbol), Diagnostic> {
        self.outer.class(path, span)
    }

    /// The declaration a bare name reaches through the file's scope.
    fn named(&self, name: &Spanned<String>) -> Result<Symbol, Diagnostic> {
        self.outer.named(name)
    }

    /// The declaration a path names, and whatever `.`-segments follow it.
    fn symbol<'p>(
        &self,
        path: &'p PathRef,
        span: &Span,
    ) -> Result<(Symbol, String, Vec<&'p Spanned<String>>), Diagnostic> {
        self.outer.symbol(path, span)
    }

    /// Split a path into the module it names, the item in it, and the rest.
    fn qualified<'p>(
        &self,
        path: &'p PathRef,
        span: &Span,
    ) -> Result<(usize, String, Vec<&'p Spanned<String>>), Diagnostic> {
        self.outer.qualified(path, span)
    }
}

impl ValueScope for Scope<'_> {
    fn lookup(&self, path: &PathRef, span: &Span) -> Result<RVal, Diagnostic> {
        let Some(bound) = self.param(path) else {
            return self.outer.lookup(path, span);
        };
        let ParamVal::Value(value) = bound else {
            return Err(Diagnostic::at(
                format!(
                    "parameter `{}` names {}, which is not a value",
                    path.head.value(),
                    bound.kind()
                ),
                span.clone(),
            ));
        };
        let mut value = value.clone();
        for segment in Scope::segments(path) {
            value = table_field(&value, segment, path.head.value())?;
        }
        Ok(value)
    }

    fn lookup_channel(&self, path: &PathRef, span: &Span) -> Result<ChanId, Diagnostic> {
        let Some(bound) = self.param(path) else {
            // A body's own channels before the file's: the body is the inner
            // scope, and a reference written in it means what it stamped.
            if let Some(id) = self.stamped(path) {
                return Ok(id);
            }
            // A handle this body stamped as a link is a declaration the outer
            // scope cannot see, so it would report a name that plainly exists
            // as naming nothing.
            if self.stamped_link(path).is_some() {
                return Err(Diagnostic::at(
                    format!(
                        "`{}` names a link, not a channel",
                        dotted_path(path.head.value(), &Scope::segments(path))
                    ),
                    span.clone(),
                ));
            }
            return self.outer.lookup_channel(path, span);
        };
        if let Some(segment) = Scope::segments(path).first() {
            return Err(no_such_segment(
                path.head.value(),
                Some("parameter"),
                segment,
            ));
        }
        match bound {
            ParamVal::Chan(id) => Ok(*id),
            other => Err(Diagnostic::at(
                format!(
                    "parameter `{}` names {}, not a channel",
                    path.head.value(),
                    other.kind()
                ),
                span.clone(),
            )),
        }
    }

    fn lookup_chan_target(&self, path: &PathRef, span: &Span) -> Result<RChanRef, Diagnostic> {
        // A parameter is never a link — an assembly takes channels, and a link
        // has no address to hand across a parameter list — so a bound name is
        // the channel lookup's whole business.
        if self.param(path).is_none()
            && let Some(id) = self.stamped_link(path)
        {
            return Ok(RChanRef::Link(id));
        }
        if self.param(path).is_none() && self.stamped(path).is_none() {
            return self.outer.lookup_chan_target(path, span);
        }
        self.lookup_channel(path, span).map(RChanRef::Decl)
    }
}

// ── pass 4a: entities that need no class ─────────────────────────────────────
//
// Everything a document declares outright — channels, pins, the named
// definitions, grants and the server's own sections — resolves here, before any
// class is instantiated, because none of it depends on one. What is left for
// expansion is the forms whose meaning is a class's: a surface's components and
// every `new`.

/// Whether a grant may name an entity of this kind at all.
///
/// Carried alongside a withheld handle so that withholding suppresses only the
/// diagnostic it should — "names nothing" — and never the one that says a
/// grant cannot name a repo. A broken body and an illegal grant are
/// independent mistakes, and the report states both on the first compile.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Grantable {
    Yes,
    No,
}

/// The handles an emission pass declared but did not put in the model.
///
/// Two shapes, because there are two ways an entity goes missing. A handle is
/// one entity whose own body was refused. A prefix is an instantiation that
/// never expanded: what it would have stamped is unknowable, so everything
/// under the prefix counts as declared, while the prefix itself — the
/// instantiation handle — names no entity and does not.
#[derive(Default)]
struct Withheld {
    handles: HashMap<String, Grantable>,
    prefixes: HashSet<String>,
}

impl Withheld {
    /// Whether a grant naming this handle names something that was declared
    /// and could hold authority.
    fn grantable(&self, handle: &str) -> bool {
        match self.handles.get(handle) {
            Some(kind) => *kind == Grantable::Yes,
            None => self.prefixes.iter().any(|prefix| {
                handle
                    .strip_prefix(prefix)
                    .is_some_and(|rest| rest.starts_with('.'))
            }),
        }
    }
}

/// What an emission pass produced: the model, and the entities it withheld.
///
/// A withheld entity is one whose body did not resolve whole. It is deliberately
/// absent from the model — no later pass may read a half-resolved value — but it
/// was still *declared*, and a check that reads absence as "never written" would
/// manufacture a second, false diagnostic about an entity the operator did write.
/// The handles are kept for exactly those checks. Every withholding site
/// registers, whatever the kind: a reader of the set may rely on it holding
/// every withheld handle rather than on which kinds a given check looks up.
#[derive(Default)]
struct Emitted {
    config: ResolvedConfig,
    withheld: Withheld,
}

impl Deref for Emitted {
    type Target = ResolvedConfig;

    fn deref(&self) -> &ResolvedConfig {
        &self.config
    }
}

impl DerefMut for Emitted {
    fn deref_mut(&mut self) -> &mut ResolvedConfig {
        &mut self.config
    }
}

impl Emitted {
    /// Record that an entity was declared under this handle and withheld.
    fn withhold(&mut self, handle: &HandlePath, grantable: Grantable) {
        self.withheld.handles.insert(handle.dotted(), grantable);
    }

    /// Record that an instantiation was declared and never expanded: whatever
    /// it would have stamped is declared too, under its handle as a prefix.
    fn withhold_stamps(&mut self, handle: &str) {
        self.withheld.prefixes.insert(handle.to_string());
    }

    /// The declaration reached the model after all: it is no longer withheld.
    ///
    /// The instantiation paths register the handle before they emit, so that
    /// every way of not reaching the model — a refused body, a class that did
    /// not resolve, arguments that did not bind — leaves the handle in the set
    /// without each of them having to remember to say so.
    ///
    /// Keyed by the dotted handle, which is one entity's only because the
    /// pipeline stops on any index error before emission ever runs: two
    /// declarations of one handle never reach this pass together.
    fn emitted(&mut self, handle: &HandlePath) {
        self.withheld.handles.remove(&handle.dotted());
    }
}

/// Resolve every class-free declaration in every file.
///
/// Channels go first and in two steps — every address, then every body —
/// because a matcher elsewhere in the document names a channel by handle and
/// gets a [`ChanId`] back, and an address itself can name no channel.
fn emit_entities(
    index: &Index,
    files: Vec<(String, File)>,
    errors: &mut Vec<Diagnostic>,
) -> Emitted {
    // Taken before the files are consumed into their item lists: a class's
    // identity is its declaring file's, and only the file carries it.
    let declaring: Vec<Declaring> = files
        .iter()
        .map(|(key, file)| Declaring {
            spec_sha256: file.source_sha256.clone(),
            package: is_packaged(key).then(|| module_name(key).to_string()),
        })
        .collect();
    let modules: Vec<Vec<Spanned<Item>>> = files.into_iter().map(|(_, file)| file.items).collect();
    let mut config = Emitted::default();
    let (mut channels, declared, minted) = channel_addresses(index, &modules, errors);
    let (mut links, minted_links) = link_handles(&modules);
    let mut stamps = StampTable::default();
    // Expansion runs between the addresses and the bodies: an assembly stamps
    // channels of its own, and a reference anywhere in the document may name
    // one of them.
    let (stamped, failed) = {
        let mut handles = Handles {
            channels: &mut channels,
            links: &mut links,
        };
        expand_assemblies(
            index,
            &modules,
            &mut handles,
            &mut stamps,
            (minted, minted_links),
            errors,
        )
    };
    // An instantiation that was refused stamped nothing, and what it would
    // have stamped is not knowable: the whole space under its handle is
    // registered as declared so a grant naming one of those entities is not
    // reported as naming nothing.
    for (position, offset) in &failed {
        if let Item::Inst(inst) = modules[*position][*offset].value() {
            config.withhold_stamps(inst.handle.value());
        }
    }
    let classes = component_classes(
        index, &modules, &declaring, &channels, &links, &stamps, errors,
    );
    let agents = agent_classes(&modules);

    // Section multiplicity is decided over the whole document before any body
    // is resolved: the walk is flat across every file, so the same section
    // written in two imported files is the same duplicate.
    let mut written: Vec<((usize, usize), &SectionNode)> = Vec::new();
    for (position, items) in modules.iter().enumerate() {
        for (offset, item) in items.iter().enumerate() {
            if let Item::Section(node) = item.value() {
                written.push(((position, offset), node));
            }
        }
    }
    let duplicates = duplicate_sections(
        written.into_iter(),
        "a document",
        crate::model::CONFIG_BLOCK_KINDWORDS,
        errors,
    );

    for (position, items) in modules.into_iter().enumerate() {
        let scope = Scope::top(index, position, &channels, &links, &stamps);
        for (offset, item) in items.into_iter().enumerate() {
            // Only a section can be in the set, and a refused one is dropped
            // whole rather than resolved.
            if duplicates.contains(&(position, offset)) {
                continue;
            }
            let declaration = declared.get(&(position, offset)).cloned();
            emit_item(
                item.into_value(),
                declaration,
                &scope,
                &classes,
                &agents,
                &mut config,
                errors,
            );
        }
    }
    // Stamped channels take the ids after every declared one, and they were
    // minted in this order: emitting them in it is what keeps a `ChanId` the
    // position it indexes.
    let tables = Tables {
        index,
        channels: &channels,
        links: &links,
        stamps: &stamps,
        classes: &classes,
        agents: &agents,
    };
    for item in stamped {
        emit_stamped(item, &tables, &mut config, errors);
    }
    config
}

/// A channel statement's resolved address, and the id minted for it.
///
/// A tuning has no handle and so no id: it is a matcher, not an identity.
type ChannelDecl = (Spanned<String>, Option<ChanId>);

/// The address of every channel in the document, and the table a reference to
/// one resolves through.
///
/// Keyed by position rather than carried on the item, because the item itself
/// is handed to the emission pass whole and by value.
fn channel_addresses(
    index: &Index,
    modules: &[Vec<Spanned<Item>>],
    errors: &mut Vec<Diagnostic>,
) -> (ChannelTable, HashMap<(usize, usize), ChannelDecl>, usize) {
    let mut channels = ChannelTable::default();
    let mut declared = HashMap::new();
    // An address resolves under a scope with no channels in it: an address is
    // what a channel *is*, so naming one here would be circular.
    let empty = ChannelTable::default();
    let nolinks = LinkTable::default();
    let unstamped = StampTable::default();
    let mut next = 0;
    for (position, items) in modules.iter().enumerate() {
        let scope = Scope::top(index, position, &empty, &nolinks, &unstamped);
        for (offset, item) in items.iter().enumerate() {
            let Item::Channel(def) = item.value() else {
                continue;
            };
            let (addr, handle) = match &**def {
                ChannelDef::Decl(decl) => (&decl.addr, Some(&decl.handle)),
                ChannelDef::Tuning(tuning) => (&tuning.addr, None),
            };
            match resolve_address(addr, &scope) {
                Ok(address) => {
                    let id = handle.map(|handle| {
                        let id = ChanId(next);
                        channels.declare("channel", position, handle.value(), id);
                        next += 1;
                        id
                    });
                    declared.insert((position, offset), (address, id));
                }
                Err(error) => errors.push(error),
            }
        }
    }
    (channels, declared, next)
}

/// Every top-level link's handle, and how many ids they took.
///
/// A separate walk from the channels' because there is nothing to resolve: a
/// link declares a handle and stops. The ids are minted in source order and the
/// emission pass pushes in the same order, which is what makes a [`LinkId`] the
/// position it indexes.
fn link_handles(modules: &[Vec<Spanned<Item>>]) -> (LinkTable, usize) {
    let mut links = LinkTable::default();
    let mut next = 0;
    for (position, items) in modules.iter().enumerate() {
        for item in items {
            let Item::Link(stmt) = item.value() else {
                continue;
            };
            links.declare("link", position, stmt.handle.value(), LinkId(next));
            next += 1;
        }
    }
    (links, next)
}

/// One top-level declaration, resolved into whatever it contributes.
///
/// `declaration` is a channel statement's pre-resolved address and minted id,
/// and nothing for every other form.
fn emit_item(
    item: Item,
    declaration: Option<ChannelDecl>,
    scope: &Scope<'_>,
    classes: &ClassTable,
    agents: &AgentTable,
    config: &mut Emitted,
    errors: &mut Vec<Diagnostic>,
) {
    /// A `keyword name { attrs }` definition: its attrs resolved, pushed onto
    /// the vector its keyword names.
    macro_rules! emit_named {
        ($def:expr, $dest:expr, $identity:expr) => {{
            let NamedAttrDef { doc, name, body } = *$def;
            let (attrs, refused) = resolve_attrs(body.attrs, scope, errors);
            let handle = HandlePath(vec![name]);
            match refused.any() {
                // Withheld, so the identity pass will not see it: its handle is
                // still a spelling worth checking on its own.
                true => {
                    if let Some(family) = $identity {
                        check_charset(&named_slug(&handle), family, errors);
                    }
                    config.withhold(&handle, Grantable::No);
                }
                false => $dest.push(RNamed { handle, attrs, doc }),
            }
        }};
    }
    match item {
        // Constants resolved in pass 3, and a class is consumed by the
        // instantiation that expands it.
        Item::ConstDef(_) | Item::Component(_) | Item::Agent(_) | Item::Assembly(_) => {}
        Item::Surface(def) => emit_surface(*def, scope, classes, config, errors),
        Item::Inst(inst) => emit_inst(*inst, scope, classes, agents, config, errors),
        Item::UuidPins(pins) => {
            for pin in pins.pins {
                match emit_pin(pin) {
                    Ok(pin) => config.uuid_pins.push(pin),
                    Err(error) => errors.push(error),
                }
            }
        }
        Item::Channel(def) => emit_channel(*def, declaration, scope, config, errors),
        Item::Link(stmt) => emit_link(*stmt, scope, config),
        Item::Remote(def) => {
            let handle = HandlePath(vec![def.name.clone()]);
            let (attrs, mut refused) = resolve_attrs(def.attrs, scope, errors);
            // A remote's vocabulary carries no `slug`: its handle is its
            // identity, and there is nothing else it could be spelled as.
            let slug = Spanned::new(handle.dotted(), def.name.span().clone());
            // The acls are checked whatever the attrs did — they read none of
            // the refused values — and a refused body withholds the entity so
            // no later pass reads a substitute.
            let acls = emit_acls(def.acls, scope, errors, &mut refused);
            if refused.any() {
                check_charset(&slug, Family::Remote, errors);
                config.withhold(&handle, Grantable::Yes);
            } else {
                config.remotes.push(RRemote {
                    handle,
                    slug,
                    attrs,
                    acls,
                    doc: def.doc,
                });
            }
        }
        Item::Webhook(def) => {
            let handle = HandlePath(vec![def.name.clone()]);
            let (attrs, mut refused) = resolve_attrs(def.attrs, scope, errors);
            let (slug, checkable) = slug_position(
                attrs.slug.as_ref().map(|attr| &attr.value),
                &refused,
                &handle,
                def.name.span(),
                errors,
            );
            let blocks = emit_webhook_blocks(
                &def.blocks,
                &format!("webhook `{}`", slug.value()),
                scope,
                errors,
                &mut refused,
            );
            if refused.any() {
                if checkable {
                    check_charset(&slug, Family::Webhook, errors);
                }
                config.withhold(&handle, Grantable::No);
            } else {
                config.webhooks.push(RWebhook {
                    handle,
                    slug,
                    attrs,
                    blocks,
                    doc: def.doc,
                });
            }
        }
        // `keyword name { attrs }` is the growth form of the language, so the
        // one shape every such definition resolves through is written once.
        Item::Repo(def) => emit_named!(def, config.repos, Some(Family::Repo)),
        Item::MqttClient(def) => {
            emit_named!(def, config.mqtt_clients, Some(Family::MqttClient))
        }
        // An mcp server has no wire identity of its own, so nothing to check.
        Item::McpServer(def) => emit_named!(def, config.mcp_servers, None),
        Item::Acl(stmt) => errors.push(Diagnostic::at(
            "an acl statement needs an enclosing entity body (surface, agent, remote, \
             or a new instance); at top level, grant authority to a named principal \
             with `grant`",
            stmt.plane.span().clone(),
        )),
        Item::Grant(stmt) => match emit_grant(*stmt, scope) {
            Ok(grant) => config.grants.push(grant),
            Err(error) => errors.push(error),
        },
        Item::Section(node) => {
            if let Some(section) = resolve_section(&node, None, scope, errors) {
                config.sections.push(section);
            }
        }
    }
}

/// One channel statement, in whichever of its two roles it was written.
///
/// `declaration` is the address the prepass resolved and the id it minted;
/// nothing where the address itself was refused, in which case the second
/// diagnostic the body would raise says nothing new.
fn emit_channel(
    def: ChannelDef,
    declaration: Option<ChannelDecl>,
    scope: &Scope<'_>,
    config: &mut Emitted,
    errors: &mut Vec<Diagnostic>,
) {
    // A declaration mints one directory entry — one handle, one id, one
    // address — so a family prefix names nothing it could declare. The flag is
    // the AST's, independent of whether the address value resolved, so the
    // refusal runs ahead of the early return below and the channel is still
    // pushed the same way a refused body is.
    if let ChannelDef::Decl(decl) = &def
        && decl.addr.is_prefix
    {
        errors.push(Diagnostic::at(
            "`prefix` names the family a handle-less tuning block tunes; a \
             declaration names exactly one channel, written `channel alice at \
             \"brenn:alice.in.messages\"`",
            decl.addr.addr.span().clone(),
        ));
    }
    let Some((address, id)) = declaration else {
        return;
    };
    match def {
        ChannelDef::Decl(decl) => {
            // A body that was refused still takes its position: the id
            // was minted with the address, so a channel dropped here
            // would leave every later id pointing one place short. Its
            // resolved values are discarded — the substitutes a refusal
            // leaves behind are never kept — and the error already keeps
            // this config from being published.
            let (attrs, refused) = channel_attrs(decl.body, scope, errors);
            let attrs = match refused.any() {
                true => ChannelAttrs::empty(),
                false => attrs,
            };
            // `ChanId` indexes `config.channels`, and the id was minted
            // in a separate walk. Refuse to publish a config whose ids
            // point at the wrong channel.
            assert_eq!(
                id.map(|id| id.0),
                Some(config.channels.len()),
                "a channel's id is its position in the resolved config"
            );
            config.channels.push(RChannel {
                handle: scope.handle(decl.handle),
                address,
                attrs,
                doc: decl.doc,
            });
        }
        ChannelDef::Tuning(tuning) => {
            let is_prefix = tuning.addr.is_prefix;
            // A tuning is not in the id space, so a refused body drops it.
            let (attrs, refused) = channel_attrs(tuning.body, scope, errors);
            if refused.any() {
                return;
            }
            config.tunings.push(RTuning {
                address,
                is_prefix,
                attrs,
                doc: tuning.doc,
            });
        }
    }
}

/// One `link` statement.
///
/// Nothing to resolve and nothing to refuse: the handle is its whole content,
/// and every rule about a link is about the bindings that name it.
fn emit_link(stmt: LinkStmt, scope: &Scope<'_>, config: &mut Emitted) {
    // `LinkId` indexes `config.links`, and the ids were minted in a separate
    // walk over the same items in the same order.
    let id = LinkId(config.links.len());
    let span = stmt.handle.span().clone();
    let handle = scope.handle(stmt.handle);
    assert_eq!(
        scope
            .outer
            .links
            .get(scope.root, &handle.dotted())
            .map(|declared| declared.0),
        Some(id.0),
        "a link's id is its position in the resolved config"
    );
    config.links.push(RLink {
        handle,
        doc: stmt.doc,
        span,
    });
}

/// Which of a body's value positions were refused.
///
/// The collecting walk substitutes a value for each refusal so the rest of the
/// body is still attempted; this is how a later check tells a substituted value
/// from a real one. Per-position rather than one flag: a refusal in one attr
/// must not suppress a check that reads a different attr.
///
/// A position is its span, so the set is only as sound as span uniqueness
/// within one body: two distinct value positions carrying one span would make
/// a resolved value read as a substitute and silently stop a check from
/// running. Parsed spans are distinct by construction — every value position
/// covers different source text — and [`Refused::substitute`] asserts it, in
/// every build: a violation that reached a release binary would suppress
/// diagnostics silently, which is the one outcome this type exists to prevent.
#[derive(Default)]
struct Refused {
    spans: HashSet<Span>,
    dropped: bool,
}

impl Refused {
    /// Whether anything in the body was refused — the body is not to be kept.
    ///
    /// A refused value position and a body statement that could not be
    /// resolved count the same: either one leaves the entity half-resolved.
    fn any(&self) -> bool {
        !self.spans.is_empty() || self.dropped
    }

    /// Record that a statement of the body was dropped, its error already
    /// reported. What is left is not the entity that was written, so the
    /// entity is withheld exactly as a refused value withholds it.
    fn drop_part(&mut self) {
        self.dropped = true;
    }

    /// Whether this value is a substitute standing in for a refusal.
    fn holds(&self, value: &RVal) -> bool {
        self.spans.contains(value.span())
    }

    /// The value that stands in for a refusal at `span`, recorded as one.
    fn substitute(&mut self, span: Span) -> RVal {
        let fresh = self.spans.insert(span.clone());
        assert!(
            fresh,
            "two value positions in one body share a span, so a resolved value \
             would read as a substitute"
        );
        Spanned::new(RValue::Bool(false), span)
    }

    /// The explicit attr value a check should read, or nothing where the
    /// position it sits in was refused.
    fn kept<'v>(&self, value: Option<&'v RVal>) -> Option<&'v RVal> {
        value.filter(|value| !self.holds(value))
    }
}

/// The closure every attr vocabulary crosses on: one value position resolved
/// under one scope, recording each refusal and carrying on.
///
/// A vocabulary's `map_values` stops at the first field it cannot resolve, so a
/// body crossed through a fallible closure reports one error however many it
/// has. This substitutes a value the caller never keeps — `refused` is what says
/// which positions the substitutes sit in, and that the body is not to be
/// emitted — so every field is attempted and everything wrong with the body
/// reaches one report.
fn collecting_resolver<'s, S: ValueScope>(
    scope: &'s S,
    errors: &'s mut Vec<Diagnostic>,
    refused: &'s mut Refused,
) -> impl FnMut(Spanned<Value>) -> Result<RVal, Diagnostic> + 's {
    move |value| {
        let span = value.span().clone();
        match resolve_value(&value, scope) {
            Ok(resolved) => Ok(resolved),
            Err(error) => {
                errors.push(error);
                Ok(refused.substitute(span))
            }
        }
    }
}

/// The message a collecting walk can never produce.
const NEVER_FAILS: &str = "the collecting resolver substitutes rather than failing";

/// One body resolved under one scope: every value attempted, every refusal
/// recorded.
///
/// The one shape "resolve a vocabulary, collect its refusals" takes, so the
/// contract is a callable rather than a four-line incantation repeated at
/// every emit site. What a refused body means is the caller's — a channel
/// keeps its position, every other entity is withheld — but how it is
/// discovered is not.
fn resolve_attrs<A, S>(attrs: A, scope: &S, errors: &mut Vec<Diagnostic>) -> (A::Output, Refused)
where
    A: MapValues<Spanned<Value>, RVal>,
    S: ValueScope,
{
    let mut refused = Refused::default();
    let resolved = attrs
        .map_all(&mut collecting_resolver(scope, errors, &mut refused))
        .expect(NEVER_FAILS);
    (resolved, refused)
}

/// A channel body's attrs, or the empty vocabulary where no body was written.
///
/// What the caller does with a refused body is the caller's, because a
/// declaration and a tuning answer that differently.
fn channel_attrs(
    body: Option<AttrBlock<ChannelAttrs>>,
    scope: &Scope<'_>,
    errors: &mut Vec<Diagnostic>,
) -> (ChannelAttrs<RVal>, Refused) {
    match body {
        Some(block) => resolve_attrs(block.attrs, scope, errors),
        None => (ChannelAttrs::empty(), Refused::default()),
    }
}

/// One `uuid_pins` entry: both sides are plain strings, and the address is
/// checked like any other.
fn emit_pin(pin: UuidPin) -> Result<RPin, Diagnostic> {
    let address = spanned_str(&pin.addr)?;
    check_scheme(address.value(), address.span())?;
    Ok(RPin {
        address,
        uuid: spanned_str(&pin.uuid)?,
    })
}

/// The two planes a `grant` may name. The full plane × scheme × family table is
/// derivation's; the word itself is checkable here and cheap.
const PLANE_WORDS: [&str; 2] = ["subscribe", "publish"];

/// `grant alice_pa subscribe prefix "brenn:alice-desk.";`.
///
/// Which entity the principal names is checked once the entity space is
/// complete ([`check_grants`]); the plane word is checked here.
fn emit_grant(stmt: GrantStmt, scope: &Scope<'_>) -> Result<RGrant, Diagnostic> {
    let principal_span = stmt.principal.head.clone();
    if !PLANE_WORDS.contains(&stmt.plane.value().as_str()) {
        return Err(Diagnostic::at(
            format!(
                "`{}` is not a plane; a grant names `subscribe` or `publish`",
                stmt.plane.value()
            ),
            stmt.plane.span().clone(),
        ));
    }
    // An assembly grants to what it was given: the parameter carries the
    // handle of the entity the argument named, and that handle is the
    // principal.
    if let Some(bound) = scope.param(&stmt.principal) {
        let ParamVal::Agent(principal) = bound else {
            return Err(Diagnostic::at(
                format!(
                    "parameter `{}` names {}, and a grant names a principal",
                    principal_span.value(),
                    bound.kind()
                ),
                principal_span.span().clone(),
            ));
        };
        if let Some(segment) = Scope::segments(&stmt.principal).first() {
            return Err(no_such_segment(
                principal_span.value(),
                Some("parameter"),
                segment,
            ));
        }
        let principal = principal.clone();
        return Ok(RGrant {
            principal,
            principal_span,
            plane: stmt.plane,
            m: resolve_matcher(&stmt.m, scope)?,
        });
    }
    // An assembly body grants about its parameters. A bare name here would
    // record a principal with no instance prefix on it, so two instantiations
    // would write one grant twice and a body name colliding with a top-level
    // one would attach the authority to the wrong entity.
    if scope.prefix.is_some() {
        return Err(Diagnostic::at(
            format!(
                "`{}` is not a parameter of this assembly, and an assembly grants \
                 about its parameters; pass the principal in",
                principal_span.value()
            ),
            principal_span.span().clone(),
        ));
    }
    // Module qualification is how the name was reached, not part of the
    // identity it reached: the handle is the declared name and whatever
    // instance segments follow it.
    let (_, name, rest) = scope.qualified(&stmt.principal, principal_span.span())?;
    let mut segments = vec![Spanned::new(name, principal_span.span().clone())];
    segments.extend(rest.into_iter().cloned());
    Ok(RGrant {
        principal: HandlePath(segments),
        principal_span,
        plane: stmt.plane,
        m: resolve_matcher(&stmt.m, scope)?,
    })
}

/// The `acl` statements of an entity body, resolved.
fn emit_acls(
    acls: Vec<AclStmt>,
    scope: &impl ValueScope,
    errors: &mut Vec<Diagnostic>,
    refused: &mut Refused,
) -> Vec<RAcl> {
    let mut resolved = Vec::new();
    for stmt in acls {
        let matchers: Result<Vec<RMatcher>, Diagnostic> = stmt
            .matchers
            .items
            .iter()
            .map(|matcher| resolve_matcher(matcher, scope))
            .collect();
        match matchers {
            Ok(matchers) => resolved.push(RAcl {
                plane: stmt.plane,
                matchers,
            }),
            Err(error) => {
                errors.push(error);
                refused.drop_part();
            }
        }
    }
    resolved
}

/// A webhook body's sub-blocks, typed by their kindword and resolved.
///
/// A token-context field (e.g. `scheme` on a signature block) reaches the
/// listing as the word that was written, not as a resolved reference.
///
/// At most one block per `(kindword, name)`: one `signature`, one
/// `replay_protection`, and one `key`/`token` per credential id.
fn emit_webhook_blocks(
    blocks: &[SectionNode],
    context: &str,
    scope: &Scope<'_>,
    errors: &mut Vec<Diagnostic>,
    withhold: &mut Refused,
) -> Vec<RWebhookBlock> {
    let mut resolved = Vec::new();
    let duplicates = duplicate_sections(
        blocks.iter().enumerate(),
        context,
        crate::model::WEBHOOK_BLOCK_KINDWORDS,
        errors,
    );
    for (offset, node) in blocks.iter().enumerate() {
        if duplicates.contains(&offset) {
            withhold.drop_part();
            continue;
        }
        let block = match crate::model::webhook_block(node) {
            Ok(block) => block,
            Err(error) => {
                errors.push(error);
                withhold.drop_part();
                continue;
            }
        };
        let (parts, refused) = resolve_attrs(block, scope, errors);
        let parts = parts.into_parts();
        // No webhook sub-block nests a second level, so a section held inside
        // one has no vocabulary to be checked against and no reader; carrying
        // it would be carrying an unchecked body, dropping it would be silent
        // loss, so it is refused. Refused before the body's own verdict is
        // read: what was nested inside the block is a separate mistake from a
        // value it could not resolve, and both belong in one report.
        refuse_subs(parts.kindword.value(), &parts.subs, errors);
        if refused.any() {
            withhold.drop_part();
            continue;
        }
        resolved.push(RWebhookBlock {
            kindword: parts.kindword,
            name: parts.name,
            attrs: parts.attrs,
            subs: Vec::new(),
            doc: parts.doc,
        });
    }
    resolved
}

/// The sections of a list that repeat a `(kindword, name)` already written,
/// refused two-site at their own kindword. Returns which of them were refused,
/// keyed however the caller identifies a position.
///
/// The key is [`crate::model::section_key`] and the refusal is
/// [`duplicate_statement`], both shared with the lowering-side belt in
/// `brenn-lib/src/config/dsl_lower.rs`, so the two layers count the same thing
/// and say the same thing about it.
///
/// Counted over the written document rather than over what survives
/// resolution: a section refused for an unrelated reason still occupies its
/// slot, so `server { <bad attr> } server { }` reports both the attr error and
/// the duplicate. A refused duplicate is dropped without resolving its body.
///
/// Only the kindwords in `admitted` are counted. Saying that a kindword the
/// context admits none of appears twice would tell the operator that one of
/// them would have been fine, and would swallow the refusal the dispatch has
/// for it — the same reason a parent that nests nothing counts nothing.
fn duplicate_sections<'a, K: Copy + Eq + std::hash::Hash>(
    sections: impl Iterator<Item = (K, &'a SectionNode)>,
    context: &str,
    admitted: &[&str],
    errors: &mut Vec<Diagnostic>,
) -> HashSet<K> {
    let keyed: Vec<(String, K, Span)> = sections
        .filter_map(|(at, node)| {
            let (kindword, span) = crate::model::section_kindword(node);
            if !admitted.contains(&kindword.as_str()) {
                return None;
            }
            let key =
                crate::model::section_key(&kindword, crate::model::section_name(node).as_deref());
            Some((key, at, span))
        })
        .collect();
    let held = check_unique(
        keyed.iter().map(|(key, at, span)| (key.clone(), *at, span)),
        |key, _, at, _, first| duplicate_statement(context, key, at.clone(), first.clone()),
        errors,
    );
    let kept: HashSet<K> = held.into_values().map(|(at, _)| at).collect();
    keyed
        .iter()
        .map(|(_, at, _)| *at)
        .filter(|at| !kept.contains(at))
        .collect()
}

/// A block that nests nothing refuses what was written inside it.
fn refuse_subs(parent: &str, subs: &[SectionNode], errors: &mut Vec<Diagnostic>) {
    for sub in subs {
        refuse_sub(parent, sub, errors);
    }
}

/// One such refusal, at the nested block's own kindword.
fn refuse_sub(parent: &str, sub: &SectionNode, errors: &mut Vec<Diagnostic>) {
    let (kindword, span) = crate::model::section_kindword(sub);
    errors.push(Diagnostic::at(
        // `the` rather than `a`: the article a kindword takes depends on how it
        // is said, and `ntfy` is not the only one that reads wrong either way.
        format!("the `{parent}` block holds no sub-blocks, so `{kindword}` has no meaning here"),
        span,
    ));
}

/// Resolve a configuration section, and the sections written inside it.
///
/// The dispatch is what refuses an unknown kindword, a wrong name arity and an
/// unknown key in the block; what it selects is the vocabulary the section's
/// values cross through, so a token context stays the word it was written as
/// and only a value position resolves. A refused section is dropped — its
/// error is already recorded and there is nothing left to carry.
fn resolve_section(
    node: &SectionNode,
    parent: Option<&str>,
    scope: &Scope<'_>,
    errors: &mut Vec<Diagnostic>,
) -> Option<RSection> {
    // Each dispatch already produced a `TypedBlock`, so the block is taken
    // apart rather than deserialized a second time.
    macro_rules! dispatched {
        ($call:expr) => {
            match $call {
                Ok(block) => {
                    let (block, refused) = resolve_attrs(block, scope, errors);
                    let parts = block.into_parts();
                    // The sub-blocks are walked whatever the body did: a value
                    // this block could not resolve says nothing about them, and
                    // an operator fixing one error at a time is what a compiler
                    // that reports the whole file exists to prevent.
                    let subs = resolve_subs(&parts.subs, parts.kindword.value(), scope, errors);
                    if refused.any() {
                        return None;
                    }
                    return Some(RSection {
                        kindword: parts.kindword,
                        name: parts.name,
                        attrs: parts.attrs,
                        subs,
                        doc: parts.doc,
                    });
                }
                Err(error) => {
                    errors.push(error);
                    return None;
                }
            }
        };
    }
    match parent {
        None => dispatched!(crate::model::config_block(node)),
        Some(other) => match nesting(other) {
            Some(Nesting::Alerting) => dispatched!(crate::model::alerting_block(node)),
            Some(Nesting::Observability) => dispatched!(crate::model::observability_block(node)),
            // Every other parent admits no sub-block — including `ntfy`, `mail`
            // and `usage`, which are themselves sub-blocks.
            None => {
                refuse_sub(other, node, errors);
                None
            }
        },
    }
}

/// Which vocabulary a nesting parent's sub-blocks are typed through.
enum Nesting {
    Alerting,
    Observability,
}

/// What a parent kindword nests, if it nests anything.
///
/// The sole list of which parents nest: [`resolve_section`] reaches its
/// dispatch through this, and [`resolve_subs`] asks it whether a sub-block is
/// admitted here at all, so the two cannot come to disagree. A parent that
/// gains a vocabulary gains a variant here, and the exhaustive match in
/// `resolve_section` then refuses to compile until it gains its dispatch arm.
fn nesting(parent: &str) -> Option<Nesting> {
    match parent {
        "alerting" => Some(Nesting::Alerting),
        "observability" => Some(Nesting::Observability),
        _ => None,
    }
}

impl Nesting {
    /// The kindwords this parent's body admits.
    fn kindwords(&self) -> &'static [&'static str] {
        match self {
            Self::Alerting => crate::model::ALERTING_BLOCK_KINDWORDS,
            Self::Observability => crate::model::OBSERVABILITY_BLOCK_KINDWORDS,
        }
    }
}

/// The sections held inside one section, resolved under their parent's
/// kindword.
fn resolve_subs(
    subs: &[SectionNode],
    parent: &str,
    scope: &Scope<'_>,
    errors: &mut Vec<Diagnostic>,
) -> Vec<RSection> {
    let context = format!("a `{parent}` section");
    // Counted only where being counted means something: under a parent that
    // nests nothing, "one of these here would be fine" is false guidance, and
    // the dispatch's own refusal is the whole truth about the document.
    let duplicates = match nesting(parent) {
        Some(nesting) => duplicate_sections(
            subs.iter().enumerate(),
            &context,
            nesting.kindwords(),
            errors,
        ),
        None => HashSet::new(),
    };
    subs.iter()
        .enumerate()
        .filter(|(offset, _)| !duplicates.contains(offset))
        .filter_map(|(_, sub)| resolve_section(sub, Some(parent), scope, errors))
        .collect()
}

/// The wire spelling of an entity: what `slug` said, else the handle's full
/// dotted path.
///
/// The full path rather than the leaf because two instantiations of one
/// assembly stamp the same leaf names, and a wire identity that collided
/// between them would collide silently. Whether the result is a legal slug for
/// its family is the check pass's.
fn slug_of(
    explicit: Option<&RVal>,
    handle: &HandlePath,
    name: &Span,
    errors: &mut Vec<Diagnostic>,
) -> Spanned<String> {
    match explicit {
        Some(value) => match str_value(value, "a slug") {
            Ok(text) => Spanned::new(text.to_string(), value.span().clone()),
            Err(error) => {
                errors.push(error);
                Spanned::new(handle.dotted(), name.clone())
            }
        },
        None => Spanned::new(handle.dotted(), name.clone()),
    }
}

/// An entity's identity, and whether it is one worth spell-checking.
///
/// A slug whose own value was refused leaves no identity at all: the fallback
/// is the handle, which the operator never proposed as a wire spelling, so
/// checking it would answer a question nobody asked — and, for a family whose
/// handles are routinely illegal as identities, tell the operator to state a
/// slug they did state. A refusal in some *other* attr says nothing about the
/// slug, and the check still runs.
fn slug_position(
    explicit: Option<&RVal>,
    refused: &Refused,
    handle: &HandlePath,
    name: &Span,
    errors: &mut Vec<Diagnostic>,
) -> (Spanned<String>, bool) {
    let kept = refused.kept(explicit);
    let stated_but_refused = explicit.is_some() && kept.is_none();
    (slug_of(kept, handle, name, errors), !stated_but_refused)
}

/// A string literal with the span of the text it was written as.
fn spanned_str(literal: &Spanned<StrLit>) -> Result<Spanned<String>, Diagnostic> {
    let span = merged_span(
        literal.value().parts.iter().map(str_part_span),
        literal.span(),
    );
    Ok(Spanned::new(decode_str(literal.value())?, span))
}

/// A channel statement's address: resolved to text, then checked for a scheme.
fn resolve_address(
    addr: &ChanAddr,
    scope: &impl ValueScope,
) -> Result<Spanned<String>, Diagnostic> {
    let span = str_like_span(&addr.addr);
    let text = resolve_str_like(addr.addr.value(), scope)?;
    check_scheme(&text, &span)?;
    Ok(Spanned::new(text, span))
}

/// Refuse an address that names no scheme, or names one and nothing else.
fn check_scheme(text: &str, span: &Span) -> Result<(), Diagnostic> {
    match split_spellable(text) {
        Some((_, rest)) if !rest.is_empty() => Ok(()),
        Some((scheme, _)) => Err(Diagnostic::at(
            format!(
                "`{}` is a scheme and nothing else; an address names something under it",
                scheme.prefix()
            ),
            span.clone(),
        )),
        None => Err(Diagnostic::at(
            format!(
                "address `{text}` names no scheme; expected one of {}",
                spellable_list()
            ),
            span.clone(),
        )),
    }
}

// ── pass 4b: classes and the instances that name them ────────────────────────
//
// A component class takes no parameters, so an instance of one is resolved
// rather than substituted: the class is looked up, its facts are copied onto
// the instance, and the body is typed against the vocabulary the placement
// implies. The two forms with parameters — agents and assemblies — are
// expansion's, and are skipped here.

/// The keys a component instance inside a surface admits.
///
/// The scalar fields of the runtime's surface component, minus the three the
/// document already says elsewhere: the kind folds from the class name, the
/// instance name is the `new` handle, and the abi is the class's.
///
/// Public so the parity gate over the runtime's `SurfaceComponentRaw` can read
/// it: a string list is the one vocabulary an exhaustive struct literal in
/// lowering cannot police.
pub const SURFACE_COMPONENT_KEYS: [&str; 6] = [
    COMPONENT_GRANTS,
    "chrome",
    "send_burst",
    "send_refill_secs",
    SURFACE_PARKED_DEPTH,
    "config",
];

/// The key of a component instance's body that is a token context rather than
/// a value: a depth is a count or the word `unbounded`, and a bare word in a
/// value position would resolve as a name.
const SURFACE_PARKED_DEPTH: &str = "parked_batch_depth";

/// The key an instance body states its capabilities with, at either placement:
/// what a component is given is the same question wherever it is placed, and
/// deny-by-default means the list is required either way.
///
/// A token context rather than a value: a bare word in a value position would
/// resolve as a name, which is the failure the projection types exist to
/// prevent. The typed vocabularies handle that with a field type, and this
/// table has to handle it by hand.
const COMPONENT_GRANTS: &str = "grants";

/// The key of a consumer body that states its wire identity.
const CONSUMER_SLUG: &str = "slug";

/// The keys a top-level component instance admits.
///
/// The consumer's scalar fields, minus what statements carry: its ports are
/// bindings and its nine authority lists are `acl` statements. Where the
/// artifact lives is not among them: the host resolves the package from the
/// class's own module name, so the deployment states no location at all.
/// Unspellable in this version, for want of a statement form: `mqtt_output` and
/// `tool_grant`.
///
/// Public for the same reason as [`SURFACE_COMPONENT_KEYS`]: the parity gate
/// over `WasmConsumerConfigRaw` reads it.
pub const CONSUMER_KEYS: [&str; 7] = [
    CONSUMER_SLUG,
    COMPONENT_GRANTS,
    "store_path",
    "store_size_limit",
    "activation_burst",
    "activation_min_period_ms",
    "config",
];

/// Every component class in the document, found where it was declared.
///
/// Keyed by declaration site rather than by name because a name is per-file:
/// two modules may each declare a `Panel`, and an instance names the one its
/// own scope reaches.
#[derive(Default)]
struct ClassTable {
    by_site: HashMap<(usize, usize), ClassRef>,
}

impl ClassTable {
    /// The class declared at a site, or nothing when the class itself was
    /// refused — in which case an instance of it says nothing new.
    fn get(&self, site: (usize, usize)) -> Option<&ClassRef> {
        self.by_site.get(&site)
    }
}

/// What the file a class was declared in contributes to every class in it.
struct Declaring {
    /// The declaring file's content hash, which the class's `spec_sha256` is.
    spec_sha256: String,
    /// The packaged module it is, or `None` for a file of the configuration
    /// tree. A class declared in a packaged module names the package the host
    /// resolves its component under, and that name is the only reference a
    /// configuration ever states for it.
    package: Option<String>,
}

/// Resolve every component class into the facts an instance carries away.
///
/// Classes resolve before instances for the same reason addresses resolve
/// before bodies: the instance's own checks — placement, ports — are questions
/// about its class.
fn component_classes(
    index: &Index,
    modules: &[Vec<Spanned<Item>>],
    declaring: &[Declaring],
    channels: &ChannelTable,
    links: &LinkTable,
    stamps: &StampTable,
    errors: &mut Vec<Diagnostic>,
) -> ClassTable {
    let mut table = ClassTable::default();
    for (position, items) in modules.iter().enumerate() {
        let scope = Scope::top(index, position, channels, links, stamps);
        for (offset, item) in items.iter().enumerate() {
            let Item::Component(class) = item.value() else {
                continue;
            };
            if let Some(reference) = class_ref(class, &declaring[position], &scope, errors) {
                table.by_site.insert((position, offset), reference);
            }
        }
    }
    table
}

/// One component class, resolved to what an instance of it needs to know.
fn class_ref(
    class: &ComponentClass,
    declaring: &Declaring,
    scope: &Scope<'_>,
    errors: &mut Vec<Diagnostic>,
) -> Option<ClassRef> {
    let Declaring {
        spec_sha256,
        package,
    } = declaring;
    // The empty hash is the hole `File::source_sha256`'s `serde(skip)` opens: a
    // `File` built anywhere but `parse_str` carries one. Refused here rather
    // than flowed onward, where two empty hashes would spuriously bind.
    assert!(
        !spec_sha256.is_empty(),
        "class `{}` was declared in a file with no source hash; every `File` \
         reaching resolution is built by `parse_str`",
        class.name.value()
    );
    let word = &class.attrs.abi.value.name;
    let Some(parsed) = Abi::parse(word.value()) else {
        errors.push(Diagnostic::at(
            format!(
                "`{}` is not an abi; expected one of {}",
                word.value(),
                Abi::ALL.map(Abi::as_str).join(", ")
            ),
            word.span().clone(),
        ));
        return None;
    };
    let abi = Spanned::new(parsed, word.span().clone());
    let needs = class_grants(class, errors)?;
    let mut ports: Vec<RPort> = Vec::new();
    for decl in &class.ports {
        if let Some(prior) = ports
            .iter()
            .find(|port| port.name.value() == decl.name.value())
        {
            errors.push(two_site(
                format!("port `{}` is declared twice", decl.name.value()),
                decl.name.span().clone(),
                "first declared here",
                prior.name.span().clone(),
            ));
            continue;
        }
        let doctype = match decl.doctype.as_ref() {
            Some(text) => match resolve_str_like(text.value(), scope) {
                Ok(resolved) => Some(Spanned::new(resolved, str_like_span(text))),
                Err(error) => {
                    errors.push(error);
                    None
                }
            },
            None => None,
        };
        ports.push(RPort {
            name: decl.name.clone(),
            dir: port_dir(decl.dir.value()),
            optional: decl.optional,
            doctype,
        });
    }
    Some(ClassRef {
        name: class.name.clone(),
        abi,
        requires: needs.requires,
        optional: needs.optional,
        ports,
        spec_sha256: spec_sha256.clone(),
        package: package.clone(),
    })
}

/// The two grant lists a class declares, checked as a pair.
struct ClassGrants {
    requires: Vec<Spanned<ComponentGrant>>,
    optional: Vec<Spanned<ComponentGrant>>,
}

/// A class's `requires` and `optional`, refused whole where either is unusable.
///
/// `None` refuses the class, which is the same answer the abi word gets: the
/// spec is what an instance's fit is checked against, so a spec that does not
/// say what it needs is answered once here rather than at every instantiation.
/// Every refusal the pair can hold is reported before returning, so an author
/// fixing one word does not discover the next on the following build.
fn class_grants(class: &ComponentClass, errors: &mut Vec<Diagnostic>) -> Option<ClassGrants> {
    let before = errors.len();
    let list = |attr: Option<&Attr<WordList>>| -> Vec<Spanned<String>> {
        attr.map(|attr| {
            attr.value
                .words
                .iter()
                .map(|word| word.name.clone())
                .collect()
        })
        .unwrap_or_default()
    };
    let requires = list(class.attrs.requires.as_ref());
    let optional = list(class.attrs.optional.as_ref());
    if class.attrs.requires.is_none() {
        errors.push(Diagnostic::at(
            format!(
                "component `{}` states no `requires`: what a component needs is \
                 deny-by-default, so a class needing nothing is written \
                 `requires = [];` rather than left out",
                class.name.value()
            ),
            class.name.span().clone(),
        ));
    }
    // Most of a processor's capabilities are WIT imports, and the host links an
    // interface only where the instance was granted it. An artifact cannot
    // import conditionally, so an optional grant is unexercisable in both
    // directions: an artifact that imports the interface panics at load under
    // an instance that omits the word, and one that does not import it is
    // unchanged by holding it. The word is refused rather than recorded as
    // meaningless.
    //
    // A word naming no interface is exempt: `takeover` is consent to a binding
    // the page gates, with nothing to import and so nothing to link
    // conditionally. Optional is exactly right for it — a surface where nothing
    // requests the overlay hands its chrome no takeover plane to bind.
    for word in &optional {
        let imports =
            ComponentGrant::parse(word.value()).is_some_and(|grant| grant.wit_import().is_some());
        if !imports {
            continue;
        }
        errors.push(Diagnostic::at(
            format!(
                "`{}` cannot be optional on a processor class: a processor reaches a \
                 capability through a WIT import the host links only where the instance \
                 grants it, and an artifact cannot import conditionally, so the word is \
                 either in `requires` or not in the spec",
                word.value()
            ),
            word.span().clone(),
        ));
    }
    // A class admits both hosts, and its instances' words are host-checked
    // where they are written.
    //
    // Every word is parsed into its grant variant here; nothing downstream
    // re-decides it from text.
    let mut parsed: [Vec<Spanned<ComponentGrant>>; 2] = [Vec::new(), Vec::new()];
    for (slot, (which, words)) in [("requires", &requires), ("optional", &optional)]
        .into_iter()
        .enumerate()
    {
        check_unique(
            words
                .iter()
                .map(|word| (word.value().as_str(), (), word.span())),
            |word, (), span, (), prior| {
                two_site(
                    format!(
                        "`{word}` is listed twice in `{which}`; one statement of a need states it"
                    ),
                    span.clone(),
                    "it is listed here",
                    prior.clone(),
                )
            },
            errors,
        );
        for word in words {
            let Some(grant) = ComponentGrant::parse(word.value()) else {
                errors.push(Diagnostic::at(
                    format!(
                        "`{}` is not a capability a component holds; a spec's `{which}` names \
                         the same words a `grants` list does: {}",
                        word.value(),
                        or_list(ComponentGrant::ALL.map(ComponentGrant::word))
                    ),
                    word.span().clone(),
                ));
                continue;
            };
            parsed[slot].push(Spanned::new(grant, word.span().clone()));
        }
    }
    // Page-DOM authority reaches outside a subtree the holder must first have:
    // every arrangement it performs is a mutation, and the instance is mountable
    // only on the scoped word. A class naming one word and not the other names a
    // combination no placement can make coherent, so it is refused here — at
    // class grain, over both lists together. A class that lists both optional
    // passes this and can still be granted one alone, which is why the grant set
    // itself is checked again where a placement builds it (`derive.rs`).
    let declared: Vec<&Spanned<ComponentGrant>> = parsed.iter().flatten().collect();
    if !declared
        .iter()
        .any(|word| *word.value() == ComponentGrant::Dom)
    {
        for word in declared
            .iter()
            .filter(|word| *word.value() == ComponentGrant::PageDom)
        {
            errors.push(Diagnostic::at(
                format!(
                    "`{}` without `{}`: the page-wide capability arranges other instances' \
                     elements and mutates them through the scoped one, and only the scoped one \
                     makes an instance mountable, so a class naming this names both",
                    ComponentGrant::PageDom.word(),
                    ComponentGrant::Dom.word(),
                ),
                word.span().clone(),
            ));
        }
    }
    for word in &requires {
        if let Some(other) = optional.iter().find(|other| other.value() == word.value()) {
            errors.push(two_site(
                format!(
                    "`{}` is both required and optional; it is one or the other",
                    word.value()
                ),
                word.span().clone(),
                "listed optional here",
                other.span().clone(),
            ));
        }
    }
    if errors.len() > before {
        return None;
    }
    let [requires, optional] = parsed;
    Some(ClassGrants { requires, optional })
}

/// The direction a port declaration faces, spelled the way a binding spells it.
fn port_dir(dir: &DeclDir) -> PortDir {
    match dir {
        DeclDir::Into => PortDir::In,
        DeclDir::Outof => PortDir::Out,
        DeclDir::Both => PortDir::Io,
    }
}

/// Where an instantiation was written. What a class may be instantiated as
/// depends on it: a surface contains components, and a top-level `new` is a
/// consumer with an identity of its own.
///
/// Everything a placement decides hangs off this one value, so a new placement
/// is a variant plus its arms and the compiler names every site.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Placement {
    Surface,
    TopLevel,
}

impl Placement {
    /// The closed key set an instance body admits here.
    fn keys(self) -> &'static [&'static str] {
        match self {
            Placement::Surface => &SURFACE_COMPONENT_KEYS,
            Placement::TopLevel => &CONSUMER_KEYS,
        }
    }

    /// The keys of that set that are token contexts: projected out of the body
    /// before the value walk, and so skipped by it.
    fn projected_keys(self) -> &'static [&'static str] {
        match self {
            Placement::Surface => &[SURFACE_PARKED_DEPTH, COMPONENT_GRANTS],
            Placement::TopLevel => &[COMPONENT_GRANTS],
        }
    }

    /// What an instance here is, for the message that says a key is not one of
    /// its own.
    fn describe(self) -> &'static str {
        match self {
            Placement::Surface => "a component instance",
            Placement::TopLevel => "a consumer",
        }
    }
}

/// A surface and everything written inside it.
fn emit_surface(
    def: SurfaceDef,
    scope: &Scope<'_>,
    classes: &ClassTable,
    config: &mut Emitted,
    errors: &mut Vec<Diagnostic>,
) {
    let SurfaceDef {
        doc,
        name,
        attrs,
        acls,
        insts,
    } = def;
    let handle = scope.handle(name.clone());
    // Every value in the body is attempted, and the rest of the body — the
    // slug, the acls, the component instances — is checked regardless: an
    // operator fixing a large surface gets every mistake in it at once.
    let (attrs, mut refused) = resolve_attrs(attrs, scope, errors);
    // The slug is skipped only where the slug's own value was refused; a
    // refusal in some other attr says nothing about it.
    let (slug, checkable) = slug_position(
        attrs.slug.as_ref().map(|attr| &attr.value),
        &refused,
        &handle,
        name.span(),
        errors,
    );
    let acls = emit_acls(acls, scope, errors, &mut refused);
    let mut components: Vec<RComponentInst> = Vec::new();
    // A component's body is part of the surface's body: a substituted value in
    // one is a half-resolved surface, so it withholds the whole entity the way
    // a refusal in the surface's own attrs does — and a component that could
    // not be resolved at all withholds it the same way, being the larger hole
    // of the two.
    for inst in insts {
        match emit_component(inst, scope, classes, &components, errors) {
            Some((component, component_refused)) => {
                if component_refused.any() {
                    refused.drop_part();
                }
                components.push(component);
            }
            None => refused.drop_part(),
        }
    }
    // A surface whose body was refused is withheld: its identity stays out of
    // the collision check and no later pass reads a substituted value. The
    // charset of the identity it did compute is still checked here, because
    // the pass that would have is one the withheld surface never reaches.
    if refused.any() {
        if checkable {
            check_charset(&slug, Family::Surface, errors);
        }
        config.withhold(&handle, Grantable::Yes);
    } else {
        config.surfaces.push(RSurface {
            handle,
            slug,
            attrs,
            acls,
            components,
            doc,
        });
    }
}

/// One component instance inside a surface.
///
/// `siblings` are the instances already resolved in this surface: an instance
/// name is what the runtime calls the component, so two of them in one surface
/// is a two-site refusal.
fn emit_component(
    inst: NewStmt,
    scope: &Scope<'_>,
    classes: &ClassTable,
    siblings: &[RComponentInst],
    errors: &mut Vec<Diagnostic>,
) -> Option<(RComponentInst, Refused)> {
    let class = resolve_class(&inst, scope, classes, Placement::Surface, errors)?;
    check_instance_name(&inst.handle, siblings, errors);
    let parked_batch_depth = parked_depth(inst.body.as_ref(), errors);
    let grants = instance_grants(inst.body.as_ref(), errors);
    let at = inst.handle.span().clone();
    let body = instance_body(inst.body, &class, Placement::Surface, &at, scope, errors);
    Some((
        RComponentInst {
            instance: inst.handle,
            class,
            parked_batch_depth,
            grants,
            attrs: body.attrs,
            acls: body.acls,
            bindings: body.bindings,
            tools: body.tools,
        },
        body.refused,
    ))
}

/// A top-level `new`: a consumer, once the class it names is a component's.
fn emit_consumer(
    inst: NewStmt,
    scope: &Scope<'_>,
    classes: &ClassTable,
    config: &mut Emitted,
    errors: &mut Vec<Diagnostic>,
) {
    let Some(class) = resolve_class(&inst, scope, classes, Placement::TopLevel, errors) else {
        return;
    };
    let handle = scope.handle(inst.handle.clone());
    let grants = instance_grants(inst.body.as_ref(), errors);
    let at = inst.handle.span().clone();
    let ResolvedBody {
        attrs,
        bindings,
        acls,
        tools,
        refused,
    } = instance_body(inst.body, &class, Placement::TopLevel, &at, scope, errors);
    // The slug is read unless the slug's own value was refused: a substitute
    // there would name the consumer after its handle, which is a different
    // identity and could collide with a real one.
    let (slug, checkable) = slug_position(
        attrs
            .iter()
            .find(|(key, _)| key == CONSUMER_SLUG)
            .map(|(_, v)| v),
        &refused,
        &handle,
        inst.handle.span(),
        errors,
    );
    // Withheld on a refusal, exactly as every other entity is: a half-resolved
    // consumer must not reach the collision pass or any later reader. Its
    // identity is still spelling-checked here, since the pass that would have
    // is one it never reaches.
    if refused.any() {
        if checkable {
            check_charset(&slug, Family::Consumer, errors);
        }
        config.withhold(&handle, Grantable::Yes);
        return;
    }
    config.emitted(&handle);
    config.consumers.push(RConsumer {
        handle,
        slug,
        class,
        grants,
        attrs,
        acls,
        bindings,
        tools,
        doc: inst.doc,
    });
}

/// A component instance's `parked_batch_depth`, projected out of the body
/// before its values resolve: a depth is a count or a bare word, and resolving
/// the word as a value would read it as a name.
fn parked_depth(
    body: Option<&Spanned<InstBody>>,
    errors: &mut Vec<Diagnostic>,
) -> Option<IntOrWord> {
    let value = body?.value().attrs.get(SURFACE_PARKED_DEPTH)?;
    match IntOrWord::from_value(value) {
        Ok(depth) => Some(depth),
        Err(error) => {
            errors.push(error);
            None
        }
    }
}

/// An instance's `grants`, projected out of the body before its values resolve.
fn instance_grants(
    body: Option<&Spanned<InstBody>>,
    errors: &mut Vec<Diagnostic>,
) -> Option<RWordList> {
    let value = body?.value().attrs.get(COMPONENT_GRANTS)?;
    match RWordList::from_value(value) {
        Ok(words) => Some(words),
        Err(error) => {
            errors.push(error);
            None
        }
    }
}

/// The class an instantiation names, once it is one this placement admits.
///
/// `None` where the instantiation was refused, and also where the form is one
/// expansion owns: an agent or an assembly at top level is not an error, it is
/// unfinished work.
fn resolve_class(
    inst: &NewStmt,
    scope: &Scope<'_>,
    classes: &ClassTable,
    place: Placement,
    errors: &mut Vec<Diagnostic>,
) -> Option<ClassRef> {
    let span = inst.cls.head.span().clone();
    let (name, symbol) = match scope.class(&inst.cls, &span) {
        Ok(found) => found,
        Err(error) => {
            errors.push(error);
            return None;
        }
    };
    match symbol.kind {
        SymKind::ComponentClass => {}
        SymKind::AgentClass | SymKind::Assembly if place == Placement::Surface => {
            errors.push(two_site(
                format!(
                    "a surface contains components; `{name}` is {}",
                    symbol.kind.describe()
                ),
                span,
                "declared here",
                symbol.span.clone(),
            ));
            return None;
        }
        // Both forms are dispatched to their own expansion before this, so
        // neither reaches here from a placement that admits it.
        SymKind::AgentClass | SymKind::Assembly => return None,
        other => {
            errors.push(two_site(
                format!("`{name}` names {}, which is not a class", other.describe()),
                span,
                "declared here",
                symbol.span.clone(),
            ));
            return None;
        }
    }
    if let Some(args) = &inst.args
        && let Some(first) = args.args.first()
    {
        errors.push(Diagnostic::at(
            "a component instantiation takes a body, not arguments; a component class \
             has no parameters, so per-instance values are written in its body",
            first.name.span().clone(),
        ));
        return None;
    }
    // A class the prepass refused: the instance would only report the same
    // thing again, one indirection further from where it was written.
    let class = classes.get((symbol.file, symbol.item))?;
    if place == Placement::TopLevel && class.package.is_none() {
        errors.push(two_site(
            format!(
                "a top-level instance is loaded from an installed component package, and the \
                 class `{name}` is declared in the configuration tree, not in a packaged \
                 module — declare it in a module imported as `use @<name>::*;`"
            ),
            span,
            "declared here",
            class.name.span().clone(),
        ));
        return None;
    }
    Some(class.clone())
}

/// What an instance body resolved to: its attrs, its bindings, the authority
/// it carried, and which of its value positions were refused.
#[derive(Default)]
struct ResolvedBody {
    attrs: Vec<(String, RVal)>,
    bindings: Vec<RBinding>,
    acls: Vec<RAcl>,
    tools: Vec<RToolGrant>,
    refused: Refused,
}

/// An instance body: its attrs typed against the placement's key set, its
/// bindings checked against the class's ports, its authority carried or
/// refused.
///
/// `at` is the instance name's span, which the required-port refusal is
/// positioned at: both halves of the binding↔port contract are checked here,
/// against the ports this walk saw, so a placement that emits instances cannot
/// wire one up without them.
fn instance_body(
    body: Option<Spanned<InstBody>>,
    class: &ClassRef,
    place: Placement,
    at: &Span,
    scope: &Scope<'_>,
    errors: &mut Vec<Diagnostic>,
) -> ResolvedBody {
    let Some(body) = body else {
        check_required_ports(class, &[], at, errors);
        return ResolvedBody::default();
    };
    let body = body.into_value();
    let (attrs, mut refused) = instance_attrs(&body.attrs, place, scope, errors);
    let mut bindings = Vec::new();
    let mut named_ports = Vec::new();
    let bound = check_unique(
        body.bindings.iter().enumerate().map(|(at, binding)| {
            (
                bound_port(binding).value().clone(),
                at,
                bound_port(binding).span(),
            )
        }),
        |port, _, at, _, first| {
            two_site(
                format!("this instance binds port `{port}` twice; a port is wired once"),
                at.clone(),
                "first bound here",
                first.clone(),
            )
        },
        errors,
    );
    let kept: HashSet<usize> = bound.into_values().map(|(at, _)| at).collect();
    // A binding that could not be resolved leaves the instance wired to less
    // than it declared, so it withholds the instance the way a refused value
    // does. A port bound twice is the same: the repeat is dropped, the port is
    // still claimed, and the required-port rule does not answer the mistake a
    // second time.
    for (at, binding) in body.bindings.into_iter().enumerate() {
        named_ports.push(bound_port(&binding).value().clone());
        if !kept.contains(&at) {
            refused.drop_part();
            continue;
        }
        match resolve_binding(binding, class, scope, errors) {
            Some(binding) => bindings.push(binding),
            None => refused.drop_part(),
        }
    }
    // Both placements: an instance is a principal in its own right wherever it
    // is placed. A surface component's authority contains the component within
    // the surface; the surface's own is what the backend admits over the wire.
    let resolved = emit_acls(body.acls, scope, errors, &mut refused);
    // A `tool` statement is authority over the registry, which only the backend
    // host reaches: on a surface the statement is refused, and the instance is
    // withheld the way any other refused part of its body withholds it.
    let tools = match place {
        Placement::Surface => {
            if !body.blocks.is_empty() {
                refuse_surface_blocks(&body.blocks, errors);
                refused.drop_part();
            }
            Vec::new()
        }
        Placement::TopLevel => {
            emit_tool_grants(&body.blocks, "an instance", scope, errors, &mut refused)
        }
    };
    check_required_ports(class, &named_ports, at, errors);
    ResolvedBody {
        attrs,
        bindings,
        acls: resolved,
        tools,
        refused,
    }
}

/// The port a binding statement names, whichever direction it faces.
fn bound_port(binding: &BindStmt) -> &Spanned<String> {
    match binding {
        BindStmt::Into(bound) => &bound.port,
        BindStmt::Outof(bound) => &bound.port,
        BindStmt::Both(bound) => &bound.port,
    }
}

/// Every port the class does not mark `optional` must be named by a binding.
///
/// The class states the contract; an instance that leaves a required port
/// unwired is a component that cannot do its job, not a component with less
/// wiring. A free `io port { … }` tuning counts: the port is claimed, even
/// though it connects to nothing but its own page-local ring.
///
/// `named_ports` is every port a binding statement *named*, resolved or not: a
/// binding dropped for an unreadable channel still claims its port, and
/// reporting it unconnected too would answer one mistake twice. A name that
/// matches no declared port is refused on its own by `check_port` and satisfies
/// nothing here.
fn check_required_ports(
    class: &ClassRef,
    named_ports: &[String],
    at: &Span,
    errors: &mut Vec<Diagnostic>,
) {
    for port in &class.ports {
        if port.optional || named_ports.iter().any(|named| named == port.name.value()) {
            continue;
        }
        errors.push(two_site(
            format!(
                "this instance leaves port `{}` of `{}` unconnected; bind it, or the \
                 class declares it `optional`",
                port.name.value(),
                class.name.value()
            ),
            at.clone(),
            "the port is declared here",
            port.name.span().clone(),
        ));
    }
}

/// An instance body's attrs, against the closed key set its placement admits.
fn instance_attrs(
    attrs: &AttrMap,
    place: Placement,
    scope: &Scope<'_>,
    errors: &mut Vec<Diagnostic>,
) -> (Vec<(String, RVal)>, Refused) {
    let legal = place.keys();
    let mut refused = Refused::default();
    let mut resolved = Vec::new();
    for (key, value) in attrs.entries() {
        // The token contexts were projected before this walk: resolving one as
        // a value would read its bare words as names.
        if place.projected_keys().contains(&key.as_str()) {
            continue;
        }
        if !legal.contains(&key.as_str()) {
            errors.push(Diagnostic::at(
                format!(
                    "`{key}` is not a key of {}; expected one of {}",
                    place.describe(),
                    legal.join(", ")
                ),
                value.span().clone(),
            ));
            continue;
        }
        // A refused value keeps its key, holding a substitute: dropping the
        // key would leave a later check reading the body as if it had never
        // been written — an absent `slug` is a *different* configuration, not
        // a missing one.
        match resolve_value(value, scope) {
            Ok(value) => resolved.push((key.clone(), value)),
            Err(error) => {
                errors.push(error);
                let substitute = refused.substitute(value.span().clone());
                resolved.push((key.clone(), substitute));
            }
        }
    }
    (resolved, refused)
}

/// One binding, once the port it names is one the class declares in that
/// direction.
fn resolve_binding(
    binding: BindStmt,
    class: &ClassRef,
    scope: &Scope<'_>,
    errors: &mut Vec<Diagnostic>,
) -> Option<RBinding> {
    match binding {
        BindStmt::Into(bound) => {
            check_port(class, &bound.port, PortDir::In, errors)?;
            let chan = bound_chan(Some(&bound.chan), scope, errors)?;
            let (tail, refused) = typed_tail(bound.tail, InTail::empty, scope, errors);
            bound_binding(bound.port, chan, RTail::In(tail), &refused)
        }
        BindStmt::Outof(bound) => {
            check_port(class, &bound.port, PortDir::Out, errors)?;
            let chan = bound_chan(Some(&bound.chan), scope, errors)?;
            let (tail, refused) = typed_tail(bound.tail, OutTail::empty, scope, errors);
            bound_binding(bound.port, chan, RTail::Out(tail), &refused)
        }
        BindStmt::Both(bound) => {
            check_port(class, &bound.port, PortDir::Io, errors)?;
            let chan = bound_chan(bound.target.as_ref(), scope, errors)?;
            let (tail, refused) = typed_tail(bound.tail, IoTail::empty, scope, errors);
            bound_binding(bound.port, chan, RTail::Io(Box::new(tail)), &refused)
        }
    }
}

/// The channel a binding names, or `None` on a free `io` port.
fn bound_chan(
    chan: Option<&ChanRef>,
    scope: &Scope<'_>,
    errors: &mut Vec<Diagnostic>,
) -> Option<Option<RChanRef>> {
    match chan {
        None => Some(None),
        Some(reference) => match resolve_chan_ref(reference, scope) {
            Ok(resolved) => Some(Some(resolved)),
            Err(error) => {
                errors.push(error);
                None
            }
        },
    }
}

/// The binding, unless one of its tail values was refused: a half-read tail
/// would leave the port tuned differently than the document says.
fn bound_binding(
    port: Spanned<String>,
    chan: Option<RChanRef>,
    tail: RTail,
    refused: &Refused,
) -> Option<RBinding> {
    match refused.any() {
        true => None,
        false => Some(RBinding { port, chan, tail }),
    }
}

/// Refuse a binding whose port the class does not declare, or declares facing
/// the other way.
///
/// The direction has to match exactly: an `io` port bound as `in` would connect
/// one half of a port the class expects to drive both ways. Loosening this
/// later is compatible; guessing now is not.
fn check_port(
    class: &ClassRef,
    port: &Spanned<String>,
    dir: PortDir,
    errors: &mut Vec<Diagnostic>,
) -> Option<()> {
    match class
        .ports
        .iter()
        .find(|declared| declared.name.value() == port.value())
    {
        None => {
            errors.push(two_site(
                format!(
                    "`{}` declares no port `{}`; it declares {}",
                    class.name.value(),
                    port.value(),
                    port_list(class)
                ),
                port.span().clone(),
                "the class is declared here",
                class.name.span().clone(),
            ));
            None
        }
        Some(declared) if declared.dir != dir => {
            errors.push(two_site(
                format!(
                    "port `{}` is an `{}` port, bound as `{}`",
                    port.value(),
                    declared.dir.as_str(),
                    dir.as_str()
                ),
                port.span().clone(),
                "declared here",
                declared.name.span().clone(),
            ));
            None
        }
        Some(_) => Some(()),
    }
}

/// The ports a class declares, for the message that says a name is not one.
fn port_list(class: &ClassRef) -> String {
    if class.ports.is_empty() {
        return "none".to_string();
    }
    class
        .ports
        .iter()
        .map(|port| format!("`{} {}`", port.dir.as_str(), port.name.value()))
        .collect::<Vec<_>>()
        .join(", ")
}

/// What a binding names: a declared channel, a declared link, or a literal
/// address where no declaration exists.
fn resolve_chan_ref(chan: &ChanRef, scope: &impl ValueScope) -> Result<RChanRef, Diagnostic> {
    match chan {
        ChanRef::Handle(path) => scope.lookup_chan_target(path, path.head.span()),
        ChanRef::Addr(text) => {
            let span = str_like_span(text);
            let address = resolve_str_like(text.value(), scope)?;
            check_scheme(&address, &span)?;
            Ok(RChanRef::Addr(Spanned::new(address, span)))
        }
    }
}

/// Where a channel reference was written.
fn chan_ref_span(chan: &ChanRef) -> Span {
    match chan {
        ChanRef::Handle(path) => path.head.span().clone(),
        ChanRef::Addr(text) => str_like_span(text),
    }
}

/// The runtime's instance charset, and uniqueness within the surface.
///
/// A component instance has no `slug` spelling: the handle is the name the
/// runtime uses, so the handle is what has to be legal.
fn check_instance_name(
    handle: &Spanned<String>,
    siblings: &[RComponentInst],
    errors: &mut Vec<Diagnostic>,
) {
    let name = handle.value();
    if !is_kebab(name) || name.contains("--") {
        errors.push(Diagnostic::at(
            format!(
                "`{name}` is not a legal component instance name (lowercase, digits and \
                 single `-`, starting with a letter or digit)"
            ),
            handle.span().clone(),
        ));
    }
    if let Some(prior) = siblings
        .iter()
        .find(|sibling| sibling.instance.value() == name)
    {
        errors.push(two_site(
            format!("this surface already has a component `{name}`"),
            handle.span().clone(),
            "first written here",
            prior.instance.span().clone(),
        ));
    }
}

// ── pass 4c: agent instantiation ─────────────────────────────────────────────
//
// An agent class takes parameters, so an instance of one is expanded rather
// than read: the arguments bind the parameters, and the class body resolves
// under a scope in which those bindings are what its references name. The class
// itself never reaches the resolved config — what a document says about an
// agent is what its instantiations stamped.

/// Every template of one kind in the document, found where it was declared.
///
/// Held behind an [`Rc`] because a template is a template: it outlives the item
/// list emission consumes, and every instantiation reads the same body.
type TemplateTable<T> = HashMap<(usize, usize), Rc<T>>;

/// Collect one kind of template before emission consumes the items around them.
fn templates<T: Clone>(
    modules: &[Vec<Spanned<Item>>],
    extract: impl Fn(&Item) -> Option<&T>,
) -> TemplateTable<T> {
    let mut table = TemplateTable::new();
    for (position, items) in modules.iter().enumerate() {
        for (offset, item) in items.iter().enumerate() {
            if let Some(template) = extract(item.value()) {
                table.insert((position, offset), Rc::new(template.clone()));
            }
        }
    }
    table
}

type AgentTable = TemplateTable<AgentClass>;

/// Collect the agent classes before emission consumes the items around them.
fn agent_classes(modules: &[Vec<Spanned<Item>>]) -> AgentTable {
    templates(modules, |item| match item {
        Item::Agent(class) => Some(&**class),
        _ => None,
    })
}

/// What an argument bound a parameter to.
///
/// A value parameter carries the resolved value; an entity parameter carries
/// the identity of the entity it named, because that is what the body does with
/// it — mounts a repo, subscribes to a channel, grants to an agent.
#[derive(Clone)]
enum ParamVal {
    Value(RVal),
    Chan(ChanId),
    Agent(HandlePath),
    Repo(HandlePath),
}

impl ParamVal {
    /// What this is, for a diagnostic that has to say what a name reached.
    fn kind(&self) -> &'static str {
        match self {
            ParamVal::Value(_) => "a value",
            ParamVal::Chan(_) => "a channel",
            ParamVal::Agent(_) => "an agent",
            ParamVal::Repo(_) => "a repo",
        }
    }
}

/// The bindings one instantiation made.
type ParamBindings = HashMap<String, ParamVal>;

/// A top-level `new`, dispatched on what its class is.
///
/// The three class kinds instantiate differently enough that the dispatch is
/// worth doing once, up front: a component is read, an agent is expanded, and
/// an assembly stamps a whole entity set.
fn emit_inst(
    inst: NewStmt,
    scope: &Scope<'_>,
    classes: &ClassTable,
    agents: &AgentTable,
    config: &mut Emitted,
    errors: &mut Vec<Diagnostic>,
) {
    let span = inst.cls.head.span().clone();
    let handle = scope.handle(inst.handle.clone());
    let symbol = match scope.class(&inst.cls, &span) {
        Ok((_, symbol)) => symbol,
        // The consumer path reports it: it resolves the class the same way, and
        // reporting here would say the same thing twice.
        Err(_) => {
            config.withhold(&handle, Grantable::Yes);
            return emit_consumer(inst, scope, classes, config, errors);
        }
    };
    match symbol.kind {
        SymKind::AgentClass => {
            // Declared until emitted: the emitter clears it on the one path
            // that pushes, so every early return leaves it registered.
            config.withhold(&handle, Grantable::Yes);
            emit_agent(inst, &symbol, agents, scope, config, errors);
        }
        // An assembly stamped its entity set in pass 4d, and its items are
        // emitted from the list that pass left behind. The assembly handle is
        // no entity of its own, so there is nothing to withhold.
        SymKind::Assembly => {}
        _ => {
            config.withhold(&handle, Grantable::Yes);
            emit_consumer(inst, scope, classes, config, errors);
        }
    }
}

/// One agent instantiation, expanded into the agent it stamps.
fn emit_agent(
    inst: NewStmt,
    symbol: &Symbol,
    agents: &AgentTable,
    scope: &Scope<'_>,
    config: &mut Emitted,
    errors: &mut Vec<Diagnostic>,
) {
    let Some(class) = agents.get(&(symbol.file, symbol.item)) else {
        return;
    };
    if let Some(body) = &inst.body {
        errors.push(two_site(
            "an agent instantiation takes arguments, not a body; per-instance values \
             are class parameters",
            body.span().clone(),
            "the class is declared here",
            class.name.span().clone(),
        ));
        return;
    }
    let Some(params) = bind_args(
        class.params.as_ref(),
        inst.args.as_ref(),
        class.name.value(),
        inst.handle.span(),
        scope,
        errors,
    ) else {
        return;
    };
    // The class body resolves in the file that declared it, not the file that
    // instantiated it: a class means what it meant where it was written.
    // Nothing the instantiating body stamped is visible in the class body: what
    // an instantiation gives a class is its arguments.
    let pscope = Scope {
        outer: FileScope::in_file(
            scope.outer.index,
            symbol.file,
            scope.outer.channels,
            scope.outer.links,
            scope.outer.stamps,
        ),
        params: Some(&params),
        prefix: None,
        root: symbol.file,
    };
    let class = (**class).clone();
    let handle = scope.handle(inst.handle.clone());
    let (attrs, mut refused) = resolve_attrs(class.attrs, &pscope, errors);
    let (slug, checkable) = slug_position(
        attrs.slug.as_ref().map(|attr| &attr.value),
        &refused,
        &handle,
        inst.handle.span(),
        errors,
    );
    // The statement halves of the body read none of the attrs, so they are
    // checked whatever the attrs did; the agent itself is withheld when
    // anything in its body — an attr value or a statement the body could not
    // resolve — did not come out whole.
    let mounts = emit_mounts(class.mounts, &pscope, errors, &mut refused);
    let mcps = emit_mcps(class.mcps, &pscope, errors, &mut refused);
    let subs = emit_subs(class.subs, &pscope, errors, &mut refused);
    let acls = emit_acls(class.acls, &pscope, errors, &mut refused);
    // One walk over the body's sub-blocks, whatever their kindwords: the
    // multiplicity check counts them together, so they cannot be gathered by
    // two walks that disagree about what a duplicate is.
    let blocks = emit_blocks(&class.blocks, &pscope, errors, &mut refused);
    let agent = RAgent {
        handle,
        slug,
        class: class.name.clone(),
        attrs,
        mounts,
        mcps,
        subs,
        acls,
        hooks: blocks.hooks,
        attachment_targets: blocks.attachment_targets,
        integration_configs: blocks.integration_configs,
        tools: blocks.tools,
        doc: inst.doc,
    };
    if refused.any() {
        if checkable {
            check_charset(&agent.slug, Family::Agent, errors);
        }
        config.withhold(&agent.handle, Grantable::Yes);
    } else {
        config.emitted(&agent.handle);
        config.agents.push(agent);
    }
}

/// Bind one instantiation's arguments to its class's parameters.
///
/// Every refusal is collected rather than returned: an instantiation with two
/// wrong arguments should say both. `None` where the bindings are incomplete,
/// because expanding a body against a parameter that was never bound would
/// report the same mistake once per use.
fn bind_args(
    params: Option<&ParamList>,
    args: Option<&ArgList>,
    class: &str,
    site: &Span,
    scope: &Scope<'_>,
    errors: &mut Vec<Diagnostic>,
) -> Option<ParamBindings> {
    let declared: &[Param] = params.map_or(&[][..], |list| list.params.as_slice());
    let written: &[Arg] = args.map_or(&[][..], |list| list.args.as_slice());
    let mut ok = true;
    let mut seen: HashMap<&str, &Spanned<String>> = HashMap::new();
    for arg in written {
        let name = arg.name.value().as_str();
        if let Some(first) = seen.get(name) {
            errors.push(two_site(
                format!("argument `{name}` is written twice"),
                arg.name.span().clone(),
                "the first one",
                first.span().clone(),
            ));
            ok = false;
            continue;
        }
        if !declared.iter().any(|param| param.name.value() == name) {
            errors.push(Diagnostic::at(
                format!(
                    "`{class}` has no parameter `{name}`; it takes {}",
                    param_list(declared)
                ),
                arg.name.span().clone(),
            ));
            ok = false;
            continue;
        }
        seen.insert(name, &arg.name);
    }
    let mut bindings = ParamBindings::new();
    for param in declared {
        let name = param.name.value();
        let written = written.iter().find(|arg| arg.name.value() == name);
        let value = match (written, param.default.as_ref()) {
            (Some(arg), _) => &arg.value,
            (None, Some(default)) => default,
            (None, None) => {
                errors.push(two_site(
                    format!(
                        "`{class}` takes `{name}`, and this instantiation states no value for it"
                    ),
                    site.clone(),
                    "the parameter",
                    param.name.span().clone(),
                ));
                ok = false;
                continue;
            }
        };
        let Some(ty) = ParamType::parse(param.ty.value()) else {
            // The definition site already refused the type; saying so again at
            // every instantiation would say it once per use.
            ok = false;
            continue;
        };
        match bind_one(param, ty, value, scope) {
            Ok(bound) => {
                bindings.insert(name.clone(), bound);
            }
            Err(error) => {
                errors.push(error);
                ok = false;
            }
        }
    }
    ok.then_some(bindings)
}

/// One argument, against the type its parameter declared.
///
/// An entity parameter is checked before the value walk rather than after: what
/// makes an argument a channel is that it *names* one, and a resolved value has
/// no name left in it.
fn bind_one(
    param: &Param,
    ty: ParamType,
    value: &Spanned<Value>,
    scope: &Scope<'_>,
) -> Result<ParamVal, Diagnostic> {
    if ty.is_entity() {
        let Value::Ref(path) = value.value() else {
            return Err(Diagnostic::at(
                format!(
                    "parameter `{}` is a `{}`; name one, rather than writing {}",
                    param.name.value(),
                    ty.as_str(),
                    value_shape(value.value())
                ),
                value.span().clone(),
            ));
        };
        return bind_entity(param, ty, path, value.span(), scope);
    }
    let resolved = resolve_value(value, scope)?;
    let matches = match (ty, resolved.value()) {
        (ParamType::String, RValue::Str(_))
        | (ParamType::Int, RValue::Int(_))
        | (ParamType::Bool, RValue::Bool(_))
        | (ParamType::Table, RValue::Table(_)) => true,
        (ParamType::String | ParamType::Int | ParamType::Bool | ParamType::Table, _) => false,
        // An entity type took the branch above.
        (ParamType::Channel | ParamType::Agent | ParamType::Repo, _) => unreachable!(),
    };
    if !matches {
        return Err(Diagnostic::at(
            format!(
                "parameter `{}` is a `{}`; this is {}",
                param.name.value(),
                ty.as_str(),
                resolved.value().kind()
            ),
            resolved.span().clone(),
        ));
    }
    Ok(ParamVal::Value(resolved))
}

/// An entity argument: the declaration it names, once that declaration is one
/// the parameter's type admits.
fn bind_entity(
    param: &Param,
    ty: ParamType,
    path: &PathRef,
    span: &Span,
    scope: &Scope<'_>,
) -> Result<ParamVal, Diagnostic> {
    if ty == ParamType::Channel {
        return Ok(ParamVal::Chan(scope.lookup_channel(path, span)?));
    }
    // An enclosing body passes its own parameter on: the binding already holds
    // the handle the outer argument named, so it travels as it is.
    if let Some(bound) = scope.param(path) {
        if let Some(segment) = Scope::segments(path).first() {
            return Err(no_such_segment(
                path.head.value(),
                Some("parameter"),
                segment,
            ));
        }
        return match (ty, bound) {
            (ParamType::Agent, ParamVal::Agent(handle)) => Ok(ParamVal::Agent(handle.clone())),
            (ParamType::Repo, ParamVal::Repo(handle)) => Ok(ParamVal::Repo(handle.clone())),
            (_, other) => Err(Diagnostic::at(
                format!(
                    "parameter `{}` is a `{}`; parameter `{}` names {}",
                    param.name.value(),
                    ty.as_str(),
                    path.head.value(),
                    other.kind()
                ),
                span.clone(),
            )),
        };
    }
    let (symbol, name, rest) = scope.symbol(path, span)?;
    if let Some(segment) = rest.first() {
        if symbol.kind == SymKind::Instance {
            let handle = dotted_handle(&name, path.head.span(), &rest);
            return stamped_entity(param, ty, &symbol, handle, span, scope);
        }
        return Err(no_such_segment(&name, None, segment));
    }
    let handle = HandlePath(vec![Spanned::new(name.clone(), span.clone())]);
    let bound = match ty {
        ParamType::Agent if symbol.kind == SymKind::Instance => {
            instance_is_an_agent(param, &name, &symbol, scope, span)?;
            ParamVal::Agent(handle)
        }
        ParamType::Repo if symbol.kind == SymKind::Repo => ParamVal::Repo(handle),
        ParamType::Agent | ParamType::Repo => {
            return Err(two_site(
                format!(
                    "parameter `{}` is a `{}`; `{name}` is {}",
                    param.name.value(),
                    ty.as_str(),
                    symbol.kind.describe()
                ),
                span.clone(),
                "declared here",
                symbol.span.clone(),
            ));
        }
        // `Channel` returned above; a value type never reaches this function.
        ParamType::Channel
        | ParamType::String
        | ParamType::Int
        | ParamType::Bool
        | ParamType::Table => unreachable!(),
    };
    Ok(bound)
}

/// The handle a reference reaching under an instance's handle names.
///
/// One spelling of "how a dotted handle is written", shared with the write side
/// through [`HandlePath::dotted`] — a separator or a normalisation that changed
/// in one place and not the other would make lookups miss silently.
fn dotted_handle(head: &str, head_span: &Span, rest: &[&Spanned<String>]) -> HandlePath {
    let mut handle = HandlePath(vec![Spanned::new(head.to_string(), head_span.clone())]);
    for segment in rest {
        handle = handle.child((*segment).clone());
    }
    handle
}

/// An entity argument reaching under an instance's handle: the entity that
/// instantiation stamped.
///
/// An assembly's agent is named by exactly the dotted handle its own identity
/// is spelled with, the way a stamped channel is. Repos are the one kind with
/// no stamped case — `assembly_item` admits no `repo`, so nothing can stamp
/// one — and reaching under an instance for one names nothing.
fn stamped_entity(
    param: &Param,
    ty: ParamType,
    symbol: &Symbol,
    handle: HandlePath,
    span: &Span,
    scope: &Scope<'_>,
) -> Result<ParamVal, Diagnostic> {
    let dotted = handle.dotted();
    match ty {
        ParamType::Agent => {}
        ParamType::Repo => {
            return Err(Diagnostic::at(
                format!(
                    "parameter `{}` is a `Repo`; `{dotted}` is stamped by an instantiation, \
                     and an instantiation stamps no repo",
                    param.name.value()
                ),
                span.clone(),
            ));
        }
        // A channel is resolved before this, and a value type never names an
        // entity; a new entity type lands here as a compile error rather than
        // as a sentence about repos.
        ParamType::Channel
        | ParamType::String
        | ParamType::Int
        | ParamType::Bool
        | ParamType::Table => unreachable!("`{}` is not an entity type", ty.as_str()),
    }
    match scope.outer.stamps.get(symbol.file, &dotted) {
        Some(StampKind::Agent) => Ok(ParamVal::Agent(handle)),
        Some(other) => Err(Diagnostic::at(
            format!(
                "parameter `{}` is an `Agent`; `{dotted}` is {}",
                param.name.value(),
                other.describe()
            ),
            span.clone(),
        )),
        // A stamped channel is recorded in the channel table rather than in
        // the stamp table, so the miss is probed there before it is reported
        // as nothing at all.
        None if scope.outer.channels.get(symbol.file, &dotted).is_some() => Err(Diagnostic::at(
            format!(
                "parameter `{}` is an `Agent`; `{dotted}` is a channel",
                param.name.value()
            ),
            span.clone(),
        )),
        None => Err(Diagnostic::at(
            format!("`{}` stamps no entity `{dotted}`", handle.0[0].value()),
            span.clone(),
        )),
    }
}

/// An `Agent` argument names an instantiation of an agent class, not of an
/// assembly or a component class.
///
/// Every top-level `new` mints the same kind of symbol, so what it instantiates
/// is only knowable through its class — resolved in the file that wrote the
/// `new`, which is where its class name means something.
fn instance_is_an_agent(
    param: &Param,
    name: &str,
    symbol: &Symbol,
    scope: &Scope<'_>,
    span: &Span,
) -> Result<(), Diagnostic> {
    let Some(path) = &symbol.class else {
        return Ok(());
    };
    let declaring = FileScope::in_file(
        scope.outer.index,
        symbol.file,
        scope.outer.channels,
        scope.outer.links,
        scope.outer.stamps,
    );
    // A class that does not resolve is refused where the instantiation is
    // expanded; here it says nothing about the argument.
    let Ok((class, class_symbol)) = declaring.class(path, &symbol.span) else {
        return Ok(());
    };
    if class_symbol.kind == SymKind::AgentClass {
        return Ok(());
    }
    Err(two_site(
        format!(
            "parameter `{}` is an `Agent`; `{name}` instantiates `{class}`, which is {}",
            param.name.value(),
            class_symbol.kind.describe()
        ),
        span.clone(),
        "the instantiation",
        symbol.span.clone(),
    ))
}

/// The parameters a class takes, for the message that says a name is not one.
fn param_list(params: &[Param]) -> String {
    if params.is_empty() {
        return "none".to_string();
    }
    params
        .iter()
        .map(|param| format!("`{}: {}`", param.name.value(), param.ty.value()))
        .collect::<Vec<_>>()
        .join(", ")
}

/// What was written where an entity was to be named.
fn value_shape(value: &Value) -> &'static str {
    match value {
        Value::Ref(_) => "a reference",
        Value::Fstr(_) => "an f-string",
        Value::Str(_) | Value::Raw(_) => "a string",
        Value::Int(_) => "an integer",
        Value::Flt(_) => "a float",
        Value::Bool(_) => "a boolean",
        Value::List(_) => "a list",
        Value::Table(_) => "a table",
        Value::M(_) => "a matcher",
    }
}

/// An agent's `mount` statements: each repo resolved to its handle.
fn emit_mounts(
    mounts: Vec<MountStmt>,
    scope: &Scope<'_>,
    errors: &mut Vec<Diagnostic>,
    refused: &mut Refused,
) -> Vec<RMount> {
    let mut resolved = Vec::new();
    for mount in mounts {
        let span = mount.repo.head.span().clone();
        // The tail resolves before the repo does, so a mount naming neither a
        // repo that exists nor a value that resolves reports both in one
        // compile rather than one per run.
        let (tail, tail_refused) = typed_tail(mount.tail, MountTail::empty, scope, errors);
        let repo = match resolve_repo(&mount.repo, scope) {
            Ok(handle) => handle,
            Err(error) => {
                errors.push(error);
                refused.drop_part();
                continue;
            }
        };
        if tail_refused.any() {
            refused.drop_part();
            continue;
        }
        resolved.push(RMount {
            repo_span: Spanned::new(mount.repo.head.value().clone(), span),
            repo,
            tail,
        });
    }
    resolved
}

/// What a `mount` names: a `repo` declaration, or a `Repo` parameter bound to
/// one.
fn resolve_repo(path: &PathRef, scope: &Scope<'_>) -> Result<HandlePath, Diagnostic> {
    let span = path.head.span().clone();
    if let Some(bound) = scope.param(path) {
        return match bound {
            ParamVal::Repo(handle) => Ok(handle.clone()),
            other => Err(Diagnostic::at(
                format!(
                    "parameter `{}` names {}, not a repo",
                    path.head.value(),
                    other.kind()
                ),
                span,
            )),
        };
    }
    let (symbol, name, rest) = scope.symbol(path, &span)?;
    if let Some(segment) = rest.first() {
        return Err(no_such_segment(&name, None, segment));
    }
    if symbol.kind != SymKind::Repo {
        return Err(two_site(
            format!(
                "a mount names a repo; `{name}` is {}",
                symbol.kind.describe()
            ),
            span,
            "declared here",
            symbol.span.clone(),
        ));
    }
    Ok(HandlePath(vec![Spanned::new(name, span)]))
}

/// An agent's `mcp_server` statements: a reference to a top-level definition,
/// or a definition of its own.
fn emit_mcps(
    mcps: Vec<McpServerStmt>,
    scope: &Scope<'_>,
    errors: &mut Vec<Diagnostic>,
    refused: &mut Refused,
) -> Vec<RMcp> {
    let mut resolved = Vec::new();
    for stmt in mcps {
        match stmt {
            McpServerStmt::Ref(name) => match scope.named(&name) {
                Ok(symbol) if symbol.kind == SymKind::McpServer => {
                    resolved.push(RMcp::Ref(name));
                }
                Ok(symbol) => {
                    errors.push(two_site(
                        format!(
                            "`{}` names {}, not an mcp server; write a body to define one here",
                            name.value(),
                            symbol.kind.describe()
                        ),
                        name.span().clone(),
                        "declared here",
                        symbol.span.clone(),
                    ));
                    refused.drop_part();
                }
                Err(error) => {
                    errors.push(error);
                    refused.drop_part();
                }
            },
            McpServerStmt::Inline(def) => {
                let NamedAttrDef { doc, name, body } = *def;
                // The one place the language defines a name inside a body, and
                // the no-shadowing rule reaches it too: a definition here that
                // repeats a name the file already reaches is two things with
                // one spelling.
                if let Ok(symbol) = scope.named(&name) {
                    errors.push(two_site(
                        format!(
                            "`{}` is already {}; nothing shadows here",
                            name.value(),
                            symbol.kind.describe()
                        ),
                        name.span().clone(),
                        "the declaration it collides with",
                        symbol.span.clone(),
                    ));
                    refused.drop_part();
                    continue;
                }
                let (attrs, inline_refused) = resolve_attrs(body.attrs, scope, errors);
                match inline_refused.any() {
                    true => refused.drop_part(),
                    false => resolved.push(RMcp::Inline(Box::new(RNamed {
                        handle: HandlePath(vec![name]),
                        attrs,
                        doc,
                    }))),
                }
            }
        }
    }
    resolved
}

/// An agent's `subscribe` statements.
fn emit_subs(
    subs: Vec<SubscribeStmt>,
    scope: &Scope<'_>,
    errors: &mut Vec<Diagnostic>,
    refused: &mut Refused,
) -> Vec<RSubscribe> {
    let mut resolved = Vec::new();
    for sub in subs {
        let (tail, tail_refused) = typed_tail(sub.tail, SubscribeTail::empty, scope, errors);
        match resolve_chan_ref(&sub.chan, scope) {
            // A link is wired port to port; a conversation holds no port, so
            // there is nothing on this side for a link to reach.
            Ok(RChanRef::Link(_)) => {
                errors.push(Diagnostic::at(
                    "a `subscribe` statement names a channel, and this names a link: a link \
                     connects ports, and an agent has none",
                    chan_ref_span(&sub.chan),
                ));
                refused.drop_part();
            }
            Ok(chan) if !tail_refused.any() => resolved.push(RSubscribe {
                chan,
                span: chan_ref_span(&sub.chan),
                tail,
            }),
            // Every key of `SubscribeTail` is a projection, so nothing in it
            // can fail to resolve and this arm cannot be reached today. It
            // withholds the statement rather than half-reading it the day the
            // vocabulary gains a value key.
            Ok(_) => refused.drop_part(),
            Err(error) => {
                errors.push(error);
                refused.drop_part();
            }
        }
    }
    resolved
}

/// What an agent body's sub-blocks say, gathered by kindword.
struct AgentBlocks {
    hooks: Vec<RHooks>,
    attachment_targets: Vec<RAttachmentTarget>,
    integration_configs: Vec<RSection>,
    tools: Vec<RToolGrant>,
}

/// An agent's sub-blocks, typed by their kindword.
///
/// At most one block per `(kindword, name)` — two `start_hooks` are two answers
/// to one question, two `attachment_target import` are two definitions of one
/// upload affordance, and two `integration_config ledger` are two override
/// trees for one map key. Blocks under different names are the normal case: the
/// config fields are a list and a map.
fn emit_blocks(
    blocks: &[SectionNode],
    scope: &Scope<'_>,
    errors: &mut Vec<Diagnostic>,
    withhold: &mut Refused,
) -> AgentBlocks {
    let mut resolved = AgentBlocks {
        hooks: Vec::new(),
        attachment_targets: Vec::new(),
        integration_configs: Vec::new(),
        tools: Vec::new(),
    };
    let duplicates = duplicate_sections(
        blocks.iter().enumerate(),
        "an agent",
        crate::model::AGENT_BLOCK_KINDWORDS,
        errors,
    );
    for (offset, node) in blocks.iter().enumerate() {
        if duplicates.contains(&offset) {
            withhold.drop_part();
            continue;
        }
        let block = match crate::model::agent_block(node) {
            Ok(block) => block,
            Err(error) => {
                errors.push(error);
                withhold.drop_part();
                continue;
            }
        };
        match block {
            // The three hook kindwords carry one vocabulary; what the block is,
            // is the word it led with.
            AgentBlock::StartHooks(block)
            | AgentBlock::PostPullHooks(block)
            | AgentBlock::StartupHooks(block) => {
                let block = *block;
                // Ordered before the body's values: a nested block is a
                // separate mistake from a value it could not resolve.
                refuse_subs(block.kindword.value(), &block.subs, errors);
                let (attrs, refused) = resolve_attrs(block.attrs, scope, errors);
                match refused.any() {
                    true => withhold.drop_part(),
                    false => resolved.hooks.push(RHooks {
                        kindword: block.kindword,
                        host: attrs.host.map(|attr| attr.value),
                        container: attrs.container.map(|attr| attr.value),
                    }),
                }
            }
            // Two `tool` blocks under one name are already refused above, by
            // the same rule that refuses two `start_hooks`.
            AgentBlock::Tool(block) => match emit_tool_grant(*block, scope, errors) {
                Some(grant) => resolved.tools.push(grant),
                None => withhold.drop_part(),
            },
            AgentBlock::AttachmentTarget(block) => {
                match emit_attachment_target(*block, scope, errors) {
                    Some(target) => resolved.attachment_targets.push(target),
                    None => withhold.drop_part(),
                }
            }
            // An open body with nothing to check: every key is legal, every
            // value is carried, and what the integration makes of the tree is
            // the integration's business at boot.
            AgentBlock::IntegrationConfig(block) => {
                let block = *block;
                refuse_subs(block.kindword.value(), &block.subs, errors);
                let (attrs, refused) = resolve_attrs(block.attrs, scope, errors);
                match refused.any() {
                    true => withhold.drop_part(),
                    false => resolved.integration_configs.push(RSection {
                        kindword: block.kindword,
                        name: block.name,
                        attrs: attrs.entries(),
                        subs: Vec::new(),
                        doc: block.doc,
                    }),
                }
            }
        }
    }
    resolved
}

/// One `attachment_target` block and the `handler` block it holds.
///
/// `None` where either half did not come out whole: the target is then withheld
/// and its diagnostics stand. This layer checks only that a handler was written
/// at all; which fields the handler type requires is lowering's concern.
fn emit_attachment_target(
    block: TypedBlock<AttachmentTargetAttrs>,
    scope: &Scope<'_>,
    errors: &mut Vec<Diagnostic>,
) -> Option<RAttachmentTarget> {
    let TypedBlock {
        doc,
        kindword,
        name,
        attrs,
        subs,
    } = block;
    let (attrs, refused) = resolve_attrs(attrs, scope, errors);
    // Walked whatever the body did: an operator fixing one error at a time is
    // what a compiler that reports the whole block exists to prevent.
    let handler = emit_handler(&subs, &kindword, scope, errors);
    if refused.any() {
        return None;
    }
    Some(RAttachmentTarget {
        kindword,
        name,
        attrs: attrs.entries(),
        subs: vec![handler?],
        doc,
    })
}

/// The `handler` block of an attachment target.
fn emit_handler(
    subs: &[SectionNode],
    parent: &Spanned<String>,
    scope: &Scope<'_>,
    errors: &mut Vec<Diagnostic>,
) -> Option<RSection> {
    let duplicates = duplicate_sections(
        subs.iter().enumerate(),
        "an attachment target",
        crate::model::ATTACHMENT_BLOCK_KINDWORDS,
        errors,
    );
    let mut resolved = None;
    for (offset, node) in subs.iter().enumerate() {
        if duplicates.contains(&offset) {
            continue;
        }
        let block = match crate::model::attachment_block(node) {
            Ok(block) => block,
            Err(error) => {
                errors.push(error);
                continue;
            }
        };
        let (parts, refused) = resolve_attrs(block, scope, errors);
        let parts = parts.into_parts();
        // A handler block nests nothing, so a section written inside one has no
        // vocabulary to be checked against and no reader.
        refuse_subs(parts.kindword.value(), &parts.subs, errors);
        if refused.any() {
            continue;
        }
        resolved = Some(RSection {
            kindword: parts.kindword,
            name: parts.name,
            attrs: parts.attrs,
            subs: Vec::new(),
            doc: parts.doc,
        });
    }
    if resolved.is_none() && subs.is_empty() {
        errors.push(Diagnostic::at(
            "an `attachment_target` states no `handler` block: what an upload does \
             has no default",
            parent.span().clone(),
        ));
    }
    resolved
}

/// The `tool` statements a body holds, in declaration order.
///
/// Two statements naming one tool are two answers to one question — which
/// invocations of it this participant reaches — so the second is refused here,
/// ahead of the panic the resolved map raises for a hand-built config.
fn emit_tool_grants(
    blocks: &[SectionNode],
    context: &str,
    scope: &Scope<'_>,
    errors: &mut Vec<Diagnostic>,
    withhold: &mut Refused,
) -> Vec<RToolGrant> {
    let duplicates = duplicate_sections(
        blocks.iter().enumerate(),
        context,
        crate::model::INSTANCE_BLOCK_KINDWORDS,
        errors,
    );
    let mut resolved = Vec::new();
    for (offset, node) in blocks.iter().enumerate() {
        if duplicates.contains(&offset) {
            withhold.drop_part();
            continue;
        }
        let block = match crate::model::instance_block(node) {
            Ok(crate::model::InstanceBlock::Tool(block)) => *block,
            Err(error) => {
                errors.push(error);
                withhold.drop_part();
                continue;
            }
        };
        match emit_tool_grant(block, scope, errors) {
            Some(grant) => resolved.push(grant),
            None => withhold.drop_part(),
        }
    }
    resolved
}

/// One `tool` statement: the tool it names, the `allow` clauses that narrow it,
/// and the throttle it may carry.
///
/// `None` where any part of it did not come out whole. A grant that admits less
/// than it says is a narrower authority than the operator wrote, and a grant
/// that admits more is one they did not write at all.
fn emit_tool_grant(
    block: TypedBlock<OpenAttrs>,
    scope: &Scope<'_>,
    errors: &mut Vec<Diagnostic>,
) -> Option<RToolGrant> {
    let TypedBlock {
        doc: _,
        kindword: _,
        name,
        attrs,
        subs,
    } = block;
    // The dispatch refuses a `tool` block with no name before this, so the
    // absence here is that refusal already reported.
    let tool = name?;
    let mut whole = true;
    // Everything a tool grant says, it says in a sub-block: the body itself has
    // no key set, so a key here is a shape mistake rather than an unknown word.
    for (key, value) in attrs.entries() {
        errors.push(Diagnostic::at(
            format!(
                "`{key}` is not a key of a `tool` statement: which invocations a grant \
                 admits is written as `allow` blocks, and what throttles them as a \
                 `rate_limit` block"
            ),
            value.span().clone(),
        ));
        whole = false;
    }
    let mut clauses = Vec::new();
    let mut rate_limit = None;
    let mut throttled: Option<Span> = None;
    for node in &subs {
        let sub = match crate::model::tool_block(node) {
            Ok(sub) => sub,
            Err(error) => {
                errors.push(error);
                whole = false;
                continue;
            }
        };
        match sub {
            ToolBlock::Allow(block) => {
                let block = *block;
                refuse_subs(block.kindword.value(), &block.subs, errors);
                let (attrs, refused) = resolve_attrs(block.attrs, scope, errors);
                let entries = attrs.entries();
                if entries.is_empty() {
                    errors.push(Diagnostic::at(
                        "an `allow` block with no requirements admits every invocation of \
                         the tool, which is what a `tool` statement with no `allow` block \
                         already says; write one or the other",
                        block.kindword.span().clone(),
                    ));
                    whole = false;
                    continue;
                }
                let mut clause = Vec::new();
                for (key, value) in entries {
                    match str_value(&value, "an `allow` requirement") {
                        Ok(text) => clause.push((key, text.to_string())),
                        Err(error) => {
                            errors.push(error);
                            whole = false;
                        }
                    }
                }
                match refused.any() || !whole {
                    true => whole = false,
                    false => clauses.push(clause),
                }
            }
            ToolBlock::RateLimit(block) => {
                let block = *block;
                refuse_subs(block.kindword.value(), &block.subs, errors);
                if let Some(prior) = &throttled {
                    errors.push(two_site(
                        "a `tool` grant carries one `rate_limit`: two buckets over one \
                         tool have no order to apply in"
                            .to_string(),
                        block.kindword.span().clone(),
                        "it is throttled here".to_string(),
                        prior.clone(),
                    ));
                    whole = false;
                    continue;
                }
                throttled = Some(block.kindword.span().clone());
                let (attrs, refused) = resolve_attrs(block.attrs, scope, errors);
                let RateLimitAttrs {
                    burst,
                    sustained_per_minute,
                } = attrs;
                let burst = rate_count(&burst.value, "burst", errors);
                let sustained =
                    rate_count(&sustained_per_minute.value, "sustained_per_minute", errors);
                match (refused.any(), burst, sustained) {
                    (false, Some(burst), Some(sustained_per_minute)) => {
                        rate_limit = Some(RRateLimit {
                            burst,
                            sustained_per_minute,
                        });
                    }
                    _ => whole = false,
                }
            }
        }
    }
    match whole {
        true => Some(RToolGrant {
            tool,
            clauses,
            rate_limit,
        }),
        false => None,
    }
}

/// One token-bucket parameter: a whole count of at least one.
///
/// Zero is refused here rather than at boot because it is spellable and
/// meaningless: a bucket that holds nothing, or refills by nothing, throttles
/// the grant to nothing and is a grant the operator did not mean to write.
fn rate_count(value: &RVal, key: &str, errors: &mut Vec<Diagnostic>) -> Option<u32> {
    let refuse = |message: String, errors: &mut Vec<Diagnostic>| {
        errors.push(Diagnostic::at(message, value.span().clone()));
        None
    };
    match value.value() {
        RValue::Int(count) => match u32::try_from(*count) {
            Ok(count) if count >= 1 => Some(count),
            _ => refuse(
                format!(
                    "`{key}` is a count of at least one; this one throttles the grant to nothing"
                ),
                errors,
            ),
        },
        other => refuse(
            format!("`{key}` is a count; this is {}", other.kind()),
            errors,
        ),
    }
}

/// Where a `tool` statement has no host to run on.
///
/// The double diagnosis a surface-placed instance gets: the statement is
/// refused here, and the `tools` grant word is refused beside it by the host
/// legality table.
fn refuse_surface_blocks(blocks: &[SectionNode], errors: &mut Vec<Diagnostic>) {
    for node in blocks {
        let (kindword, span) = crate::model::section_kindword(node);
        match crate::model::instance_block(node) {
            // One arm per kindword, so a second word states its own reason
            // instead of inheriting this one.
            Ok(crate::model::InstanceBlock::Tool(_)) => errors.push(Diagnostic::at(
                format!(
                    "`{kindword}` is backend-only in v1: the surface host links no tools \
                     interface, so a component placed on a surface reaches no registry tool"
                ),
                span,
            )),
            Err(error) => errors.push(error),
        }
    }
}

/// A statement's trailing block, deserialized into the statement form's own
/// vocabulary.
///
/// A statement with no tail carries the vocabulary a body nobody wrote
/// carries — every key absent — which is why `empty` is a parameter rather
/// than a bound: each tail names its own.
fn typed_tail<A, F>(
    tail: Option<AttrBlock<A>>,
    empty: F,
    scope: &Scope<'_>,
    errors: &mut Vec<Diagnostic>,
) -> (A::Output, Refused)
where
    A: MapValues<Spanned<Value>, RVal>,
    F: FnOnce() -> A::Output,
{
    match tail {
        Some(block) => resolve_attrs(block.attrs, scope, errors),
        None => (empty(), Refused::default()),
    }
}

// ── pass 4d: assembly expansion ──────────────────────────────────────────────
//
// An assembly is a template for a set of entities, and instantiating one stamps
// every item of its body under the instantiation's handle: `new alice_desk:
// Deskbar(…)` turns the body's `channel messages_p1` into the channel
// `alice_desk.messages_p1`. The walk runs before any body is emitted, because
// the channels an assembly stamps have to be reachable by every reference in
// the document — including a reference written outside it, which names one
// through the instance handle. What the walk leaves behind is a flat list of
// items, each with the frame it resolves under.

type AssemblyTable = TemplateTable<AssemblyDef>;

/// Collect the assemblies before emission consumes the items around them.
fn assembly_defs(modules: &[Vec<Spanned<Item>>]) -> AssemblyTable {
    templates(modules, |item| match item {
        Item::Assembly(def) => Some(&**def),
        _ => None,
    })
}

/// What one assembly body's items resolve under.
///
/// Shared by every item of the body, so it is built once per instantiation and
/// handed out by reference.
struct Frame {
    /// The file the assembly was declared in: a class means what it meant where
    /// it was written.
    file: usize,
    /// The file the top-level instantiation was written in. Stamped handles
    /// belong to that file, because that is where a reference from outside
    /// reaches them through the instance's name.
    root: usize,
    /// The handle the body's entities hang beneath. Always set for a body; the
    /// top-level frame has none, which is what makes it the top level.
    prefix: Option<HandlePath>,
    params: ParamBindings,
}

impl Frame {
    /// The scope this frame's items resolve in.
    fn scope<'a>(
        &'a self,
        index: &'a Index,
        channels: &'a ChannelTable,
        links: &'a LinkTable,
        stamps: &'a StampTable,
    ) -> Scope<'a> {
        Scope {
            outer: FileScope::in_file(index, self.file, channels, links, stamps),
            params: Some(&self.params),
            prefix: self.prefix.as_ref(),
            root: self.root,
        }
    }

    /// The handle a name written in this body is stamped under.
    fn stamp(&self, name: Spanned<String>) -> HandlePath {
        HandlePath::stamp(self.prefix.as_ref(), name)
    }
}

/// One item an instantiation stamped, ready to emit.
struct Stamped {
    item: AssemblyItem,
    /// A channel item's address and minted id, resolved by the walk.
    declaration: Option<ChannelDecl>,
    /// A link item's minted id.
    link: Option<LinkId>,
    frame: Rc<Frame>,
    /// Where this item's top-level instantiation was written, and how far into
    /// that expansion it came. Expansion completes in dependency order; the
    /// config carries source order, and this is what sorts it back.
    order: ((usize, usize), usize),
}

/// Expand every assembly instantiation the document reaches.
///
/// A fixpoint worklist rather than one pass in source order: an argument may
/// name a channel or an agent a sibling instantiation stamps, and which
/// instantiation is written first is not the operator's problem. An attempt
/// waiting on a still-pending sibling goes back on the list; a sweep that makes
/// no progress leaves only instantiations waiting on each other, which is one
/// error naming them all.
///
/// `minted` is how many channel ids the declared channels took: a stamped
/// channel's id continues from there, and the emission order the returned list
/// carries is what keeps an id the position it indexes.
fn expand_assemblies(
    index: &Index,
    modules: &[Vec<Spanned<Item>>],
    handles: &mut Handles<'_>,
    stamps: &mut StampTable,
    minted: (usize, usize),
    errors: &mut Vec<Diagnostic>,
) -> (Vec<Stamped>, HashSet<(usize, usize)>) {
    let (minted, minted_links) = minted;
    let mut walk = Walk {
        index,
        assemblies: assembly_defs(modules),
        next: minted,
        next_link: minted_links,
        out: Vec::new(),
        root: (0, 0),
        seq: 0,
    };
    let frames: Vec<Rc<Frame>> = (0..modules.len())
        .map(|position| {
            Rc::new(Frame {
                file: position,
                root: position,
                prefix: None,
                params: ParamBindings::new(),
            })
        })
        .collect();
    let mut queue: Vec<Pending<'_>> = Vec::new();
    for (position, items) in modules.iter().enumerate() {
        for (offset, item) in items.iter().enumerate() {
            let Item::Inst(inst) = item.value() else {
                continue;
            };
            queue.push(Pending {
                site: (position, offset),
                inst,
                frame: Rc::clone(&frames[position]),
                waiting: None,
            });
        }
    }
    let mut pending: HashSet<(usize, usize)> = queue.iter().map(|item| item.site).collect();
    // An instantiation that was attempted and refused stamps nothing. Anything
    // reaching under its handle would report a second error blaming a
    // reference that is only broken because its producer is, so those
    // instantiations are dropped instead, and are themselves producers of
    // nothing.
    let mut failed: HashSet<(usize, usize)> = HashSet::new();
    // Each attempt writes its own diagnostics, so that a deferred one leaves
    // none behind and the report reads in source order however the sweeps ran.
    let mut reported: Vec<((usize, usize), Vec<Diagnostic>)> = Vec::new();
    while !queue.is_empty() {
        let mut progress = false;
        let mut deferred = Vec::new();
        for mut item in queue {
            item.waiting = Waits {
                index,
                channels: handles.channels,
                links: handles.links,
                stamps,
                assemblies: &walk.assemblies,
                pending: &pending,
                failed: &failed,
            }
            .of(item.inst, &item.frame);
            if let Some(wait) = &item.waiting {
                if failed.contains(&wait.site) {
                    pending.remove(&item.site);
                    failed.insert(item.site);
                    progress = true;
                    continue;
                }
                deferred.push(item);
                continue;
            }
            let mut raised = Vec::new();
            walk.root = item.site;
            // A top-level `new` of any other class kind is emitted by its own
            // pass; only the expansion mattered here.
            let _expanded = walk.instantiate(
                item.inst,
                &item.frame,
                handles,
                stamps,
                &mut Vec::new(),
                &mut raised,
            );
            pending.remove(&item.site);
            if !raised.is_empty() {
                failed.insert(item.site);
            }
            reported.push((item.site, raised));
            progress = true;
        }
        queue = deferred;
        if !progress {
            break;
        }
    }
    reported.sort_by_key(|(site, _)| *site);
    for (_, raised) in reported {
        errors.extend(raised);
    }
    // Nothing in a mutual-wait knot expanded, so each of them stamped nothing
    // either; they are producers of nothing for the same reason a refused one is.
    failed.extend(queue.iter().map(|item| item.site));
    if let Some(error) = mutual_wait(&queue) {
        errors.push(error);
    }
    let mut out = walk.out;
    out.sort_by_key(|item| item.order);
    // The ids were minted as the instantiations completed; the config carries
    // source order, so they are renumbered to it and the table follows.
    let mut remap: HashMap<ChanId, ChanId> = HashMap::new();
    let mut next = minted;
    for item in &mut out {
        if let Some((_, Some(id))) = &mut item.declaration {
            let fresh = ChanId(next);
            next += 1;
            remap.insert(*id, fresh);
            *id = fresh;
        }
    }
    handles.channels.renumber(&remap);
    let mut link_remap: HashMap<LinkId, LinkId> = HashMap::new();
    let mut next_link = minted_links;
    for item in &mut out {
        if let Some(id) = &mut item.link {
            let fresh = LinkId(next_link);
            next_link += 1;
            link_remap.insert(*id, fresh);
            *id = fresh;
        }
    }
    handles.links.renumber(&link_remap);
    (out, failed)
}

/// The two handle spaces a declaration lands in, carried together.
///
/// The expansion walk writes to both, and every scope it builds reads both, so
/// they travel as one — separately they are two parameters that must never be
/// passed in the wrong order and never one without the other.
struct Handles<'a> {
    channels: &'a mut ChannelTable,
    links: &'a mut LinkTable,
}

/// A top-level instantiation the walk has not expanded yet.
struct Pending<'a> {
    /// The file and item the `new` was written at, which is both its identity
    /// in the pending set and the source order the config is sorted back into.
    site: (usize, usize),
    inst: &'a NewStmt,
    frame: Rc<Frame>,
    /// The reference the last attempt deferred on, kept so the report does not
    /// resolve every pending argument a second time.
    waiting: Option<Wait>,
}

/// What one instantiation is waiting for: the sibling that produces it, the
/// reference it was written as, and where.
struct Wait {
    site: (usize, usize),
    reference: String,
    span: Span,
}

/// What the deferral test reads: everything a reference could resolve through,
/// plus the two sets that say which siblings are worth waiting for.
struct Waits<'a> {
    index: &'a Index,
    channels: &'a ChannelTable,
    links: &'a LinkTable,
    stamps: &'a StampTable,
    assemblies: &'a AssemblyTable,
    /// The instantiations that have not expanded yet.
    pending: &'a HashSet<(usize, usize)>,
    /// The instantiations that were attempted and refused, whose dependents
    /// must not report a second, derived error.
    failed: &'a HashSet<(usize, usize)>,
}

impl Waits<'_> {
    /// The reference an instantiation is waiting on, where it is waiting on
    /// one.
    ///
    /// Only a reference reaching *under* another top-level instantiation's
    /// handle defers: a bare name is a declaration, which expansion never
    /// mints, and anything else that fails to resolve is an error the attempt
    /// should raise.
    ///
    /// The arguments of the instantiation itself are not the whole story — an
    /// assembly body's own `new` takes arguments too, and those resolve in the
    /// file that declared the assembly, so a body reaching a sibling's stamped
    /// entity waits exactly the way a top-level argument does. The body is
    /// walked transitively for them; a parameter reference is not a wait, and
    /// the walk makes the parameter names opaque so it stays that way whatever
    /// the declaring file holds under the same spelling.
    fn of(&self, inst: &NewStmt, frame: &Frame) -> Option<Wait> {
        self.walk(inst, frame, &[], &mut Vec::new())
    }

    /// One instantiation's waits, and those of every `new` its body reaches.
    ///
    /// `params` are the parameter names of the body this `new` was written in —
    /// empty at top level, where nothing is in scope but the file.
    fn walk(
        &self,
        inst: &NewStmt,
        frame: &Frame,
        params: &[&str],
        seen: &mut Vec<(usize, usize)>,
    ) -> Option<Wait> {
        let scope = frame.scope(self.index, self.channels, self.links, self.stamps);
        if let Some(args) = inst.args.as_ref() {
            for arg in &args.args {
                if let Some(wait) = self.wait_for(&scope, &arg.value, params) {
                    return Some(wait);
                }
            }
        }
        // The class the `new` names, so its body's own instantiations can be
        // asked the same question. A class that does not resolve is the
        // attempt's to refuse, not this walk's.
        let span = inst.cls.head.span().clone();
        let (_, symbol) = scope.class(&inst.cls, &span).ok()?;
        let site = (symbol.file, symbol.item);
        if symbol.kind != SymKind::Assembly || seen.contains(&site) {
            return None;
        }
        let def = self.assemblies.get(&site)?;
        // The body resolves in the file that declared the assembly, with no
        // parameters bound — this walk has no arguments to bind them to. Real
        // resolution binds them, and a parameter shadows the file scope, so the
        // names are carried and made opaque below; reading one through the file
        // scope would manufacture a dependency that does not exist.
        let inner_params: Vec<&str> = def
            .params
            .params
            .iter()
            .map(|param| param.name.value().as_str())
            .collect();
        let inner = Frame {
            file: symbol.file,
            root: frame.root,
            prefix: None,
            params: ParamBindings::new(),
        };
        seen.push(site);
        let mut found = None;
        for item in &def.items {
            let AssemblyItem::Inst(nested) = item.value() else {
                continue;
            };
            found = self.walk(nested, &inner, &inner_params, seen);
            if found.is_some() {
                break;
            }
        }
        seen.pop();
        found
    }

    /// The wait one argument expresses, where it expresses one.
    ///
    /// A reference headed by one of the enclosing body's parameters is never a
    /// wait: at real resolution it binds to the parameter, whatever the
    /// declaring file holds under that name.
    fn wait_for(&self, scope: &Scope<'_>, value: &Spanned<Value>, params: &[&str]) -> Option<Wait> {
        let Value::Ref(path) = value.value() else {
            return None;
        };
        if params.contains(&path.head.value().as_str()) {
            return None;
        }
        let span = value.span();
        let (symbol, name, rest) = scope.symbol(path, span).ok()?;
        if rest.is_empty() || symbol.kind != SymKind::Instance {
            return None;
        }
        let site = (symbol.file, symbol.item);
        if !self.pending.contains(&site) && !self.failed.contains(&site) {
            return None;
        }
        Some(Wait {
            site,
            reference: dotted_handle(&name, path.head.span(), &rest).dotted(),
            span: span.clone(),
        })
    }
}

/// What is left on the worklist when no sweep can make progress.
///
/// One diagnostic for the whole knot, with a line per member saying which
/// reference it is stuck on — the instance-level counterpart of the class-level
/// instantiation cycle.
fn mutual_wait(queue: &[Pending<'_>]) -> Option<Diagnostic> {
    let first = queue.first()?;
    let names: Vec<String> = queue
        .iter()
        .map(|item| format!("`{}`", item.inst.handle.value()))
        .collect();
    let mut error = Diagnostic::at(
        format!(
            "these instantiations wait on each other, so none of them can expand: {}",
            names.join(", ")
        ),
        first.inst.handle.span().clone(),
    );
    // Each member's last sweep recorded what it was waiting on; the answer
    // cannot have changed since, because no sweep after it made progress.
    for item in queue {
        if let Some(wait) = &item.waiting {
            error.related.push((
                format!(
                    "`{}` waits on `{}`",
                    item.inst.handle.value(),
                    wait.reference
                ),
                wait.span.clone(),
            ));
        }
    }
    Some(error)
}

/// The state one expansion walk threads.
struct Walk<'a> {
    index: &'a Index,
    assemblies: AssemblyTable,
    /// The next channel id to mint.
    next: usize,
    /// The next link id to mint.
    next_link: usize,
    out: Vec<Stamped>,
    /// The top-level instantiation being expanded, which every item it stamps
    /// is ordered under.
    root: (usize, usize),
    /// How far into the whole walk an item was stamped, which orders the items
    /// of one instantiation among themselves.
    seq: usize,
}

impl Walk<'_> {
    /// One `new`, expanded where it names an assembly.
    ///
    /// Answers with the kind of class the `new` named, which is the one
    /// resolution of it anyone makes: an instantiation of any other class kind
    /// is the emission pass's, and so is a class name that resolves to nothing
    /// (`None`) — this pass reports neither, because reporting it here would
    /// say it twice.
    fn instantiate(
        &mut self,
        inst: &NewStmt,
        parent: &Rc<Frame>,
        handles: &mut Handles<'_>,
        stamps: &mut StampTable,
        chain: &mut Vec<((usize, usize), String)>,
        errors: &mut Vec<Diagnostic>,
    ) -> Option<SymKind> {
        let span = inst.cls.head.span().clone();
        let symbol = {
            let scope = parent.scope(self.index, handles.channels, handles.links, stamps);
            match scope.class(&inst.cls, &span) {
                Ok((_, symbol)) => symbol,
                Err(_) => return None,
            }
        };
        if symbol.kind != SymKind::Assembly {
            return Some(symbol.kind);
        }
        let site = (symbol.file, symbol.item);
        let def = self.assemblies.get(&site).map(Rc::clone)?;
        if let Some(body) = &inst.body {
            errors.push(two_site(
                "an assembly instantiation takes arguments, not a body; per-instance \
                 values are assembly parameters",
                body.span().clone(),
                "the assembly is declared here",
                def.name.span().clone(),
            ));
            return Some(SymKind::Assembly);
        }
        if chain.iter().any(|(seen, _)| *seen == site) {
            let mut through: Vec<&str> = chain.iter().map(|(_, name)| name.as_str()).collect();
            through.push(def.name.value());
            errors.push(Diagnostic::at(
                format!(
                    "instantiating `{}` reaches itself: {}",
                    def.name.value(),
                    through.join(" -> ")
                ),
                span,
            ));
            return Some(SymKind::Assembly);
        }
        let params = {
            let scope = parent.scope(self.index, handles.channels, handles.links, stamps);
            bind_args(
                Some(&def.params),
                inst.args.as_ref(),
                def.name.value(),
                inst.handle.span(),
                &scope,
                errors,
            )
        };
        let Some(params) = params else {
            return Some(SymKind::Assembly);
        };
        let frame = Rc::new(Frame {
            file: symbol.file,
            root: parent.root,
            prefix: Some(parent.stamp(inst.handle.clone())),
            params,
        });
        // Channels first, and all of them, before anything nested: an id is
        // minted here and read everywhere, so the order it is minted in is the
        // order the config will carry.
        for item in &def.items {
            let AssemblyItem::Channel(channel) = item.value() else {
                continue;
            };
            self.stamp_channel(channel, &frame, handles, stamps, errors);
        }
        for item in &def.items {
            let AssemblyItem::Link(stmt) = item.value() else {
                continue;
            };
            let id = LinkId(self.next_link);
            self.next_link += 1;
            handles.links.declare(
                "link",
                frame.root,
                &frame.stamp(stmt.handle.clone()).dotted(),
                id,
            );
            self.out.push(Stamped {
                item: item.value().clone(),
                declaration: None,
                link: Some(id),
                frame: frame.clone(),
                order: (self.root, self.seq),
            });
            self.seq += 1;
        }
        chain.push((site, def.name.value().clone()));
        for item in &def.items {
            match item.value() {
                AssemblyItem::Channel(_) | AssemblyItem::Link(_) => {}
                AssemblyItem::Surface(surface) => {
                    stamps.record(
                        frame.root,
                        frame.stamp(surface.name.clone()).dotted(),
                        StampKind::Surface,
                    );
                    self.push(item.value().clone(), None, &frame);
                }
                AssemblyItem::Inst(nested) => {
                    let kind = self.instantiate(nested, &frame, handles, stamps, chain, errors);
                    if let Some(stamped) = kind.and_then(StampKind::of_class) {
                        stamps.record(
                            frame.root,
                            frame.stamp(nested.handle.clone()).dotted(),
                            stamped,
                        );
                    }
                    // Anything the walk did not expand is the emission pass's.
                    if kind != Some(SymKind::Assembly) {
                        self.push(item.value().clone(), None, &frame);
                    }
                }
                AssemblyItem::Grant(_) => self.push(item.value().clone(), None, &frame),
            }
        }
        chain.pop();
        Some(SymKind::Assembly)
    }

    /// One stamped item, in the order the walk reached it.
    fn push(&mut self, item: AssemblyItem, declaration: Option<ChannelDecl>, frame: &Rc<Frame>) {
        self.out.push(Stamped {
            item,
            declaration,
            link: None,
            frame: frame.clone(),
            order: (self.root, self.seq),
        });
        self.seq += 1;
    }

    fn stamp_channel(
        &mut self,
        def: &ChannelDef,
        frame: &Rc<Frame>,
        handles: &mut Handles<'_>,
        stamps: &StampTable,
        errors: &mut Vec<Diagnostic>,
    ) {
        let (addr, handle) = match def {
            ChannelDef::Decl(decl) => (&decl.addr, Some(&decl.handle)),
            ChannelDef::Tuning(tuning) => (&tuning.addr, None),
        };
        // An address names no channel, so it resolves against the parameters
        // and nothing else — the same reason the declared addresses do.
        let empty = ChannelTable::default();
        let address = {
            let scope = frame.scope(self.index, &empty, handles.links, stamps);
            match resolve_address(addr, &scope) {
                Ok(address) => address,
                Err(error) => {
                    // A refused address mints no id, but the item is still
                    // stamped so the emission pass reaches the diagnostics that
                    // read the statement itself rather than its address.
                    errors.push(error);
                    self.push(AssemblyItem::Channel(Box::new(def.clone())), None, frame);
                    return;
                }
            }
        };
        let id = handle.map(|handle| {
            let id = ChanId(self.next);
            self.next += 1;
            handles.channels.declare(
                "channel",
                frame.root,
                &frame.stamp(handle.clone()).dotted(),
                id,
            );
            id
        });
        self.push(
            AssemblyItem::Channel(Box::new(def.clone())),
            Some((address, id)),
            frame,
        );
    }
}

/// What emitting a stamped item reads: the tables the whole document shares.
struct Tables<'a> {
    index: &'a Index,
    channels: &'a ChannelTable,
    links: &'a LinkTable,
    stamps: &'a StampTable,
    classes: &'a ClassTable,
    agents: &'a AgentTable,
}

/// One stamped item, emitted under the frame its instantiation gave it.
fn emit_stamped(
    stamped: Stamped,
    tables: &Tables<'_>,
    config: &mut Emitted,
    errors: &mut Vec<Diagnostic>,
) {
    let scope = stamped
        .frame
        .scope(tables.index, tables.channels, tables.links, tables.stamps);
    let (classes, agents) = (tables.classes, tables.agents);
    match stamped.item {
        AssemblyItem::Channel(def) => {
            emit_channel(*def, stamped.declaration, &scope, config, errors)
        }
        AssemblyItem::Link(stmt) => emit_link(*stmt, &scope, config),
        AssemblyItem::Surface(def) => emit_surface(*def, &scope, classes, config, errors),
        AssemblyItem::Inst(inst) => emit_inst(*inst, &scope, classes, agents, config, errors),
        AssemblyItem::Grant(stmt) => match emit_grant(*stmt, &scope) {
            Ok(grant) => config.grants.push(grant),
            Err(error) => errors.push(error),
        },
    }
}

// ── pass 5: identity ─────────────────────────────────────────────────────────
//
// A wire identity is what the runtime will call an entity, so the charset it
// has to satisfy is the runtime's. Two families, because the runtime has two:
// the unreserved set is the shared predicate, and the kebab one is this
// language's own — nothing in the runtime states it.

/// An entity family, and with it the two things every identity check needs to
/// know about one: how its identities are spelled, and what to call it in a
/// message.
///
/// One table. A family's charset and its label are asked for at the emit site
/// of a withheld entity and again at the collision pass, and a second copy of
/// the pairing would drift the first time a family's spelling changes — in the
/// refused-body branch, which an operator only reaches once something is
/// already wrong.
#[derive(Clone, Copy)]
enum Family {
    Surface,
    Consumer,
    Webhook,
    Remote,
    Agent,
    Repo,
    MqttClient,
}

impl Family {
    /// The spelling rule this family's identities follow.
    fn charset(self) -> Charset {
        match self {
            Family::Agent | Family::Repo => Charset::Kebab,
            Family::Surface
            | Family::Consumer
            | Family::Webhook
            | Family::Remote
            | Family::MqttClient => Charset::Unreserved,
        }
    }

    /// Whether this family's identities can be spelled by a `slug` attr.
    ///
    /// Where they cannot, the handle *is* the identity and the only way to fix
    /// an illegal one is to rename the declaration — telling the operator to
    /// write a `slug` would send them to a key the vocabulary refuses.
    fn spells_slug(self) -> bool {
        match self {
            Family::Surface | Family::Consumer | Family::Webhook | Family::Agent => true,
            Family::Remote | Family::Repo | Family::MqttClient => false,
        }
    }

    /// What a message calls one of these.
    fn label(self) -> &'static str {
        match self {
            Family::Surface => "surface",
            Family::Consumer => "consumer",
            Family::Webhook => "webhook",
            Family::Remote => "remote",
            Family::Agent => "agent",
            Family::Repo => "repo",
            Family::MqttClient => "mqtt client",
        }
    }
}

/// Which spelling rule a family's identities follow.
#[derive(Clone, Copy)]
enum Charset {
    /// Agents and repos: lowercase, digits and `-`, leading alphanumeric.
    Kebab,
    /// Everything addressable: the RFC 3986 unreserved set.
    Unreserved,
}

impl Charset {
    /// What a message says the legal spelling is.
    fn describe(self) -> &'static str {
        match self {
            Charset::Kebab => "lowercase, digits, `-`",
            Charset::Unreserved => "letters, digits, `.`, `_`, `~`, `-`",
        }
    }

    fn admits(self, text: &str) -> bool {
        match self {
            Charset::Kebab => is_kebab(text),
            Charset::Unreserved => is_unreserved_name(text),
        }
    }

    /// The slug a reader could write instead of the one that was refused.
    fn suggest(self, text: &str) -> String {
        let mapped: String = text
            .chars()
            .map(|c| match self {
                Charset::Kebab if c.is_ascii_uppercase() => c.to_ascii_lowercase(),
                _ if self.admits(&c.to_string()) => c,
                _ => '-',
            })
            .collect();
        let trimmed = mapped.trim_matches('-').to_string();
        if trimmed.is_empty() {
            "a-name".to_string()
        } else {
            trimmed
        }
    }
}

fn is_kebab(text: &str) -> bool {
    let mut chars = text.chars();
    chars
        .next()
        .is_some_and(|first| first.is_ascii_lowercase() || first.is_ascii_digit())
        && chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
}

/// Every entity's wire identity: legal for its family, and unique within it.
fn check_identity(config: &ResolvedConfig, errors: &mut Vec<Diagnostic>) {
    check_family(
        config.surfaces.iter().map(|entity| &entity.slug),
        Family::Surface,
        errors,
    );
    check_family(
        config.consumers.iter().map(|entity| &entity.slug),
        Family::Consumer,
        errors,
    );
    check_family(
        config.webhooks.iter().map(|entity| &entity.slug),
        Family::Webhook,
        errors,
    );
    check_family(
        config.remotes.iter().map(|entity| &entity.slug),
        Family::Remote,
        errors,
    );
    check_family(
        config.agents.iter().map(|entity| &entity.slug),
        Family::Agent,
        errors,
    );
    fn handles<A>(entities: &[RNamed<A>]) -> Vec<Spanned<String>> {
        entities
            .iter()
            .map(|entity| named_slug(&entity.handle))
            .collect()
    }
    let repos = handles(&config.repos);
    check_family(repos.iter(), Family::Repo, errors);
    for repo in &repos {
        // The runtime spends `all` on "every repo", so no repo may be it.
        if repo.value() == "all" {
            errors.push(Diagnostic::at(
                "`all` is how the runtime says every repo, so it is not a repo name",
                repo.span().clone(),
            ));
        }
    }
    let clients = handles(&config.mqtt_clients);
    check_family(clients.iter(), Family::MqttClient, errors);
}

/// Every `grant`'s principal names an entity authority can be held by.
///
/// Post-expansion, because the entity space is not complete until every
/// assembly has been stamped: a grant may name an entity a later instantiation
/// writes.
///
/// A withheld entity of a grantable kind counts as a principal: it was
/// declared, and the compile already fails on whatever withheld it. Reporting
/// its grants as naming nothing would fan one bad attr value out into one
/// false diagnostic per grant that mentions the entity. A withheld repo or
/// webhook does not count: a grant may never name one, and that mistake is
/// independent of whatever broke the body.
fn check_grants(config: &ResolvedConfig, withheld: &Withheld, errors: &mut Vec<Diagnostic>) {
    let mut principals: HashSet<String> = HashSet::new();
    principals.extend(config.surfaces.iter().map(|e| e.handle.dotted()));
    principals.extend(config.agents.iter().map(|e| e.handle.dotted()));
    principals.extend(config.remotes.iter().map(|e| e.handle.dotted()));
    principals.extend(config.consumers.iter().map(|e| e.handle.dotted()));
    for grant in &config.grants {
        let handle = grant.principal.dotted();
        if !principals.contains(&handle) && !withheld.grantable(&handle) {
            errors.push(Diagnostic::at(
                format!(
                    "`{handle}` is not a principal; a grant names a surface, an agent, \
                     a remote or a consumer"
                ),
                grant.principal_span.span().clone(),
            ));
        }
    }
}

/// The two rules that read every address in the expanded document at once.
///
/// Both are post-expansion because an assembly stamps channels: two
/// instantiations that write one address collide only once both have been
/// stamped, and the site to cite is the shared declaration inside the body.
fn check_addresses(config: &ResolvedConfig, errors: &mut Vec<Diagnostic>) {
    // The first declaration of each address holds it; a later one is the
    // collision. Tunings are not here: a tuning is a matcher over a family, not
    // an identity, and derivation refuses one on a declarable address outright,
    // so no tuning key can be a channel's address.
    let declared = check_unique(
        config.channels.iter().map(|channel| {
            (
                channel.address.value().as_str(),
                &channel.handle,
                channel.address.span(),
            )
        }),
        |address, _, span, prior_handle, prior_span| {
            two_site(
                format!("two channels declare the address `{address}`"),
                span.clone(),
                format!("`{}` declares it here", prior_handle.dotted()),
                prior_span.clone(),
            )
        },
        errors,
    );

    // One spelling: where a channel exists, it is named. A second spelling of
    // its address is a second name for one thing, and every later pass that
    // keys on identity would see two.
    for (text, span) in literal_addresses(config) {
        if let Some((handle, _)) = declared.get(text) {
            errors.push(Diagnostic::at(
                format!(
                    "`{text}` is the address channel `{}` declares; name the channel, \
                     not its address",
                    handle.dotted()
                ),
                span.clone(),
            ));
        }
    }
}

/// Every address written as a string literal where a declared channel could
/// have been named instead: bindings, subscriptions and `exact` matchers.
///
/// A `prefix` matcher is not one of them — it is written about a family, and
/// the family a declared channel belongs to is not that channel.
///
/// Nor are the value positions inside an entity. The value language admits a
/// matcher anywhere a value goes, so `description = exact "brenn:…";` is
/// writable — but an attribute value is not a channel reference, nothing
/// downstream reads one as an identity, and a matcher where a scalar belongs is
/// a type error lowering raises. The rule is about the positions that *do* name
/// a channel; widening it to every value would refuse a second spelling nobody
/// spells and nothing resolves.
fn literal_addresses(config: &ResolvedConfig) -> Vec<(&str, &Span)> {
    // Destructured, not field-accessed: this walk is a hand-written mirror of
    // the resolved model, and a new entity vector has to be answered for here
    // or the one-spelling rule quietly stops covering it. Adding a field to
    // `ResolvedConfig` breaks this pattern.
    let ResolvedConfig {
        channels: _,
        tunings: _,
        // A link has no address at all, so it spells none.
        links: _,
        uuid_pins: _,
        surfaces: _,
        consumers: _,
        agents: _,
        remotes: _,
        // A webhook block carries attrs, no chan_ref.
        webhooks: _,
        repos: _,
        mqtt_clients: _,
        mcp_servers: _,
        grants: _,
        sections: _,
    } = config;
    let mut found = Vec::new();
    for surface in &config.surfaces {
        acl_literals(&surface.acls, &mut found);
        for component in &surface.components {
            binding_literals(&component.bindings, &mut found);
        }
    }
    for consumer in &config.consumers {
        acl_literals(&consumer.acls, &mut found);
        binding_literals(&consumer.bindings, &mut found);
    }
    for agent in &config.agents {
        acl_literals(&agent.acls, &mut found);
        for sub in &agent.subs {
            chan_ref_literal(&sub.chan, &mut found);
        }
    }
    for remote in &config.remotes {
        acl_literals(&remote.acls, &mut found);
    }
    for grant in &config.grants {
        matcher_literal(&grant.m, &mut found);
    }
    found
}

fn acl_literals<'a>(acls: &'a [RAcl], found: &mut Vec<(&'a str, &'a Span)>) {
    for acl in acls {
        for matcher in &acl.matchers {
            matcher_literal(matcher, found);
        }
    }
}

fn binding_literals<'a>(bindings: &'a [RBinding], found: &mut Vec<(&'a str, &'a Span)>) {
    for binding in bindings {
        if let Some(chan) = &binding.chan {
            chan_ref_literal(chan, found);
        }
    }
}

fn chan_ref_literal<'a>(chan: &'a RChanRef, found: &mut Vec<(&'a str, &'a Span)>) {
    if let RChanRef::Addr(address) = chan {
        found.push((address.value().as_str(), address.span()));
    }
}

fn matcher_literal<'a>(matcher: &'a RMatcher, found: &mut Vec<(&'a str, &'a Span)>) {
    match matcher.kind.value() {
        MatcherKind::Exact => {}
        // A prefix is written about a family, and the family a declared channel
        // belongs to is not that channel. The three transport kinds name
        // system-minted addresses, which no declaration can hold.
        MatcherKind::Prefix
        | MatcherKind::TopicFilter
        | MatcherKind::Endpoint
        | MatcherKind::Client => return,
    }
    if let RMatcherVal::Lit(text) = matcher.val.value() {
        found.push((text.as_str(), matcher.val.span()));
    }
}

/// A handle used as an identity, with the span of the segment that named it.
fn named_slug(handle: &HandlePath) -> Spanned<String> {
    let span = handle
        .0
        .last()
        .map(|segment| segment.span().clone())
        .unwrap_or_else(Span::unknown);
    Spanned::new(handle.dotted(), span)
}

/// One identity against the charset its family spells identities in.
///
/// A withheld entity never reaches [`check_family`], which protects the
/// collision check from half-resolved identities. The charset question is
/// about this identity alone and is still worth answering, so the emit path
/// asks it directly.
fn check_charset(slug: &Spanned<String>, family: Family, errors: &mut Vec<Diagnostic>) {
    let charset = family.charset();
    let spells_slug = family.spells_slug();
    let label = family.label();
    if !charset.admits(slug.value()) {
        let suggestion = charset.suggest(slug.value());
        let advice = match spells_slug {
            true => format!("state one: `slug = \"{suggestion}\";`"),
            false => format!("rename the {label} `{suggestion}`"),
        };
        errors.push(Diagnostic::at(
            format!(
                "`{}` is not a legal {label} identity ({}); {advice}",
                slug.value(),
                charset.describe(),
            ),
            slug.span().clone(),
        ));
    }
}

/// One family's identities: each legal, and no two the same.
fn check_family<'a>(
    slugs: impl Iterator<Item = &'a Spanned<String>>,
    family: Family,
    errors: &mut Vec<Diagnostic>,
) {
    let label = family.label();
    let mut spanned: Vec<(&str, (), &Span)> = Vec::new();
    for slug in slugs {
        check_charset(slug, family, errors);
        spanned.push((slug.value().as_str(), (), slug.span()));
    }
    check_unique(
        spanned.into_iter(),
        |slug, (), span, (), prior_span| {
            two_site(
                format!("two {label}s resolve to the identity `{slug}`"),
                span.clone(),
                "the other one is here",
                prior_span.clone(),
            )
        },
        errors,
    );
}

#[cfg(test)]
mod tests {
    //! The file scope, reachable only from crate-internal callers.

    use super::*;
    use crate::parse_str;

    /// One source through load-free indexing: the pipeline every unit test
    /// below starts from, `files` included for the emit-level ones.
    fn indexed_files(source: &str) -> (Index, Vec<(String, File)>, Vec<Diagnostic>) {
        let file = parse_str(source, "t.brenn").expect("a parse");
        let files = vec![(ROOT_KEY.to_string(), file)];
        let mut errors = Vec::new();
        let mut index = Index::build(&files, &mut errors);
        index.resolve_constants(&files, &mut errors);
        (index, files, errors)
    }

    fn indexed(source: &str) -> (Index, Vec<Diagnostic>) {
        let (index, _, errors) = indexed_files(source);
        (index, errors)
    }

    /// One source through emission, for a test whose subject is what the
    /// emitter left behind rather than what it reported.
    fn emitted(source: &str) -> (Emitted, Vec<Diagnostic>) {
        let (index, files, mut errors) = indexed_files(source);
        assert!(errors.is_empty(), "{errors:?}");
        let emitted = emit_entities(&index, files, &mut errors);
        (emitted, errors)
    }

    /// A parameter name is opaque to the deferral walk.
    ///
    /// The walk reads an assembly body under the declaring file's scope with
    /// no arguments bound, so without this a body reference headed by a
    /// parameter would resolve to whatever that file holds under the same
    /// spelling — manufacturing a dependency real resolution does not have,
    /// and silently dropping the instantiation when that name has failed. The
    /// arrangement is unreachable through a document (`Index::check_params`
    /// refuses the collision one pass earlier), so the walk is pinned here.
    #[test]
    fn a_parameter_name_is_never_a_wait() {
        let source = concat!(
            "component Sink { abi = processor; requires = []; }\n",
            "new thing: Sink;\n",
            "new wired: Sink { config = thing.messages; }\n",
        );
        let file = parse_str(source, "t.brenn").expect("a parse");
        let value = file
            .instantiations()
            .find(|inst| inst.handle.value() == "wired")
            .and_then(|inst| inst.body.as_ref())
            .map(|body| body.value().attrs.get("config").expect("the attr").clone())
            .expect("the reference");
        let (index, _) = indexed(source);
        let channels = ChannelTable::default();
        let links = LinkTable::default();
        let stamps = StampTable::default();
        let assemblies = AssemblyTable::new();
        let frame = Frame {
            file: 0,
            root: 0,
            prefix: None,
            params: ParamBindings::new(),
        };
        let scope = frame.scope(&index, &channels, &links, &stamps);
        let Value::Ref(path) = value.value() else {
            panic!("a reference");
        };
        let (symbol, ..) = scope.symbol(path, value.span()).expect("the instance");
        let pending = HashSet::from([(symbol.file, symbol.item)]);
        let failed = HashSet::new();
        let waits = Waits {
            index: &index,
            channels: &channels,
            links: &links,
            stamps: &stamps,
            assemblies: &assemblies,
            pending: &pending,
            failed: &failed,
        };
        assert!(
            waits.wait_for(&scope, &value, &[]).is_some(),
            "the reference reaches under a pending instantiation"
        );
        assert!(
            waits.wait_for(&scope, &value, &["thing"]).is_none(),
            "a parameter of that name shadows it"
        );
    }

    /// The shadowing rule reaches the walk through `Waits::of`, not only
    /// `wait_for`.
    ///
    /// `of` walks an assembly body carrying the declaring assembly's parameter
    /// names; passing nothing there would read a body reference headed by a
    /// parameter through the file scope and silently drop the instantiation
    /// when that name has failed. Driven from `of` so the wiring is pinned and
    /// not only the predicate it calls.
    #[test]
    fn an_assembly_body_walk_carries_its_parameter_names() {
        let source = concat!(
            "component Sink { abi = processor; requires = []; }\n",
            "assembly Leaf(chan: Channel) {\n",
            "}\n",
            "assembly Shadowing(thing: String) {\n",
            "    new leaf: Leaf(chan = thing.messages);\n",
            "}\n",
            "assembly Waiting(look: String) {\n",
            "    new leaf: Leaf(chan = thing.messages);\n",
            "}\n",
            "new thing: Sink;\n",
            "new shadowing: Shadowing(thing = \"x\");\n",
            "new waiting: Waiting(look = \"x\");\n",
        );
        let file = parse_str(source, "t.brenn").expect("a parse");
        let modules = vec![file.items.clone()];
        let assemblies = assembly_defs(&modules);
        let (index, _) = indexed(source);
        let channels = ChannelTable::default();
        let links = LinkTable::default();
        let stamps = StampTable::default();
        let frame = Frame {
            file: 0,
            root: 0,
            prefix: None,
            params: ParamBindings::new(),
        };
        let scope = frame.scope(&index, &channels, &links, &stamps);
        // The site of `new thing`, read the way the reference in either body
        // would read it.
        let probe = parse_str("const probe = thing;\n", "p.brenn").expect("a parse");
        let probe = probe.consts().next().expect("one constant").value.clone();
        let Value::Ref(path) = probe.value() else {
            panic!("a reference");
        };
        let (symbol, ..) = scope.symbol(path, probe.span()).expect("the instance");
        let pending = HashSet::from([(symbol.file, symbol.item)]);
        let failed = HashSet::new();
        let waits = Waits {
            index: &index,
            channels: &channels,
            links: &links,
            stamps: &stamps,
            assemblies: &assemblies,
            pending: &pending,
            failed: &failed,
        };
        let instantiation = |handle: &str| {
            file.instantiations()
                .find(|inst| inst.handle.value() == handle)
                .expect("the instantiation")
                .clone()
        };
        assert!(
            waits.of(&instantiation("waiting"), &frame).is_some(),
            "the body reaches under a pending instantiation"
        );
        assert!(
            waits.of(&instantiation("shadowing"), &frame).is_none(),
            "a parameter of that name shadows it"
        );
    }

    /// The value one f-string resolves to under a file's own scope.
    fn interpolated(source: &str, probe: &str) -> Result<String, Diagnostic> {
        let (index, errors) = indexed(source);
        assert!(errors.is_empty(), "{:?}", errors[0].message);
        let file = parse_str(probe, "p.brenn").expect("a parse");
        let constant = file.consts().next().expect("one constant");
        let channels = ChannelTable::default();
        let links = LinkTable::default();
        let stamps = StampTable::default();
        let scope = FileScope::in_file(&index, 0, &channels, &links, &stamps);
        let resolved = resolve_value(&constant.value, &scope)?;
        match resolved.value() {
            RValue::Str(text) => Ok(text.clone()),
            other => panic!("expected a string, found {}", other.kind()),
        }
    }

    #[test]
    fn a_reference_resolves_to_its_constants_value() {
        let text = interpolated(
            "const host = \"example.com\";\n",
            "const probe = f\"https://{host}/hook\";\n",
        )
        .expect("the splice");
        assert_eq!(text, "https://example.com/hook");
    }

    #[test]
    fn a_dotted_reference_indexes_a_table_constant() {
        let text = interpolated(
            "const defaults = { soft_pct = 70, name = \"alice\" };\n",
            "const probe = f\"{defaults.name} at {defaults.soft_pct}%\";\n",
        )
        .expect("both splices");
        assert_eq!(text, "alice at 70%");
    }

    #[test]
    fn a_missing_table_key_names_the_keys_there_are() {
        let error = interpolated(
            "const defaults = { soft_pct = 70 };\n",
            "const probe = f\"{defaults.hard_pct}\";\n",
        )
        .expect_err("no such key");
        assert_eq!(
            error.message,
            "`defaults` has no key `hard_pct`; it has soft_pct"
        );
    }

    #[test]
    fn a_non_value_name_says_what_it_names() {
        let error = interpolated(
            "surface alice_desk {\n    grants = [subscribe];\n}\n",
            "const probe = f\"{alice_desk}\";\n",
        )
        .expect_err("a surface is not a value");
        assert_eq!(
            error.message,
            "`alice_desk` names a surface, which is not a value"
        );
    }

    #[test]
    fn a_kind_that_does_not_splice_is_named() {
        for (source, kind) in [
            ("const ratio = 1.5;\n", "a float"),
            ("const on = true;\n", "a boolean"),
            ("const items = [1, 2];\n", "a list"),
            ("const defaults = { soft_pct = 70 };\n", "a table"),
        ] {
            let name = source
                .split_whitespace()
                .nth(1)
                .expect("the constant's name");
            let error = interpolated(source, &format!("const probe = f\"{{{name}}}\";\n"))
                .expect_err("only a string or an integer splices");
            assert_eq!(
                error.message,
                format!("cannot interpolate {kind}; only a string or an integer splices")
            );
        }
    }

    #[test]
    fn a_dot_segment_on_something_that_is_not_a_table_says_what_it_is() {
        let error = interpolated(
            "const host = \"example.com\";\n",
            "const probe = f\"{host.port}\";\n",
        )
        .expect_err("a string has no fields");
        assert_eq!(
            error.message,
            "`host` is a string, not a table; `.port` names nothing in it"
        );
    }

    #[test]
    fn an_f_string_decodes_its_braces_and_its_escapes() {
        let text = interpolated(
            "const host = \"example.com\";\n",
            "const probe = f\"{{{host}}}\\tdone\\n\";\n",
        )
        .expect("braces, an escape and a splice");
        assert_eq!(text, "{example.com}\tdone\n");
    }

    /// Every withholding site registers the handle it withheld.
    ///
    /// The kinds `check_grants` reads are only some of the kinds an emit path
    /// can withhold; a set holding only those would be a coupling between two
    /// distant passes, true by accident. The webhook arm and the `emit_named!`
    /// kinds are the ones no grant can reach today, so they are pinned here at
    /// the emitter rather than through a document-level diagnostic.
    #[test]
    fn every_withholding_site_registers_its_handle() {
        let source = concat!(
            "webhook push_alice {\n",
            "    mount = nowhere;\n",
            "}\n",
            "repo notes {\n",
            "    remote = nowhere;\n",
            "}\n",
            "mqtt_client bob_hub {\n",
            "    url = nowhere;\n",
            "}\n",
            "mcp_server tools {\n",
            "    command = nowhere;\n",
            "}\n",
        );
        let (emitted, errors) = emitted(source);
        // One refused value per body, and each body's entity is absent from
        // the model rather than merely registered beside it.
        assert_eq!(errors.len(), 4, "{errors:?}");
        assert!(emitted.config.webhooks.is_empty());
        assert!(emitted.config.repos.is_empty());
        assert!(emitted.config.mqtt_clients.is_empty());
        assert!(emitted.config.mcp_servers.is_empty());
        for handle in ["push_alice", "notes", "bob_hub", "tools"] {
            assert_eq!(
                emitted.withheld.handles.get(handle),
                Some(&Grantable::No),
                "{handle}: {:?}",
                emitted.withheld.handles
            );
        }
    }

    /// A document that resolves whole leaves nothing withheld.
    ///
    /// The set says "declared, and not in the model". A handle that reached
    /// the model and stayed in it would make the set mean something weaker,
    /// which is the reading its one consumer relies on.
    #[test]
    fn an_entity_that_reached_the_model_is_not_withheld() {
        let (emitted, errors) = emitted(CLEAN_AGENTS);
        assert!(errors.is_empty(), "{errors:?}");
        assert_eq!(emitted.config.agents.len(), 2);
        assert!(
            emitted.withheld.handles.is_empty(),
            "{:?}",
            emitted.withheld.handles
        );
    }

    /// And a document where one of the two is refused withholds exactly it.
    #[test]
    fn a_refused_sibling_is_the_only_handle_withheld() {
        let source = CLEAN_AGENTS.replacen("\"sonnet\"", "nowhere", 1);
        let (emitted, errors) = emitted(&source);
        assert_eq!(errors.len(), 1, "{errors:?}");
        assert_eq!(
            emitted.withheld.handles.keys().collect::<Vec<_>>(),
            ["alice_pa"]
        );
    }

    /// Two agent classes, both instantiated, nothing refused.
    const CLEAN_AGENTS: &str = concat!(
        "agent Assistant() {\n",
        "    slug = \"alice-pa\";\n",
        "    model = \"sonnet\";\n",
        "}\n",
        "\n",
        "agent Helper() {\n",
        "    slug = \"bob-pa\";\n",
        "    model = \"sonnet\";\n",
        "}\n",
        "\n",
        "new alice_pa: Assistant();\n",
        "new bob_pa: Helper();\n",
    );
}
