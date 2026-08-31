# Guest scaffolding

A component's specification is the single, hash-bound statement of its shape:
its abi, its ports, their doctypes and directions, and the capabilities it
needs. Everything on the guest side that names a port or reaches a capability
is generated from it, so guest source cannot drift from the shape the host was
configured against.

The generator is `dsl_cli scaffold`; its output is a module the guest crate
declares as `mod spec;`. Nothing about it is committed.

```
dsl_cli scaffold [--class <Name>] -o <out.rs> <spec.brenn>
```

## Why generation, and why from this file

Before it, every port name was a free string on both sides of every ABI. A
typo was detected at the first publish rather than at boot; inbound dispatch
was a hand-written string match whose arms nothing checked; documentation
tables were held shut by comments saying that nothing compiled them shut.

The input label is the *same label* the component package embeds and the host
hash-binds at boot (`component-packages.md`). So the bytes that generate the
code are the bytes the running system checks, and the generated module inherits
the whole binding chain for free: a port the module does not know about is a
port no configuration could have bound.

## What is generated

The abi is read from the class's `abi` attribute, not passed as a flag.

### Both abis

- **`InPort`** — one variant per port facing inbound (`in` and `io`), with
  `ALL` in declaration order, `name()`, and `from_name()`. Dispatch on this
  enum and adding a port to the specification breaks every guest that has not
  handled it; transposing two arms is a type error where the arms bind
  differently. A class with no inbound ports yields an uninhabited enum, which
  is correct — such a component is activated only for its own deferred views.
- **`pub mod port`** — the raw name of every port, every direction, as a
  `&'static str` constant, for the parts of the SDK that take a name as text
  (`publish_deferred`, `deferred_for`, `defer_cancel`).
- **Doctypes** carried as doc comments on the variant, the handle and the
  constant. They are nominal tags with no runtime consumer; the comment is the
  whole of their appearance here. A doctype written as an interpolated string
  carries no note: interpolation names constants only the resolver has scope
  for.
- The class's own prose becomes the module's `//!` documentation.

### Processor only

- **`InPort::of(&PortWindow)`** — classifies an activation window. An
  undeclared port is not bad input but host misbehavior, since the artifact is
  hash-bound to the specification that generated the module, so it fails the
  activation.
- **A payload marker trait and a `const fn` publish handle per outbound port**
  (`out` and `io`). The handle's type parameter is bounded by the port's own
  trait, so the guest binds a payload type to a port once, as an impl:

  ```rust
  impl spec::ResultsPayload for Body<'_> {}
  ```

  and then publishes through either idiom — a `const` for an owned payload,
  `const OUT: OutPort<Body> = spec::results();`, or an inline call for a
  borrowed one, `spec::results().publish(&body)?`, whose lifetime no `const`
  can name. Publishing a type that is not bound to the port does not compile.
  Two types may bind to one port; two message shapes on one channel is
  expressible, and the impls are the visible record of it. The SDK's own
  `OutPort::new` stays public and unbounded, for a guest with no generated
  module. The binding covers publishes through the handle and nothing else:
  the string-taking SDK surface — `publish_json`, `publish`,
  `publish_deferred` — takes any payload and is deliberately left that way,
  which is the idiom for a raw-string body. There the generated module is
  name-only, through the `port` constant.
- **One capability re-export per declared grant word that names an SDK
  module** — `store`, `log`, `alert`, `config`, `tools`, `mqtt`. Reaching a
  capability through `spec::` means deleting the word from the specification
  breaks the guest compile. It is a nudge and not a wall: a guest can still
  `use brenn_guest::store` directly, and what actually enforces the
  specification against the artifact is the grant-parity check below. `ports`
  is embodied by the handles themselves; `takeover` is dom-only vocabulary.

### Dom only

The dom emission is deliberately lighter, because the dom SDK surface is free
functions over `&str` and its activation type is a plain serde struct: the
enum and the `port` module, no window classifier (dom components match on
`window.port` directly), no publish handles (there is no dom `OutPort`), and no
capability re-exports (dom capabilities are free functions, not modules). That
asymmetry is recorded rather than papered over; it is revisited if the dom SDK
grows capability modules.

## Identifier mapping

Port names are kebab-case on the wire. They map to:

| namespace | form | `self-tick` becomes |
|---|---|---|
| `InPort` variant | `CamelCase` | `InPort::SelfTick` |
| payload marker trait | `CamelCase` + `Payload` | `spec::SelfTickPayload` |
| publish handle | `snake_case` | `spec::self_tick()` |
| `port` constant | `SCREAMING_SNAKE_CASE` | `spec::port::SELF_TICK` |

The three namespaces — type name, publish handle, constant — cannot collide
with each other, only within themselves. The variant and the payload trait
share the type namespace, so no reader is asked to tell one `SelfTickPayload`
from another.

## What the generator refuses

Each refusal is a diagnostic with a span into the specification.

- Zero component classes, or more than one without `--class`; a `--class` that
  names no class in the document. Non-class items — `assembly`, `const`,
  `use` — are ignored, so a specification shipping an assembly beside its class
  generates fine.
- An unknown abi word, or an unknown capability word.
- A port name that does not map to a legal Rust identifier, that collides with
  another port's mapped identifier within one namespace (two ports differing
  only in `-` versus `_`), or whose mapped identifier would be a Rust keyword.
  Both checks are per *emitted* identifier, not per port: an inbound port named
  `in` emits `InPort::In` and `port::IN` and no function, so the keyword its
  handle would have been is never written; and a dom class emits no publish
  handle and no payload trait at all, so `in foo-payload; out foo;` is one
  `FooPayload` under the processor abi and a collision only there. There is no
  raw-identifier escape hatch — refusal keeps names boring.

The generator's validation stops there, deliberately. The compiler — class
resolution, the grant lists, the spec fit check — remains the sole authority on
what a specification may say. The generator refuses only what it cannot emit,
so it never becomes a second opinion on legality: a specification it accepts
and the resolver refuses fails at deployment compile, as it always did.

## The build wiring

`guest_spec_scaffold(name, spec, class_name = ...)` in `bazel/wasm/defs.bzl`
runs the generator in the exec configuration, runs rustfmt over what it wrote,
and declares `src/spec.rs` as its output. Crates take it through the
`generated_srcs` parameter — `{"src/spec.rs": ":spec"}`, the path a generated
module occupies in the crate mapped to the target producing it — which filters
that path out of the source glob and appends the generated file:
`wasm_guest_cdylib` for backend components, `surface_wasm_crate` for surface
ones, feeding both its host `rust_library` and its wasm32
`rust_shared_library`, since the generated module is plain Rust. A raw-WIT
crate's `src/bindings.rs` rides the same parameter. A dom kind's specification
label is derived from its package directory, so `surface_component` wires this
with no per-component editing.

**The output is untracked.** `.gitignore` covers `src/spec.rs` under both
component trees. Bazel's dependency tracking replaces a drift gate: the
specification label is an action input, so editing the specification rebuilds
the module and every stale guest fails to compile. There are no committed
copies and no parity test over the output. What is pinned instead is the
generator itself — `brenn-dsl/tests/corpus/scaffold/` holds specifications and
their expected modules as committed goldens, generated and formatted exactly as
a component's module is, so a golden is byte-for-byte what a real crate
compiles.

Layout is rustfmt's, not the emitter's: the build rule formats the generator's
output, so the emitter states the code and the toolchain states how it looks.
The goldens are additionally compiled, which is what puts rustc and clippy on
the shapes no in-tree component has — an uninhabited port enum, a class with no
ports at all. The dom half builds on the host
(`//brenn-dsl:scaffold_goldens_compile`); the processor half names the guest SDK
and so builds for wasm32, reached from a host command line through
`//brenn-dsl:scaffold_processor_goldens_wasm32`.

One consequence to expect while working in a checkout: `rustfmt` cannot be run
by hand over a crate whose `mod spec;` target is absent from the worktree.
Generate the module into `src/` temporarily and delete it afterwards. The
build's rustfmt aspect formats only a target's committed sources, so it never
hits that.

## The grant-parity check

Generation makes the guest agree with the specification. The other edge — that
the *artifact* agrees with it — is a Bazel test per packaged processor
component:

```
reflected_imports(artifact) − {brenn:processor/types} == { g.wit_import() | g ∈ spec.requires }
```

Set equality, both directions. An import the specification does not require is
drift, because the code grew a need the author never declared; a required grant
the artifact never imports is drift too, because the author declared a need the
code does not have. Replay-world packages have no specification and are
excluded, as everywhere.

The grant-to-import mapping is single-sourced in
`brenn-envelope/src/grants.rs` (`ComponentGrant::wit_import`), and the host
linker gates on those same words — there is no parallel capability enum to
drift from them. A word naming no interface (`takeover`, which is consented to
at a page binding) is refused in a processor class's `requires` rather than
skipped, so the equality is total over the list.

Names are compared *with* their versions, byte for byte against the canonical
name each word links. That is stricter than the host, which resolves an import
by semver compatibility, so the check can refuse an artifact the host would have
bound — never the reverse. The alternative, comparing version-stripped names,
lets an artifact built against another version of an interface pass here and
then be refused at load, which is the failure this edge exists to move to build
time. If an interface version ever moves, the host's matching rule and this
check are updated together.

Making it a test rather than a packaging step keeps the emit script free of
grant knowledge and still gates every release: `make check` runs the full test
graph, and CD re-runs it against the pinned ref.

With this in place the triangle closes on all three edges — specification
against instance at compile, instance against artifact at boot, specification
against artifact in CI.

### A processor specification has no optional grants

The check formalizes a consequence worth stating on its own. Under the host's
link-only-when-granted policy, importing an ungranted interface is a load-time
panic, and an artifact cannot import conditionally. So an `optional` grant on a
processor class is unexercisable: if the artifact imports the interface, an
instance omitting the word — which the fit check permits for `optional` — is a
guaranteed boot panic; if the artifact does not import it, granting it does
nothing. The resolver refuses the word where it is written. Dom classes keep
`optional`, because a surface grant is not an import the linker decides. See
`config-dsl.md`, *Authority*.

## Status

**The generated module's shape is an in-tree contract for now.** No
out-of-tree authoring path consumes it yet, and `dsl_cli scaffold` is not
shipped anywhere an out-of-tree author could run it. Both are deliberate: the
shape is promoted to an external contract when a distribution path for it is
designed, not by accident of being written down here.
