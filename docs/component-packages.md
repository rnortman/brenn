# Component packages

A backend WASM component does not ship as a bare `.wasm`. It ships as a
**package**: the artifact, the specification its author wrote, and a record
binding the two. The host re-computes that binding at boot and refuses to start
when anything disagrees.

This page is the contract. An out-of-tree component author needs everything
here and nothing else to make a backend component loadable. Surface-placed
components are bound the same way with a different record shape, which is
in-tree only — the last section says why.

## Why the binding exists

A component's specification — its abi, its ports, their doctypes, which ports
are optional, the capabilities it needs — is a statement about the artifact, and
only the artifact's author is in a position to make it (`config-dsl.md`,
*Ownership*). A deployment does not restate it: it imports the author's file
(`use @<kind>::*;`) from a module root the release installed, and compiles its
configuration against those bytes.

The bytes still have to be the ones the installed artifact was built with. A
module root left behind by an earlier release, or a configuration written for a
newer component than the host has installed, is a configuration that compiled
cleanly against a contract the running artifact does not honour. Nothing in the
artifact's own load path catches it: the loader reflects imports and enforces
grants, which says nothing about ports or doctypes.

The package closes that window by making the author's specification travel with
the artifact, and by making the host check that the specification the
configuration used is byte-for-byte the one the component was built with.

## Contract evolution

Two kinds of contract come out of this repository, and they evolve under
different rules because they are consumed at different times.

**Runtime contracts are additive.** These are what a running host reads from a
bundle built earlier, against a brenn commit the host did not choose. The host
and the bundle are installed by different releases, so a cut in one of these is
a boot refusal on somebody else's machine:

- the WIT packages `brenn:processor` and `brenn:replay` — a new capability is a
  new interface, and no existing shape moves;
- the **backend package record** (`package.json`, below) — `v` is the whole
  story, and a bump re-releases every bundle;
- the **surface record** `processor/<kind>/manifest.json` v2 and the served
  layout `processor/<kind>/{<kind>.js, <kind>.component.wasm,
  <kind>.spec.brenn, manifest.json, …}`;
- the grant-word set and the import→capability mapping both hosts reconcile
  (`capability_for_import`, `SURFACE_IMPORTS`);
- the **packaged-module subset of the DSL** — `component` classes, `assembly`,
  `const`, `use @` — because the host's compiler reads a bundle's
  `modules/<name>.brenn` at boot.

A new class attribute arrives with a default, a new grant word is a new WIT
interface, a new record field is a `v` bump, and no existing vocabulary in that
subset changes meaning. Breaking one of these is a deliberate coordinated
event: every bundle is re-released against the new brenn before brenn's pin
moves, and the design that makes the cut says so.

**Build-time contracts are hard-cut, at the pin.** These are consumed against a
commit the consumer names in its own `git_override`: the macros in
`bazel/wasm/defs.bzl` and `bazel/surface/dist.bzl`, `brenn-guest`'s Rust API,
the scaffold's generated module shape, `surface/page-harness`, `dsl_cli`'s
subcommands, and the guest alias sets. A cut costs the consumer a migration
when it next bumps that pin, and nothing before, because nothing that is
already deployed reads them. `examples/component/` is the canary that a cut is
complete on brenn's side; a consumer repository's CI at its next pin bump is
where the cut is paid.

Freezing the macro vocabulary of a one-operator ecosystem buys nothing a pin
does not already buy, which is why the two are split rather than held to one
promise. There is no changelog file: the git history of the files named above
is the record.

The out-of-tree population that these rules exist for is not hypothetical any
more — `brenn-component-demo` is a repository outside this one that builds
components with these macros for both placements, bundles them, and deploys.

## The package directory

A package is a directory under the host's components root, named by the
**package name**:

```
processor-demo/
  package.json               the binding record
  processor-demo.brenn       the author's specification, verbatim
  brenn_processor_demo.wasm  the artifact, under its built basename
```

The package name is the whole reference. A configuration states no location at
all: it imports the author's vocabulary as `use @processor-demo::*;`, and the
host resolves `<components root>/processor-demo/` from that same name when it
loads an instance of a class the module declares. The components roots are
named on the command line, `serve --components <dir>`, once per root; exactly
one of them may hold the directory (*Bundles and multiple roots*, below).

So the package name, the module name and the directory's basename are one name,
and the packaged specification carries it too. Only the artifact keeps its own
built basename — the record names it, and renaming it would buy nothing.

`<name>.brenn` is the component's **packaged module**, not merely its spec. It
is the authored file entire, so besides the component class it may carry the
assemblies and constants its author ships as the vocabulary for using the
component — everything a deployment imports when it writes `use @<name>::*;`.
What it may never carry is instantiation; the discipline is in `config-dsl.md`,
*Packaged-module imports*.

A **replay-world** component packages as a directory holding two files, artifact
and record, with no specification. It has no component class, no ports and no
grants; a specification for one would be vocabulary with nothing to say. The
record's `world` field is what keeps the two shapes from being confused. Its
name is therefore not anchored by an import — a replay component ships no
module — so a webhook endpoint states the package outright, as
`component = "replay-generic";` on its `replay_protection` block, and a typo
surfaces at boot rather than at compile.

## The record, v2

The record's basename is fixed: `package.json` in the package directory. There
is no shared stem left for it to derive one from.

```json
{
  "v": 2,
  "name": "processor-demo",
  "world": "brenn:processor",
  "artifact": "brenn_processor_demo.wasm",
  "artifact_sha256": "<64 lowercase hex>",
  "spec": "processor-demo.brenn",
  "spec_sha256": "<64 lowercase hex>"
}
```

| field | meaning |
|---|---|
| `v` | Record schema version. This host reads `2` and refuses anything else. |
| `name` | The package's name — the directory's basename, and the module name a configuration imports. |
| `world` | The WIT package the artifact targets: `brenn:processor` or `brenn:replay`. |
| `artifact` | The artifact's basename within the package directory. No path separator; must end `.wasm`. |
| `artifact_sha256` | SHA-256 of the artifact's bytes, lowercase hex. |
| `spec` | The packaged specification's basename, always `<name>.brenn`. Present **iff** `world` is `brenn:processor`. |
| `spec_sha256` | SHA-256 of the packaged specification's bytes. Present iff `spec` is. |

**v1 → v2 is a breaking change.** v1 was a flat trio of sidecar files sharing
the artifact's stem, and its `name` was that stem; v2 is a directory and its
`name` is the package. A v2 host refuses a v1 record outright, naming the
version skew. There is no shim and no dual-read window — see the stance below.

Spec-fields-iff-processor is enforced in both directions, at the emitter and at
the reader: a replay record carrying a specification and a processor record
carrying none both describe a component shape that does not exist.

The three names — `name`, `artifact`, `spec` — are checked against the layout,
not trusted: `name` must equal the directory's basename, `spec` must equal
`<name>.brenn`, and `artifact` must be a plain `.wasm` basename inside the
directory. A record naming another package, or a specification that is not the
one beside it, is a package that was assembled wrong and is refused at boot. State them as the emitter does; a field the reader ignored would be a
guarantee this contract does not actually carry.

**There is no `imports` field.** The host reflects a component's imports from
the artifact itself and reconciles them against the grants the configuration
gave the instance. Restating them in the record would be a second copy of a fact
the artifact already carries, and the copy is the thing that rots.

**There is no provenance, signature, or release metadata.** Those are fields a
later version may add. They are absent rather than reserved.

### Compatibility stance

The record is a runtime contract; *Contract evolution* above is the rule it
evolves under. What is specific to the record: `v` is the whole story, a record
written by a newer build is refused by an older host rather than partially read,
and the reader rejects unknown fields for the same reason.

## What the host checks at boot

Immediately before loading a component, per instance:

0. Resolve `<package name>/` under exactly one components root. The name must be one plain
   directory name that does not begin with `.`: an empty name, a path
   separator, `.` or `..` names a location rather than a package, and a
   dot-named directory is one no release installs and one a glob-driven
   install sweep would leave behind — all refused before the name resolves to
   anything. A name with no directory under any root is a panic naming every
   root searched and the instantiation; a name present under two roots is a
   panic naming both. A configuration
   may import any module the module root ships, but only a component the
   release ships as a backend package is top-level loadable — a surface kind
   ships its module and no package, and this is where instantiating one lands.
   A host started without `--components` panics naming the flag.
1. Read `package.json` in that directory. Missing, unreadable, unparseable,
   wrong `v`, unknown field, unknown world, spec fields inconsistent with the
   world, or a `name`/`artifact`/`spec` the layout contradicts — each is a
   panic naming the path and the remedy.
2. Assert the record's `world` matches the way this instance is being loaded: a
   top-level consumer demands `brenn:processor`, a webhook replay endpoint
   demands `brenn:replay`. A cross-wired install is refused before any hash is
   compared.
3. Hash the artifact's bytes and compare to `artifact_sha256`; for a processor
   package, hash the packaged specification and compare to `spec_sha256`.
4. For a top-level consumer only: compare `spec_sha256` to the hash of the
   **configuration's** specification file — the file that declared the class the
   instance names. Byte-identical or refuse.

Step 4 is the drift check the package exists for, and it is byte equality rather
than a comparison of parsed facts on purpose. Byte equality transfers every
compile-time check the configuration passed — spec fit, port optionality,
doctypes, satisfiability — onto the artifact actually installed, because the
configuration compiled against exactly those bytes. Comparing facts would need a
second parse at boot and a decision about which facts count; the hash needs
neither and is strictly stricter.

Two consequences worth stating plainly:

- **Packaging is mandatory.** There is no opt-out and no warn-first window for
  any backend component a configuration loads. A component without a record does
  not load.
- **A class declared inline in a deployment's configuration cannot drive a
  top-level consumer.** That is now a compile refusal rather than a boot one:
  a top-level instance's class must come from a packaged module, because the
  module's name is the only thing the host has to resolve a package with.

Verification reads the artifact and the loader then reads it again, so there is
a window in which the bytes could change. Accepted deliberately: this binding is
anti-drift, not anti-attacker. Anyone who can write the components directory
between the two reads already owns the host — that directory is
operator-installed beside the operator's configuration, and the trust table in
`security-posture.md` puts both on the same side.

## Authoring an out-of-tree component

The record above is the load-time contract. The authoring path is brenn's own
build: brenn is a Bazel module, and the rules that build, gate and package its
components load from `@brenn//bazel/wasm:defs.bzl` in any module that depends
on it. An author depends on the module and builds with the same rules and the
same gates brenn's own components pass. No tool is shipped — `dsl_cli`,
`wasm-tools` and `wit-bindgen` are built or fetched inside the dependency, in
the consumer's execution configuration.

`examples/component/` is a complete consumer: one crate, one specification,
one `BUILD.bazel`, one root document, in its own Bazel root inside brenn's
repository. brenn's CI builds and tests it (`make example-check`) on every
push, so a change to the rules that a consumer would not survive fails brenn's
own gate. Copy it to start a component repository; what follows is what the
copy has to say and why.

### The `MODULE.bazel` prelude

```
bazel_dep(name = "brenn", version = "")
git_override(
    module_name = "brenn",
    commit = "<the brenn commit to build against>",
    remote = "https://github.com/rnortman/brenn.git",
)
```

brenn is in no registry, so the dependency is a `git_override` at a commit. The
example writes a `local_path_override` to `../..` in its place because it lives
in brenn's tree; nothing else about it differs from a real consumer.

### Three facts only the consumer can state

bzlmod reads these from the root module and ignores a dependency's, so brenn's
own declarations do nothing for a consumer:

- **The Rust toolchain.** A `rust.toolchain(...)` tag with brenn's
  `RUST_VERSION` (the constant at the top of brenn's `MODULE.bazel`), edition
  2024, and `extra_target_triples = ["wasm32-unknown-unknown"]`. brenn's crates
  compile under the consumer's toolchain, so a version below brenn's floor
  fails at compile with rustc's own diagnostics. Pin the same version.
- **The `fltk` override.** `bazel_dep(name = "fltk", version = "")` plus
  `git_override(module_name = "fltk", commit = ..., remote = ...)` at the commit
  brenn's `MODULE.bazel` names. `//brenn-dsl:dsl_cli`, which scaffolds the port
  module and runs the grant-parity gate, links fltk's runtime crates; fltk is in
  no registry and a dependency's `git_override` is ignored, so without this line
  module resolution fails with "fltk not found".
- **The fltk serde flag**, in the consumer's `.bazelrc`:
  `build --@fltk//crates/fltk-serde-core:serde=@brenn//brenn-dsl:fltk_serde`.
  fltk's serde-backed crates must link the serde instance `brenn-dsl` links;
  without the flag `@brenn//brenn-dsl:dsl_cli` fails to compile with
  mismatched-serde-instance errors.

One naming rule goes with them: the consumer's crate hub is not called `crates`
or `wasm_crates`. rules_rust refuses two hubs of one name across modules, naming
both, and brenn owns those two. The example's is `example_crates`. The hub needs
a committed cargo-bazel lockfile (`lockfile = "//:cargo-bazel-lock.json"`,
regenerated with `CARGO_BAZEL_REPIN=1 bazel mod deps`) and
`supported_platform_triples` covering `wasm32-unknown-unknown` and the host
triple — the same shape brenn's own hubs have.

### brenn's guest serde instance

`brenn-guest`'s public API is expressed in serde types: `push_events::<T:
Serialize>` and the spec-generated payload markers. A derive written against a
serde from the consumer's own hub produces impls for a different crate instance
and satisfies none of those bounds; the compile error names two crates called
`serde`. So a component that talks to brenn-guest takes serde from brenn's
aliases instead:

```
"@brenn//brenn-wasm/components/guest:brenn-guest",
"@brenn//brenn-wasm/components/guest:serde",
"@brenn//brenn-wasm/components/guest:serde_json",
```

and does not list serde in its own `Cargo.toml` at all. Those two aliases are
the whole set — the external crates that appear in brenn-guest's API. Every
other third-party crate a component wants comes from the consumer's own hub;
its types never cross the boundary.

### The gate set

The example's `BUILD.bazel` is the list: `guest_spec_scaffold` generates the
port and capability module from the specification (`guest-scaffolding.md`);
`wasm_guest_cdylib` builds the crate for wasm32 with explicit `deps` and
`proc_macro_deps` (`all_crate_deps(normal = True)` and
`all_crate_deps(proc_macro = True)` from the consumer's hub, plus the brenn
targets above); `wasm_component` componentizes and import-GCs it, and its
WASI-import test runs beside it; `component_package` emits the package
directory and cross-checks the declared world against the artifact's imports;
`grant_parity_test` holds the specification's `requires` equal to the
artifact's imports; `component_install_tree` assembles a components root from
the packages, shaped as a bundle's `components/`; `deployed_components_test`
holds a `deployed-components.txt` manifest equal to the packages built. The
specification lives beside the code (the example's `spec/`), and that directory
is also the module root the author's own fit check reads:

`config_fit_test` compiles a root document against the module roots a host
would resolve it from, so the author's own configuration is a build target
rather than a command to remember:

```
config_fit_test(
    name = "dev_fit",
    config = "config/dev.brenn",
    modules = ["//spec:modules", "@brenn//:modules"],
)
```

`bazel test //...` runs all of them.

The docstring of `bazel/wasm/defs.bzl` names every macro that is part of this
contract and the three that are not; `bazel/surface/dist.bzl` does the same for
the surface half. Both are build-time contracts under *Contract evolution*
above: hard-cut at the pin the consumer names.

### The surface half

A page-hosted component is not a different kind of component. There is no
`abi = dom`: a UI kind is `abi = processor` with `dom` in its `requires`, built
by the same `wasm_guest_cdylib` + `wasm_component` pair as a headless one. What
differs is where the artifact is staged and how the browser loads it, and that
is one more macro:

```
surface_processor_assets(
    name = "demo-panel_assets",
    kind = "demo-panel",
    component = "//demo-panel:component",
    spec = "//spec:demo-panel.brenn",
)
```

It transpiles the component with brenn's own pinned jco — the consumer states
nothing about node or jco, and the version recorded in the emitted record is
the version of the binary that ran — and emits one directory,
`processor/<kind>/`, holding the transpiled JS tree, `<kind>.component.wasm`,
the packaged specification `<kind>.spec.brenn`, and the `manifest.json` that
binds them. `grant_parity_test` arrives with it, exactly as it does for a
backend package.

One naming rule is worth stating because it used to be a build-green,
boot-refusing trap: **the `kind` must be the wire fold of the component class's
name** — `DemoPanel` → `demo-panel`. The served directory, the directory the
build stages, and the kind the compiler derives from the class all have to be
the same string. The staging step asserts it (it asks `dsl_cli wire-kind` for
the class's fold and fails naming both), so a mismatch is a build failure with
both names in it rather than a boot refusal about a missing manifest.

`brenn-component-demo` is the worked example beside `examples/component/`: a
repository outside brenn that ships one headless component, one DOM component,
two assemblies wiring them, host tests against both hostings, and a browser
suite — and deploys as a bundle.

### Without Bazel

The record is still the contract, and a non-Bazel author may still produce one
by hand: pick the package name; build the component and hash it
(`sha256sum <artifact>`); copy the specification into the directory as
`<name>.brenn` and hash it too; write `package.json` in the shape above; and
install the same specification bytes as `<name>.brenn` in a module root the host
is started with. If the two copies differ by so much as a comment, the host
refuses to boot and says so. Nothing on the loading side knows or cares which
path produced the bytes.

## Building a bundle

A **bundle** is a component repository's release: the tree an installer unpacks
on a host, described under *Bundles and multiple roots* below. `component_bundle`
stages it and pairs it with the same kind of contract gate brenn's own tarball
passes:

```
component_bundle(
    name = "bundle",
    manifest = "deployed-components.txt",
    packages = [":demo-counter_package"],
    spec_root = ["//spec:modules"],
    surface_kinds = [":demo-panel_assets", ":demo-counter_assets"],
)
```

At least one of `packages` and `surface_kinds` is required; `manifest` is
required exactly when `packages` is non-empty, and refused otherwise.
`spec_root` is required and may be empty. The tree lands under `<name>/`:

- `components/<package>/` per `component_package`, plus the manifest copied as
  `components/deployed-components.txt` and the `scripts/manifest_names.sh` an
  installer execs to read it. `packages` must be exactly the manifest's set — a
  bundle repository has no unshipped packages — and the assembly holds the two
  equal in both directions.
- `surface/processor/<kind>/` per `surface_processor_assets`, each staging
  target's directory copied whole. No kernel and no flat sidecars: exactly one
  surface root holds the kernel and that one is brenn's.
- `modules/<name>.brenn`, the packaged specification of every package and every
  kind, flat. A component shipped for both hostings is staged once and the two
  copies must be byte-identical or the build fails naming both.

`spec_root` is the repository's **authored** module root — glob it, so the label
tracks the directory — and the staged `modules/` tree is held set-equal to it,
byte for byte. That direction is what lets a deployment certify its `use @…;`
imports against a bundle repository's checkout instead of against a built
bundle: without it a file authored under that root and shipped by nothing is
vocabulary a config can import, a config gate accepts, and the host refuses at
its next boot.

`<name>_contract` is declared beside it: `bundle_check.sh` over the staged tree,
re-computing every hash, holding every record against the files beside it and
the module root against both, in both directions. It is the same script
`package_check.sh` runs over brenn's own tarball — one implementation, two
callers — so a bundle is held to the tarball's contract and not to a second,
looser one.

The bundle carries no `VERSION` and no tarball rule. Those are the deploying
side's: a release pipeline builds the tree from a pinned ref, adds its own
installer and a `VERSION`, and tars it, the way brenn's own tarball is built.

## Testing a component against the hosts

Both hosts are public, so a component can be driven where it is authored rather
than only after it is installed.

**Backend.** `brenn_wasm::ProcessorComponent::load(ProcessorLoadSpec { … })` is
the real host: it instantiates the artifact, links exactly the grants the load
spec names, and returns a `ProcessorOutcome` naming what the component
published. The fixtures a load spec needs — `noop_proc_alerter`, `allow_all`,
`test_out_spec` — are exported from `brenn_wasm_dispatch::tests` behind the
default-on `testutils` feature. Default-on because a consumer cannot enable a
feature on a dependency module's Bazel target; nothing a consumer writes turns
it on or off.

**Page.** `brenn_page_harness::Harness` is the recording page host brenn's own
DOM kinds are tested against: a fake element tree with a transcript of every
`create-element`, `set-text`, `append` and `listen` call, publish and park
records, and recording `alert` and `config` implementations.
`Harness::new(artifact, page, grants)` links exactly the grants given, so a
component reaching for a capability its specification does not declare fails to
instantiate rather than working in a test and refusing at boot.

Both take the artifact as a path, which is what a `rust_test` has to be told:

```
rust_test(
    name = "tests",
    data = ["//demo-counter:component", "//demo-panel:component"],
    env = {
        "DEMO_COUNTER_WASM": "$(rootpath //demo-counter:component)",
        "DEMO_PANEL_WASM": "$(rootpath //demo-panel:component)",
    },
    deps = [
        "@brenn//brenn-envelope",
        "@brenn//brenn-wasm",
        "@brenn//brenn-wasm-dispatch",
        "@brenn//surface/page-harness",
    ],
)
```

read back with `std::env::var`. brenn's own suites walk
`CARGO_MANIFEST_DIR/../brenn-wasm/target/components/` instead; that convention
is in-tree only and does not survive the move out, because a consumer crate sits
wherever its own repository puts it.

The costed part is the compile, not the run: depending on `brenn-wasm` and
`brenn-wasm-dispatch` pulls wasmtime and sqlite into the consumer's own output
base. That is the price of testing against the real hosts rather than a mock of
them, it is paid once per cold cache, and a CI job that does it wants a timeout
in hours rather than minutes on its first run.

A component that is meant to run at both placements is tested at both, on one
script, reduced to the one thing both hosts can be compared on: the sequence of
`(port, body)` pairs it published. The two hosts share no transcript type — one
returns an outcome, the other records host calls as strings — so each suite
reduces its own host's output and the two are compared against one constant.
That is the transplant invariant, at the grain a consumer can hold without a
shared abstraction.

## In-tree packaging

In this repository packages are built, not written. `component_package` in
`bazel/wasm/defs.bzl` runs `bazel/wasm/emit_package.sh` over a built component
and its authored `config/specs/<name>.brenn`, emitting the record and the
packaged copy. The world is declared on the target and cross-checked against the
artifact's own `brenn:` imports, so a component that moved between worlds cannot
keep a stale tag.

The same specification label also generates the component's guest-side port and
capability module — see `guest-scaffolding.md`, which also covers the CI check
holding a specification's `requires` equal to its artifact's imports.

Adding a shipped component means two entries, not one: the package name in
`brenn-wasm/deployed-components.txt`, and a `component_package` in
`brenn-wasm/BUILD.bazel`'s `COMPONENT_PACKAGES`. `//brenn-wasm:deployed_components_test`
holds both directions — a manifest entry with no package target fails there
rather than shipping a component the host will refuse.

Downstream of that, the release tree stages each package directory,
`bazel/release/package_check.sh` re-computes both hashes over the staged tree so
the tarball is proven internally bound before it ships, and the deploy script
syncs the package directories into the host's components root.

Both crate hubs in `MODULE.bazel` carry a committed cargo-bazel lockfile
(`cargo-bazel-lock.json` at the root and under `brenn-wasm/components/`) so
that another Bazel module can depend on brenn and build with these rules;
rules_rust does not load a non-root module's hub without one. Changing a
`Cargo.lock` without regenerating the matching lockfile fails the build with a
digest-mismatch error from rules_rust. That is expected, not a broken checkout:
run `CARGO_BAZEL_REPIN=1 bazel mod deps` and commit the regenerated file.

## The module root

A release also carries the authored modules themselves, as a flat `modules/`
tree:

```
modules/processor-demo.brenn        the backend component's authored module
modules/mode-clock.brenn            a surface kind's, harvested from surface/
modules/protobar.brenn
```

That is the directory a deployment's `--modules` names and its `use @<name>::…`
imports resolve against (`config-dsl.md`, *Packaged-module imports*), so the
staged name is the authored basename — the wire kind — rather than the
artifact's stem. Every component the release ships contributes one, backend and
surface alike, so a deployment can import whatever it instantiates; the backend
half is staged from the same `COMPONENT_PACKAGES` dict that builds the packages,
and the surface half is harvested out of the staged surface tree, whose kind
directories carry it.

`package_check.sh` holds the root to its packages in both directions: every
shipped specification has a byte-identical module staged, and every staged
module is byte-identical to a shipped specification — or listed as a library
module, below. A file that is neither is a module a deployment could import and
the host would refuse at boot.

### Library modules

Not every module a release ships belongs to a component. Shared vocabulary —
assemblies and constants a deployment stamps but no artifact implements — has no
package and no surface kind to be harvested off, so it is **listed** instead:

```
modules/surface-description.brenn   a library module: vocabulary, no artifact
modules/library-modules.txt         the list, one basename per line, sorted
```

A `release_package` or `component_bundle` names them in `library_modules`, each
staged as `modules/<basename>` with its basename appended to
`modules/library-modules.txt`. The list travels in the tree because every reader
of `modules/` — the contract test, a deploying repo's preflight — otherwise has
to pair each module with an owning package and must refuse what it cannot pair.
A tree that lists none carries no list file at all, so its `modules/` is
byte-identical to what it was before the carrier existed.

Two rules keep the two halves apart. A library module may not take a basename
the harvest already staged — two files under one import, decided by copy order —
and a staged module is owned *or* listed, never both, because two statements of
one ownership can disagree at the next pin. A bundle's library modules must also
be under its `spec_root`, which is the authored tree a deployment's config gate
reads instead of building the bundle.

Nothing else in the build compiles a library module: no component owns one, so
without a gate the first reader of it is the compiler at a deployment's boot,
with the service already stopped. Each `library_modules` entry therefore gets a
`library_module_test`, which compiles a one-line root document importing just
that module against just its own directory. A module that declares a top-level
channel, an instance, a principal — the packaged subset's top-level rules —
fails brenn's own build.

The gate stops at the top level. An assembly's body is resolved only when
something stamps it, and that root stamps nothing, so a body that names a depth
no constant or parameter resolves to, or reaches for anything else the resolver
refuses, passes here and is refused at a deployment's `config-check` instead.

## Bundles and multiple roots

brenn's release installs its packages and modules into one components root and
one module root, and the installer empties both on every deploy — each is
exclusively that release's. A component built elsewhere cannot land in either:
the next brenn deploy deletes it. So the host takes more than one of each.

`--modules DIR` and `serve --components DIR` are repeatable. Every root is a
directory of the same shape as brenn's own — `<name>.brenn` files flat in a
module root, `<name>/` package directories in a components root — and the host
treats the list as one namespace with a rule: **a name may appear under exactly
one root.** A module basename present in two module roots, or a package
directory name present in two components roots, is a boot refusal naming the
name and both roots, whether or not the configuration imports or instantiates
it — a broken install is refused independently of what today's configuration
happens to touch. The same path given twice is refused too, compared after
canonicalization. Identical bytes under two roots are still refused: two
releases shipping one module means one of them is stale the moment the other
updates. The module scan runs when the configuration loads; the package scan
runs immediately after, before anything is served.

A **bundle** is the release of a component repository — a tree carrying up to
three of the subdirectories brenn's tarball has, and only the ones it ships:

| tree | what it holds | installs as |
|---|---|---|
| `components/` | one `<name>/` package directory per backend component, plus `components/deployed-components.txt` and the `scripts/manifest_names.sh` an installer execs to read it | one `serve --components` root |
| `surface/` | `processor/<kind>/` per page-hosted kind — the transpiled tree, the component bytes, the packaged spec, the record binding them | one `serve --surface` root |
| `modules/` | the authored module of every one of them, flat | one `--modules` root |

No kernel bundle and no flat sidecars: exactly one surface root holds the
kernel, and that one is brenn's. Every `--surface` root must offer one or the
other — the kernel pair, or at least one `processor/<kind>/` — so a flag pointed
one directory off (a bundle's install root rather than its `surface/` tree) is
refused at that boot instead of at the later one that first stamps the kind.

`modules/` is always present and is empty when the bundle owes it nothing: a
replay-world package ships no specification, so a bundle whose packages are all
replay-world is imported by no configuration and named instead by a
`replay_protection` block's `component =`.

The repository is the store of record; the bundle is what its CI builds from a
pinned ref, the way brenn's tarball is built from brenn's. `component_bundle`
stages it and pairs it with the same contract gate brenn's own tarball passes,
so a record that does not bind the bytes beside it fails the bundle's build
rather than the target host's next boot. A bundle installs into roots of **its
own**, one per tree it ships, exclusively its own in the same sense as brenn's,
and the service is started with one flag more per root:

```
brenn --config prod.brenn --modules /srv/brenn/modules --modules /srv/caser/modules \
  serve --components /srv/brenn/lib --components /srv/caser/components \
  --surface /srv/brenn/surface --surface /srv/caser/surface
```

The exclusivity rule brenn's installer checks among its own install directories
covers bundle roots too: the deploying side names the directory bundles install
under alongside brenn's own, and refuses a layout in which one equals or nests
inside another. Without that, a bundle root placed inside one of brenn's sync
directories is deleted by brenn's next deploy, and the service then fails to
start with the "not an installed package directory" refusal naming the missing
package and every root searched. Give each bundle directories of its own. A bundle whose package name collides
with a brenn package is refused at boot as a cross-root duplicate; the name is
the author's to change.

One shared root with per-release ownership manifests was considered and
rejected: two bundles shipping one package name would overwrite each other
silently, and a single root has no second copy for the host to compare. Multiple
roots give the host the collision, and it refuses.

### What a deployment still copies by hand

One piece of brenn's own vocabulary still reaches a deployment by
transcription rather than by import, and it drifts silently.

The **chrome wiring**. A surface must hold exactly one chrome, and the block
that wires its four reserved `local:brenn/*` control planes plus `io toast-tick`
is written out per page. An unrecognised reserved name is refused; a *missing*
plane is not, so a page whose chrome omits one boots and goes quiet.
`TODO(standard-chrome-vocabulary)`.

Until it is packaged vocabulary, that block in a bundle repository's `config/`
is a copy with a shelf life, and the pin bump that updates brenn is when it is
re-copied. The self-description assemblies — `SurfaceCommons`,
`SurfaceDescription(slug)` and `KindDescription(kind)` — are not in that
position: they ship as a library module (*Library modules*, above), imported
with `use @surface-description::*;`.

Each has its own arity, and a deployment whose stamps are missing is refused
with every missing channel named at once. `config-check` makes that refusal — so
the bundle installer's pre-stop check does too — and the fit test does not, since
it compiles without lowering. `SurfaceCommons` is stamped **once per
deployment**; `SurfaceDescription` **once per surface slug**;
`KindDescription` **once per processor kind the deployment's pages instantiate**,
the chrome kind included:

```
use @surface-description::*;

new surface_commons: SurfaceCommons;
new demo_desc: SurfaceDescription(slug = "demo");
new panel_kind_desc: KindDescription(kind = "demo-panel");
new chrome_kind_desc: KindDescription(kind = "chrome");
```

`SurfaceCommons` carries the two channels every surface-serving deployment
publishes on whatever surfaces it declares — the error lane and the topology
index — which is why it is stamped once and not per slug.

## Surface packages

Surface-placed components are bound the same way, with a different carrier. A
surface kind is not one file: it ships as a jco-transpiled directory. So the
record takes the shape that already affords. The vocabulary is the backend's — a
packaged verbatim copy of the author's specification, a content hash of it in a
versioned `deny_unknown_fields` record, and byte-hash equality checked at boot —
and only the carrier differs.

`processor/<kind>/manifest.json` is v2: `source_sha256`, `jco_version`,
`imports` and `files` plus `spec` and `spec_sha256`, with the packaged copy in
the kind directory as `<kind>.spec.brenn`. The spec fields are required; there
is no spec-less surface kind.

At boot `validate_surface_assets` reads the record for every configured kind,
re-derives each stated filename from the kind, re-hashes each named file, and
then binds **per instance**: the specification hash the configuration compiled
against must equal the record's. Per instance rather than per kind because the
compiler's kind fold admits comment-divergent copies of one class under one
kind; at most one of those copies is the bytes the tree was built from, and the
one that is not now refuses at boot. The surface kernel is exempt: it is not a
component, so it has no kind, no class and no specification to bind.

The records are emitted by the build, in tree and out of it alike:
`surface_processor_assets` names the spec at its call site. `package_check.sh`
re-verifies every surface record over the staged release tree, and the deploy
installs the asset tree as a whole rather than overlaying it, so no file from a
prior release survives beside a fresh record.

**These shapes are external contracts**, granted that status deliberately now
that there is an out-of-tree surface authoring path to consume them
(*Authoring an out-of-tree component*, the surface half). The record and the
served layout are runtime contracts under *Contract evolution*: a host reads
them out of a bundle built against a brenn commit it did not choose, so a new
field is a `v` bump and the layout below `processor/<kind>/` does not move.
Both records — the backend package's and this one — are now external and
versioned; the asymmetry that used to be stated here is gone, and the two
carriers remain two only because a surface kind is a directory and a backend
component is a file.
