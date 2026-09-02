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

The record is an **external contract**, and the house rule for external
contracts applies: no compatibility shims, stated up front. `v` is the whole
story. A version bump is a breaking change; a record written by a newer build is
refused by an older host rather than partially read, and the reader rejects
unknown fields for the same reason. Ship the release whose binary and components
were built together.

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

```
bazel test //...
bazel run @brenn//brenn-dsl:dsl_cli -- check --modules spec config/dev.brenn
```

The docstring of `bazel/wasm/defs.bzl` names every macro that is part of this
contract and the two that are not, and states the evolution policy the
contract is under. Until a component repository outside brenn's exists, a
macro's attributes and outputs may move as a hard cut; the example is the
canary that such a cut is complete, not the population that ends the regime.

### Without Bazel

The record is still the contract, and a non-Bazel author may still produce one
by hand: pick the package name; build the component and hash it
(`sha256sum <artifact>`); copy the specification into the directory as
`<name>.brenn` and hash it too; write `package.json` in the shape above; and
install the same specification bytes as `<name>.brenn` in a module root the host
is started with. If the two copies differ by so much as a comment, the host
refuses to boot and says so. Nothing on the loading side knows or cares which
path produced the bytes.

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
module is byte-identical to a shipped specification. A file that is neither is a
module a deployment could import and the host would refuse at boot.

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

A **bundle** is the release of a component repository — a tree with the same two
subdirectories brenn's tarball has, `components/<name>/` (the package
directories, plus `components/deployed-components.txt`) and
`modules/<name>.brenn` (the same specifications, flat). The repository is the
store of record; the bundle is what its CI builds from a pinned ref, the way
brenn's tarball is built from brenn's. The example's `component_install_tree`
output and its `spec/` directory are exactly those two trees. A bundle installs
into **its own** components root and module root, exclusively its own in the
same sense as brenn's, and the service is started with one `--modules` and one
`--components` more per bundle:

```
brenn --config prod.brenn --modules /srv/brenn/modules --modules /srv/caser/modules \
  serve --components /srv/brenn/lib --components /srv/caser/components
```

The exclusivity rule brenn's installer checks among its own six install
directories extends to bundle roots by statement rather than by check — brenn's
installer cannot see directories its configuration does not name. A bundle root
placed inside one of brenn's sync directories is deleted by brenn's next
deploy, and the service then fails to start with the existing "not an installed
package directory" refusal naming the missing package and every root searched.
Give each bundle directories of its own. A bundle whose package name collides
with a brenn package is refused at boot as a cross-root duplicate; the name is
the author's to change.

One shared root with per-release ownership manifests was considered and
rejected: two bundles shipping one package name would overwrite each other
silently, and a single root has no second copy for the host to compare. Multiple
roots give the host the collision, and it refuses.

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

In-tree, the records are emitted by the build: `surface_processor_assets` names
the spec at its call site. `package_check.sh`
re-verifies every surface record over the staged release tree, and the deploy
installs the asset tree as a whole rather than overlaying it, so no file from a
prior release survives beside a fresh record.

**These shapes are in-tree contracts, not external ones.** There is no
out-of-tree surface authoring path today, and claiming a contract nobody can
consume would freeze a shape that still needs room to move as surface
components migrate out of tree. The version counters are in place for the day
that status is granted deliberately. The backend package above is an external
contract and is stated as one; this asymmetry is deliberate, not an oversight.
The authoring paths are asymmetric for the same reasons: a backend component is
built out of tree by depending on brenn's module (*Authoring an out-of-tree
component*), while a surface kind's jco transpile and served-tree layout are
still brenn's build alone.
