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
loads an instance of a class the module declares. The components root is named
once on the command line, `serve --components <dir>`.

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

0. Resolve `<components root>/<package name>/`. The name must be one plain
   directory name that does not begin with `.`: an empty name, a path
   separator, `.` or `..` names a location rather than a package, and a
   dot-named directory is one no release installs and one a glob-driven
   install sweep would leave behind — all refused before the name resolves to
   anything. A name with no directory there
   is a panic naming the resolved path and the instantiation. A configuration
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

To be loadable, ship a package directory as above into the host's components
root, with the record's hashes matching the bytes beside it. Concretely:

1. Pick the package name. It is the name a deployment writes in
   `use @<name>::*;`, the directory's basename, and the packaged
   specification's basename.
2. Build the component and hash it: `sha256sum <artifact>`.
3. Copy your specification into the directory as `<name>.brenn` and hash that
   too. Processor world only.
4. Write `package.json` in the shape above.
5. Install the same specification bytes into the host's module root as
   `<name>.brenn`, so a deployment can import them. If the two copies differ by
   so much as a comment, the host refuses to boot and says so.

## In-tree packaging

In this repository packages are built, not written. `component_package` in
`bazel/wasm/defs.bzl` runs `bazel/wasm/emit_package.sh` over a built component
and its authored `config/specs/<name>.brenn`, emitting the record and the
packaged copy. The world is declared on the target and cross-checked against the
artifact's own `brenn:` imports, so a component that moved between worlds cannot
keep a stale tag.

Adding a shipped component means two entries, not one: the package name in
`brenn-wasm/deployed-components.txt`, and a `component_package` in
`brenn-wasm/BUILD.bazel`'s `COMPONENT_PACKAGES`. `//brenn-wasm:deployed_components_test`
holds both directions — a manifest entry with no package target fails there
rather than shipping a component the host will refuse.

Downstream of that, the release tree stages each package directory,
`bazel/release/package_check.sh` re-computes both hashes over the staged tree so
the tarball is proven internally bound before it ships, and the deploy script
syncs the package directories into the host's components root.

## The module root

A release also carries the authored modules themselves, as a flat `modules/`
tree:

```
modules/processor-demo.brenn        the backend component's authored module
modules/mode-clock.brenn            a dom kind's, harvested from surface/
modules/protobar.brenn
```

That is the directory a deployment's `--modules` names and its `use @<name>::…`
imports resolve against (`config-dsl.md`, *Packaged-module imports*), so the
staged name is the authored basename — the wire kind — rather than the
artifact's stem. Every component the release ships contributes one, backend and
surface alike, so a deployment can import whatever it instantiates; the backend
half is staged from the same `COMPONENT_PACKAGES` dict that builds the packages,
and the surface half is harvested out of the staged surface tree, whose records
name their kinds.

`package_check.sh` holds the root to its packages in both directions: every
shipped specification has a byte-identical module staged, and every staged
module is byte-identical to a shipped specification. A file that is neither is a
module a deployment could import and the host would refuse at boot.

## Surface packages

Surface-placed components are bound the same way, with a different carrier. A
surface artifact is not one file: a `dom` kind ships as a wasm-bindgen module
pair flat in the surface asset root, a surface-hosted `processor` kind as a
jco-transpiled directory. So the record takes the shape each already affords.
The vocabulary is the backend's — a packaged verbatim copy of the author's
specification, a content hash of it in a versioned `deny_unknown_fields`
record, and byte-hash equality checked at boot — and only the carrier differs.

**A dom kind**, flat in the asset root, sharing the artifact stem:

```
brenn_mode_clock.js                 the module
brenn_mode_clock_bg.wasm            its wasm
brenn_mode_clock.spec.brenn         the author's specification, verbatim
brenn_mode_clock.manifest.json      the binding record, v1
```

The record states the kind, each of the three named files, and a hash of each.
The module-pair hashes make the dom path's own staleness detectable for the
first time; the shared `snippets/` tree, the `.d.ts` files and the help and
schema sidecars are deliberately unhashed — nothing per-kind can state
`snippets/` truthfully, and the rest are not load-bearing at boot.

**A surface-hosted processor kind** already had a record, so the record grew
rather than gaining a sibling. `processor/<kind>/manifest.json` is v2: the
pre-existing `source_sha256`, `jco_version`, `imports` and `files` plus `spec`
and `spec_sha256`, with the packaged copy in the kind directory as
`<kind>.spec.brenn`. Both new fields are required; there is no spec-less
surface kind.

Neither record states the abi. Its location and shape is that statement — a
record in the asset root and a record under `processor/<kind>/` cannot be
confused — and the configuration's abi decides which lookup runs.

At boot `validate_surface_assets` reads the record for every configured kind,
re-derives each stated filename from the stem, re-hashes each named file, and
then binds **per instance**: the specification hash the configuration compiled
against must equal the record's. Per instance rather than per kind because the
compiler's kind fold admits comment-divergent copies of one class under one
kind; at most one of those copies is the bytes the tree was built from, and the
one that is not now refuses at boot. The surface kernel is exempt: it is not a
component, so it has no kind, no class and no specification to bind.

In-tree, the records are emitted by the build. `surface_component` derives its
kind's specification label from the package path — `//:config/specs/<kind>.brenn`
— so a dom component with no authored specification does not build at all;
`surface_processor_assets` names the spec at its call site. `package_check.sh`
re-verifies every surface record over the staged release tree, and the deploy
installs the asset tree as a whole rather than overlaying it, so no file from a
prior release survives beside a fresh record.

**These shapes are in-tree contracts, not external ones.** There is no
out-of-tree surface authoring path today, and claiming a contract nobody can
consume would freeze a shape that still needs room to move as surface
components migrate out of tree. The version counters are in place for the day
that status is granted deliberately. The backend package above is an external
contract and is stated as one; this asymmetry is deliberate, not an oversight.
