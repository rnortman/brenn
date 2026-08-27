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
*Ownership*). A deployment carries a **verbatim copy** of that specification and
compiles its configuration against the copy.

Two files in two repositories, expected to be identical, with a release cycle
in between. The copy going stale — a spec change that shipped in the component
but was never re-copied, or a configuration written for a newer component than
the host has installed — is a configuration that compiled cleanly against a
contract the running artifact does not honour. Nothing in the artifact's own
load path catches it: the loader reflects imports and enforces grants, which
says nothing about ports or doctypes.

The package closes that window by making the author's specification travel with
the artifact, and by making the host check that the specification the
configuration used is byte-for-byte the one the component was built with.

## The three files

Flat, beside each other, sharing the artifact's stem:

```
brenn_processor_demo.wasm            the artifact
brenn_processor_demo.spec.brenn      the author's specification, verbatim
brenn_processor_demo.package.json    the binding record
```

The configuration names only the `.wasm`, as `component_path` on the consumer
instance. The siblings are derived from it by extension, so a package needs no
separate declaration anywhere in the configuration.

The packaged specification is renamed to the artifact's stem rather than keeping
the authored filename (`processor-demo.brenn`), so that every file in the
package follows from the artifact's basename with no lookup. The rename costs
nothing: the binding is over bytes, and the bytes are unchanged.

A **replay-world** component packages as two files, artifact and record, with no
specification. It has no component class, no ports and no grants; a
specification for one would be vocabulary with nothing to say. The record's
`world` field is what keeps the two shapes from being confused.

## The record, v1

```json
{
  "v": 1,
  "name": "brenn_processor_demo",
  "world": "brenn:processor",
  "artifact": "brenn_processor_demo.wasm",
  "artifact_sha256": "<64 lowercase hex>",
  "spec": "brenn_processor_demo.spec.brenn",
  "spec_sha256": "<64 lowercase hex>"
}
```

| field | meaning |
|---|---|
| `v` | Record schema version. This host reads `1` and refuses anything else. |
| `name` | The component's name — the artifact's stem. |
| `world` | The WIT package the artifact targets: `brenn:processor` or `brenn:replay`. |
| `artifact` | The artifact's basename, beside the record. |
| `artifact_sha256` | SHA-256 of the artifact's bytes, lowercase hex. |
| `spec` | The packaged specification's basename. Present **iff** `world` is `brenn:processor`. |
| `spec_sha256` | SHA-256 of the packaged specification's bytes. Present iff `spec` is. |

Spec-fields-iff-processor is enforced in both directions, at the emitter and at
the reader: a replay record carrying a specification and a processor record
carrying none both describe a component shape that does not exist.

The three names — `name`, `artifact`, `spec` — are checked against the files
they sit beside, not trusted. The host derives all of them from the artifact's
path, so a record naming another component's artifact, or a specification that
is not the one beside it, is a package that was assembled wrong and is refused
at boot. State them as the emitter does; a field the reader ignored would be a
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

1. Read `<stem>.package.json`. Missing, unreadable, unparseable, wrong `v`,
   unknown field, unknown world, spec fields inconsistent with the world, or a
   `name`/`artifact`/`spec` that is not the file it sits beside — each is a
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
  top-level consumer**, unless the file declaring it happens to be byte-identical
  to the packaged specification — that is, unless it *is* the author's file,
  standing alone. A file holding a class plus other items, or two classes, can
  match no package. This is the ownership rule made mechanical at boot rather
  than only at a deployment's CI.

Verification reads the artifact and the loader then reads it again, so there is
a window in which the bytes could change. Accepted deliberately: this binding is
anti-drift, not anti-attacker. Anyone who can write the components directory
between the two reads already owns the host — that directory is
operator-installed beside the operator's configuration, and the trust table in
`security-posture.md` puts both on the same side.

## Authoring an out-of-tree component

To be loadable, ship the three files above into the host's components directory,
with the record's hashes matching the bytes beside it. Concretely:

1. Build the component and hash it: `sha256sum <artifact>`.
2. Copy your specification next to the artifact as `<stem>.spec.brenn` and hash
   that too. Processor world only.
3. Write `<stem>.package.json` in the shape above.
4. Give the deployment the same specification bytes to copy into its own
   configuration tree, and have it `use` them there. If the two copies differ by
   so much as a comment, the host refuses to boot and says so.

A specification must be a file declaring exactly one component class and nothing
else — that is the authoring convention anyway (`config-dsl.md`), and the
binding makes it load-bearing.

## In-tree packaging

In this repository packages are built, not written. `component_package` in
`bazel/wasm/defs.bzl` runs `bazel/wasm/emit_package.sh` over a built component
and its authored `config/specs/<kind>.brenn`, emitting the record and the
renamed copy. The world is declared on the target and cross-checked against the
artifact's own `brenn:` imports, so a component that moved between worlds cannot
keep a stale tag.

Adding a shipped component means two entries, not one: the artifact's basename
in `brenn-wasm/deployed-components.txt`, and a `component_package` in
`brenn-wasm/BUILD.bazel`'s `COMPONENT_PACKAGES`. `//brenn-wasm:deployed_components_test`
holds both directions — a manifest entry with no package target fails there
rather than shipping a component the host will refuse.

Downstream of that, the release tree stages all three files into `lib/`,
`bazel/release/package_check.sh` re-computes both hashes over the staged tree so
the tarball is proven internally bound before it ships, and the deploy script
installs the sidecars beside each artifact.

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
