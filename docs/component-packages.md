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

- the WIT packages `brenn:processor` (`brenn-wasm/wit/processor.wit`,
  `processor-0.2.wit` and `processor-0.3.wit`, read together) and
  `brenn:replay` — a new capability is a new interface; a breaking change to a shape is a new version of the
  interfaces that change and of no other, so an interface's version is the
  version at which its shape last changed. brenn releases first and serves the
  old and the new `brenn:processor` contract together for a period;
  components follow on their own schedule (*The byte-carrying contract*,
  below);
- the **backend package record** (`package.json`, below) — `v` is the whole
  story, and a bump re-releases every bundle;
- the **surface record** `processor/<kind>/manifest.json` v3 and the served
  layout `processor/<kind>/{<kind>.js, <kind>.component.wasm,
  <kind>.spec.brenn, manifest.json, …}`;
- the grant-word set and the import→capability mapping both hosts reconcile
  (`capability_for_import`, `SURFACE_IMPORTS`);
- the **packaged-module subset of the DSL** — `component` classes, `assembly`,
  `const`, `use @` — because the host's compiler reads a bundle's
  `modules/<name>.brenn` at boot.

A new class attribute arrives with a default, a new grant word is a new WIT
interface, a new record field is a `v` bump, and no existing vocabulary in that
subset changes meaning. Breaking one of these is a deliberate event, and the
design that makes the cut says so — but it is not a coordinated one, because
bundles are authored on their own schedules and a host that cannot start until
every third party has shipped is not deployable.

So a runtime-record bump is **withheld, not fatal, under a bundle mount**. A
surface kind whose record names a version this host does not read is not served:
it is left out of the served kind set, alerted once, named as `withheld` in the
page manifest of every surface configuring it (whose instances go `failed` with
the reason), described as withheld in its help and schema documents, and 404ed
under `/surface-static` with no fail2ban signal. The reload that follows the
bundle's re-release picks it up with no restart. The same record mismatch under
**brenn's own mount** is still a boot refusal: the surface tree, the kernel and
the binary travel in one tarball and are installed as a sync, so a mismatch
there is a broken install and not a version skew anybody can converge.

An additive change in force now: **`brenn:processor/calls`**, the interface a
component reaches a peer through. It is a new interface and a new grant word
(`calls`), so nothing existing moves and a bundle built before it is unaffected:
a kind that does not name `calls` in `requires` does not import it, and its
artifact, its record and its spec are what they were. A kind that *does* is
hosted on either placement — the interface is legal on the backend and on a
surface — and needs a brenn new enough to link it, which is the ordering rule
every bundle already follows: release brenn before a bundle that needs a newer
brenn.

A second additive change in force: **`brenn:processor/provides`**, the interface
a component serves registry tools through. It is a new interface and a new
grant word (`provides`), and its requests arrive on a new reserved input port,
`tool-requests`. A kind that does not name `provides` in `requires` does not
import it, and its artifact, its record and its spec are what they were;
`examples/component/` (which requires `[ports, log]`) builds unchanged against
the grown world. A kind that does name it is backend-only — a surface host
links no tool substrate — and needs a brenn new enough to link it, by the same
ordering rule as `calls`. The tools a kind serves are declared in its spec by
a new class statement, `provide <tool> { … }` (`docs/config-dsl.md`, "Provided
tools"), part of the packaged-DSL subset; a class writes none by default, and a
kind with no `provide` statement is unaffected. The request body (`{"v": 1, "id", "tool", "args"}`)
and the tool-result body grow by added fields, which `brenn-guest` ignores; `v`
moves only for a change a provider built against the older body would misread,
and `brenn-guest` refuses a version it does not know.

`provides` has since gained **`pending(id)`**, a function beside `complete`
and `call-async-for`: whether `id` is pending for the component, true exactly
where `complete` would not answer `not-pending`. It reads nothing else,
changes nothing and draws on no per-activation count, and it tells a provider
nothing `complete` did not: a refused completion already answers `not-pending`
or `invalid-outcome` by whether the id is pending. A component imports only
the functions it calls, so an artifact built before it links as it did; one
that calls it needs a brenn new enough to serve it, by the same ordering rule
as `calls`. A surface links no `provides`, so a kind that imports it is
refused there by name. `brenn-guest` reaches it through `provides::pending`.
The provider kit asks it before it reads a result's in-flight row, before it
issues or queues a call and before it decodes a backlog entry: for a request
the host no longer holds it builds no body, attempts no call, keeps no row and
spends neither of its budgets, calls no `advance`, and deletes such a request's
in-flight row and backlog entry unread. A kit component built from a brenn that
has `pending` imports it.

A third additive change in force: **`mqtt-publish-expiring`**, a new function
in the existing `brenn:processor/mqtt` interface. It is `mqtt-publish` plus the
MQTT 5 message-expiry interval, under the same `mqtt` grant, budgets and
errors; a zero interval is `invalid-payload`. Unlike `calls` and `provides` it
is not a new interface, and it took the route `ports.publish-with-urgency`
took: a new function beside an unchanged one, with no package version bump and
no new grant word. A component imports only the functions it calls, so a
bundle built before it links as it did, and one that calls `mqtt-publish` alone
is unaffected. A kind that calls the new function needs a brenn new enough to
link it, by the same ordering rule as `calls`. No shim exists in either
direction. `brenn-guest` reaches it through `mqtt::try_publish_expiring` and
`mqtt::publish_text_expiring`.

A fourth additive change in force: **`brenn:processor/blobs@0.3.0`**, the
interface a component reads, creates and attaches blobs through, in
`brenn-wasm/wit/processor-0.3.wit`. It is a new interface and a new grant word
(`blobs`). `0.3.0` is the interface's version, not a third component
contract: a component that imports it is a 0.2.0 component, told apart by
`bus@0.2.0`, and names the world `brenn:processor/processor@0.3.0`, which is
the 0.2.0 world with `blobs` imported. A kind that does not name `blobs` in
`requires` does not import it, and its artifact, its record and its spec are
what they were. A kind that does needs a brenn new enough to link it, by the
same ordering rule as `calls`. The backend serves the whole interface. A
surface serves everything but the tool-call forms and `open-request` —
`limits`, `open`, a blob's `size`, `content-type` and `read`, `create`, the
`writer` resource and `publish` — and a kind that imports `open-request`,
`call-async`, `call-async-for` or `complete` is refused when its artifact is
checked at boot, naming the function. A kind placed on a surface with the `blobs` grant reads, makes and
forwards blobs there. On a page `open` succeeds for any attachment of a message
in the activation's windows, and `read` answers `unavailable` when the page
does not hold that attachment's bytes (`docs/message-bus.md` §2.1.2a, "A
surface page is a reader by place"); the backend never answers `unavailable`.
A page holds the blobs it makes in page memory and uploads them only after the
activation returns `ok`, so the backend store's verdict on one arrives after
the call: a refused upload, or a forwarded attachment whose message left the
server's window, loses the activation's wire batch whole and the page announces
it; the message is never published without its attachments. On a surface
`budget-exhausted` is the page's own budget (`docs/message-bus.md` §2.1.2a, "A
surface page makes blobs").
Publishing with attachments goes through `blobs.publish`, and a class that
calls it requires `ports` as well as `blobs`, whether or not it also calls
`ports.publish`: the function's error type wraps `ports@0.2.0`'s
`publish-error`, so the artifact imports `brenn:processor/ports@0.2.0` for the
type, which is what the build check and the load check read. `ports.publish`
still carries none. The interface is **declared stable**, with the promise
`brenn:processor@0.2.0` carries, in the terms its WIT header states: no type or
function of `blobs@0.3.0` is changed or removed, no case is added to a variant
or an enum and no field to a record, and each keeps doing what the file says.
While brenn serves the interface, an artifact built against the shapes in the
file loads and means what it meant. brenn does not stop serving it without a
removal period announced in `CHANGELOG.md` and in the file no later than the
first release the period counts; no length is promised. A breaking change is a
new version served beside this one through that period. An addition is a
function no existing component imports, added beside the functions the
interface has, which keep their shapes and behaviour; a host coming to serve a
function it refuses today is an addition of the same kind. Bounds stay host
policy and outside the promise. The promise is to an artifact built against the
shapes in the file, not at any commit that carried it, because one shape
changed in place before the declaration: `read-error` gained `unavailable`,
last. A component built against the earlier shape that calls `blob.read` no
longer links and is rebuilt against the current `brenn-guest`; one that matches
`brenn_guest::blobs::ReadError` exhaustively gains an arm for
`ReadError::Unavailable`.

An addition to `blobs@0.3.0` in force: **`open-request`**, a function that
opens an attachment of a request pending for the calling component by the
request's pending id and the attachment's name, with an error variant of its
own, `open-request-error`. It was added under the interface's rule above, as
`mqtt-publish-expiring` was to `mqtt`: no package version bump and no new
grant word, and no existing type or function changed. A component that does
not call it imports nothing new, so a bundle built before it links as it did.
One that calls it needs a brenn that serves it, by the same ordering rule as
`calls`. `brenn-guest` reaches it through `provides::open_request_attachment`.

**Blobs and tool calls.** An asynchronous tool call carries named
attachments by reference in both directions. Three functions of `blobs` are
the forms that do, each taking a list of `(name, blob)` pairs as
`blobs.publish` does — a handle from `open` or `writer.seal`, never an id:

- `blobs.call-async` is `tools.call-async` carrying blobs to the tool;
- `blobs.call-async-for` is `provides.call-async-for`, a provider's nested
  call for a request it serves, carrying blobs;
- `blobs.complete` is `provides.complete`, answering with blobs beside the
  outcome. An `err` outcome carries none and is refused `invalid-outcome`.

The plain forms — `tools.call-async`, `provides.call-async-for`,
`provides.complete` — carry none, and neither do `calls.call` and
`tools.call-fast`, which return inside the activation and put no message on
the bus. An empty list behaves as the plain form.

*The grants.* Each function needs the grant its plain form is linked under:
`call-async` needs `tools`, `call-async-for` and `complete` need `provides`,
and every one needs `blobs`. Each error type wraps the plain form's —
`call-error` holds `tools.tool-error` as `plain(…)`, and `complete-error`
holds `provides.complete-error` — so an artifact that calls `call-async` or
`call-async-for` imports `brenn:processor/tools@0.1.0` for the type, and one
that calls `complete` imports `brenn:processor/provides@0.1.0`, whether or not
it calls a plain form. That import is what the build check and the load check
read, so:

- a class that calls a tool with attachments requires `tools` and `blobs`;
- a provider that completes with attachments requires `provides` and `blobs`;
- a provider that makes nested calls, plain or with attachments, requires
  `tools` as well, because a nested call is admitted by the provider's own
  tool grants and its result arrives on `tool-results`;
- a class that publishes with attachments requires `ports` and `blobs`;
- a provider that opens its requests' attachments by pending id
  (`open-request`) requires `provides` and `blobs`. Its error type wraps
  nothing, so the artifact imports no interface but `blobs` for it, and the
  host checks `provides` at the call: without it every call is
  `not-permitted`.

A component that calls none of the three imports none of them, and nothing
through them: an artifact already built is unaffected by their addition.

*What arrives.* A request on `tool-requests` and a result on `tool-results`
carry the attachments in the message's header like any message, under the
names the caller or the tool gave; their bodies are unchanged. `blobs.open`
reaches them by `(port, index, name)`, as on any input port. A result carries
attachments only when its outcome is `ok`. `brenn-guest`'s
`tools::ToolResult` and `provides::ToolRequest` list them in `attachments` and
open one with `open(name)`. `ToolRequest::open` opens by the message's place,
through `blobs.open`, and only in the activation that delivers the request.
`provides::open_request_attachment(id, name)` opens through `blobs.open-request`
by the request's pending id, in any activation while the request is pending,
wherever its message sits or no longer sits in the `tool-requests` window. It
answers `NotPermitted` without the `provides` grant, `NotPending` when `id` is
not pending for the component — answered, failed, expired, abandoned by its
caller, from before a restart, or another component's, told apart from
nothing — `NotAttached` when the request carries no attachment of that name,
and `QuotaExceeded` past the activation's blob-call or handle bound.

*A host tool that takes and answers attachments.* `api-request` is one: its
`body_attachment` argument names the call's attachment to send as the request
body, and its `response_attachment` argument asks for a 2xx answer's body as
an attachment of the result under that name (`docs/config-dsl.md`, "The OAuth
tools"). A component sends a file by `blobs.call-async` with the attachment
named in `body_attachment`, and reads an answer by opening the result's
attachment. A component whose instance is not granted `blobs` cannot open such
an answer, and `api-request` refuses its `response_attachment` `denied`.

*Who holds a blob.* A call is buffered and flushed as the plain form is: the
request reaches the bus iff the activation returns `ok`, and the buffered
request holds its blobs until then; an `Err` or a trap drops them. On the bus,
the request row holds them, whatever becomes of the call, until the bus GC pass
that follows the row leaving its channel's window. A provided tool's request is
also held by its pending record, from before the request is published until
the call ends — completed, failed, past its deadline or its caller gone: one
count on each distinct blob the request names, charging nothing, for at most
Σ `max_concurrency` records an instance at once, each for at most the 240
seconds a provided call is given. The record's hold outlasts the row only when
a GC pass evicts the row of a request still pending, and the blob then goes at
the first reap after the call ends, so the provider's side keeps alive at
most the attachments of that many requests for at most one GC interval past
each one's end. The record is in memory: after a restart none exists, and the
rows hold what they held. A completion is immediate, as `provides.complete` is, and it is the one
exception to "nothing a component holds outlives the activation": the
completion holds the blobs it names from the call, so a blob the activation
made and completed with survives the activation's later `Err` or trap, as the
completion does. A completion whose waiter is gone drops them. A handle given
to any of the three is cloned, not consumed, and the component may drop its
handle after the call. No byte is copied anywhere on the path.

*Which tool takes attachments.* A tool declares it: `accepts_attachments =
true` on a `provide` statement (`docs/config-dsl.md`, "Provided tools"),
default `false`. A call that carries attachments to a tool that does not is
refused `attachments-not-accepted`, told only to a caller granted the tool. A
tool that returns attachments needs no declaration.

The shared bounds are the plain forms': the per-activation call cap, the
publish-entry ceiling, the `call-id`, `args-json` and outcome-text caps, which
weigh the JSON alone. At most 32 attachments per call or completion, each name
in the attachment grammar, no name twice; each is a typed refusal. These
three do not count against the per-activation blob-call or handle bounds.

**Blobs and mutability.** A sealed blob is immutable, and the interface does
not entrench it: an in-place mutable blob would add a mutability flag at
`create`, a write path on a handle to a sealed mutable blob, and a way for a
reader to learn which kind it holds, so a blob without the flag keeps its
guarantee. All three are additions. Three choices lean on immutability and
none forecloses mutation: `blob` has `read` and no `write`, so a handle to a
sealed blob is proof nothing changes under its reader — mutation adds a write
path, on `blob` or a third resource; `size` and `content-type` are fixed on a
handle, so no call fails and a guest sizes its reads once — mutation needs a
fixed extent or a size "at open"; and forwarding shares the blob rather than
copying it, which is zero copy — mutation adds nothing there, but every holder
then sees every write, a concurrency rule this interface does not yet state.

**The byte-carrying contract.** The backend host serves two
`brenn:processor` contracts side by side. Under the string contract, `brenn:processor@0.1.0` (`brenn-wasm/wit/processor.wit`),
a component is handed each message as `envelope-json` text and publishes
`payload: string`. Under the byte-carrying contract, `brenn:processor@0.2.0`
(`brenn-wasm/wit/processor-0.2.wit`), it is handed each message as a header
and a body of bytes with an optional content type, and publishes, parks,
edits, calls and replies with such a body. No base64 and no JSON string sits
around a body on that path.

The 0.2.0 package declares only the three interfaces whose shape differs:
`bus` (the body, the envelope and the activation), `ports` and `calls`. Every
other interface keeps its `@0.1.0` name under both contracts. An artifact's
imports are therefore:

| | 0.1.0 | 0.2.0 |
|---|---|---|
| carries the activation | `brenn:processor/types@0.1.0` | `brenn:processor/bus@0.2.0` (and `types@0.1.0`, only if it uses `node`) |
| `ports`, `calls` grants | `ports@0.1.0`, `calls@0.1.0` | `ports@0.2.0`, `calls@0.2.0` |
| every other grant | its `@0.1.0` interface | the same `@0.1.0` interface |

A grant word means the same capability under either contract. A reader tells
an artifact's contract from its imports (`wasm-tools component wit
<artifact>`): one importing `brenn:processor/bus@0.2.0` is 0.2.0, any other is
0.1.0. Nothing else records it: not the package record, not the spec, whose
`abi` stays `processor`. The loader and the build-time parity gate accept
exactly the detected contract's import set; an artifact mixing the two, or
naming an interface at a version this brenn does not serve, is refused at load
by name.

**`brenn:processor@0.2.0` is stable.** The interfaces `bus@0.2.0`,
`ports@0.2.0` and `calls@0.2.0` and the `receive` export of the 0.2.0 world
are frozen whole: no type or function in them is changed, removed or added,
and each keeps doing what `processor-0.2.wit` says it does. While brenn serves
`brenn:processor@0.2.0`, an artifact built against it at any brenn commit that
carried it loads and means what it meant. brenn does not stop serving it
without a removal period, announced in `CHANGELOG.md` and in the WIT header
no later than the first release the period counts, as 0.1.0's is. A breaking
change is a new version of the interfaces that change, served beside 0.2.0
through that period. No such period has begun for 0.2.0, and no length is
promised for one.

The contract grows without a new version in three ways. A new capability is a
new interface under a new grant word, with its own version; a component that
does not import it is linked, activated and answered exactly as before. Such an
interface may act on the ports a component already has — publish, park, read
or edit a parked message, call a peer, shape the reply to a sync call — and
may carry what the 0.2.0 functions cannot. The 0.2.0 functions keep their
shapes and their behaviour beside it: what one of them does with a message's
body does not change, and what it does with any part of a message it cannot
express is stated by the interface that introduces that part. And
`bus.envelope.header` is an open JSON object: it gains keys, the keys it has
keep their meaning, and a guest ignores a key it does not know. And a function
may be added to an `@0.1.0` interface of `processor.wit` that this promise does
not freeze, beside the functions it has, which keep their shapes and their
behaviour, as `mqtt-publish-expiring` was; a component that does not call it
imports nothing new.

How the host adds a header key, so the next key's author starts from it: the
key is a field of `MessageEnvelope` in `brenn-envelope`, `Option` and absent
when unset. `ShapedEnvelope::serialize` writes it with `optional_field` — in
the shared part, which every shape reads, or in `HeaderShape`'s header-only
position — so an envelope without it serializes byte-identically to one built
before the key existed. `EnvelopeHeader` and the text-shape reader
`TextShapeIn` hold it, and `MessageEnvelope::header()` and `from_header()`
destructure without `..`, so the compiler refuses the change until both carry
it. A durable channel stores it in one nullable column of `messaging_messages`
that joins every SELECT and decoder rebuilding an envelope; a ring holds the
whole envelope and needs nothing. Attach v5 carries the header as an open
object too, so the key crosses it with no protocol version bump. `transport`
took this route through the shared part.

A key not every shape carries takes the second form: it is not written in the
shared part but by a method of the shape trait, `EnvelopeShape`, which each
shape implements, so each shape states what it writes. `attachments` took this
route. It is a map, empty rather than `Option` when unset, and each writer
skips it when empty, so an envelope without attachments is still
byte-identical in every shape. `HeaderShape` writes each attachment's full
reference (`{"photo": {"id": …, "size": N, "content_type": "…"}}`), the
conversation's shape writes `size` and `content_type` without the id, and the
text shape — 0.1.0's `envelope-json` and attach v4 — writes nothing, and its
reader does not hold the field. A 0.2.0 component sees the key in
`bus.envelope.header` under the growth rule above. The id there redeems
nothing: a component granted `blobs` opens an attachment by its place in the
activation — port, index and name — and forwards it by passing the handle to
`blobs.publish`. A component's `ports.publish` carries no attachments, so
republishing a message through it drops them (`docs/message-bus.md`
§2.1.2a).

The limits a host applies — body caps, budgets, depths — are host and operator
policy, not part of this promise; the refusal a limit draws keeps its variant
and its meaning.

**The string contract's removal period.** The string contract
`brenn:processor@0.1.0` is being removed: the string contract is served by the
release that declares `brenn:processor@0.2.0` stable and by the three brenn
releases after it, and no release before the fourth after it removes it. The
declaring release is the one whose `CHANGELOG.md` heading holds the entry
"`brenn:processor@0.2.0` is stable"; a release is a `v*` tag with its own
`CHANGELOG.md` heading. Removal also does not happen before the three known
out-of-tree component repositories have moved to 0.2.0: `brenn-fastmail`,
`brenn-component-demo`, and a third, private one. What is removed
is the string contract and nothing else: its world, the activation records of
`types`, `ports@0.1.0` and `calls@0.1.0`. Every other interface in
`processor.wit` stays as it is — it is what a 0.2.0 component imports. After
removal a host refuses a string-contract artifact at load. The period can be
lengthened; once the declaring release is tagged, it is not shortened.

The shapes do not change. An artifact that does not have them — bound by hand
against some other text, for instance — is refused at load, by name or by
type, never misread: the loader holds every import to the contract's names,
and wasmtime type-checks the `ports` and `calls` imports, which carry every
`bus` type, and the `receive` export when the component is linked — on the
backend with every other import, on a surface those three alone (*On a
surface*, below). A shape whose meaning changed without a change of name or
type would pass both, so a shape whose meaning changes changes its name or its
version with its meaning; that is why the promise covers behaviour and not
only types. The promise is to an artifact that has the shapes the WIT files
state for every interface it imports, the `@0.1.0` interfaces of
`processor.wit` included. A host that accepts an artifact which does not have
them — a surface host today, for an import outside `ports` and `calls` — may
refuse it in a later release; that is a correction to the host, announced in
`CHANGELOG.md`, not a breaking change to the contract, and it draws no dual
period.

What a 0.2.0 author relies on:

- **The header is open.** `bus.envelope.header` is the canonical envelope as
  JSON without its body. Keys are added over time, without a new contract
  version; the keys it has keep their meaning, and a guest ignores a key it
  does not know. A new interface may give a message parts that travel as
  header keys. The first added key is `transport`: present only on a message
  a transport adaptor published in the raw ingress shape
  (`docs/message-bus.md` §2.7.1), an object with one member named for the
  transport — `{"mqtt": {"client_slug", "topic", "qos", "received_at"}}` or
  `{"webhook": {"headers", "key_id", "client_ip", "received_at",
  "endpoint_slug"}}`. Its presence is how a reader tells a raw ingress message,
  whose body is the payload, from a wrapped one, whose body is the wrapper
  JSON.
- **A body is validated where it enters the host.** A content type must be a
  well-formed media type; a text body (no content type, `text/*`,
  `application/json` or a `+json` subtype, with no charset other than UTF-8
  or US-ASCII) must be UTF-8. The size caps count raw bytes. The content type
  is the sender's assertion and is never checked against the bytes.
- **The refusals it can draw that a 0.1.0 component cannot:** `invalid-body`,
  for a malformed content type or text that is not UTF-8, on a publish, an
  edit or a call; `text-only-channel`, for a non-text body published, parked or
  edited onto a channel not declared `non_text`; `text-only-peer`, for a
  non-text body sent to a peer built against the string contract, which does
  not run. A reply that is malformed or over the cap traps the replying
  component, as an over-cap reply always has.
- **A mixed sync pair.** A reply is judged against the replying component's
  own contract only. A 0.2.0 component may reply to any caller with a
  non-text body; its activation ends ok and its buffer flushes. A 0.1.0
  caller cannot read that reply, so it is the *caller's* activation that
  traps, naming its `call` port and the peer. A typed text reply reaches a
  0.1.0 caller as its text, as a typed text message does on a channel.
- **On a surface.** The surface host serves 0.2.0 beside 0.1.0; the page
  manifest names each kind's contract and the page brings each instance up on
  its own contract's imports. What a
  page-hosted 0.2.0 kind carries today, shown on the node rig;
  `e2e/tests/bytes.spec.ts` runs the publish, the read-back and the delivery
  in Chromium, and an SDK-built kind's binary state on a page-local `io`
  port:
  - any body, text or not, with its type, on a page-local channel declared
    `non_text` — a surface binding tail states `non_text = true` where its
    channel stays on the page (`docs/message-bus.md` §2.6) — published,
    parked, edited and read back byte for byte;
  - any body, text or not, with its type, on a port bound to a channel that
    leaves the page and is declared `non_text`, over the page's wire (attach
    v5): published, parked, edited and delivered byte for byte;
  - typed text on any channel, page-local or not, with its type; a non-text
    body on a channel not declared `non_text` is `text-only-channel`, and
    nothing is buffered or sent;
  - any body, text or not, typed or not, in a sync call and its reply between
    two 0.2.0 kinds.

  The server holds a 0.2.0 kind's artifact to the shapes it serves before
  serving it, because the page's transpiled glue checks nothing against the
  host: the server holds its import names to the ones a surface host
  serves, and wasmtime type-checks its `ports` and `calls` imports and its
  `receive` export. An artifact that does not have the
  contract's shape is **withheld** under a bundle mount, by the rule above
  for a record it does not read, with a reason naming the kind, the contract
  and the check's account; under brenn's own mount it is a boot refusal. An
  artifact the server cannot read is a broken install and fatal under any
  mount.
- **The guest SDK builds 0.2.0.** A component built on `brenn-guest` is a
  0.2.0 artifact, and every in-tree SDK component is one. A component that
  stays on 0.1.0 does so by keeping an older brenn pin or by writing raw
  bindings over `processor.wit` alone; a raw 0.2.0 component reads both WIT
  files, `processor.wit` first, with the world
  `brenn:processor/processor@0.2.0`.

A backend that loads any 0.1.0 artifact, and one whose surfaces instantiate
any 0.1.0 kind, says so in one `warn!` each at boot and after each applied
reload, naming them, stating the removal period and telling the operator to
rebuild them against 0.2.0.

One runtime contract is a **candidate for deletion**, recorded here so that a
bundle author meets it before it happens rather than after: `brenn:replay` is
the one world native backend code calls synchronously per request, and the
backend host now mints sync-call activations for any processor with a declared
`sync` port. A replay component could therefore be an ordinary processor that
answers a `sync check;` port, and the world would go. That is an external ABI
cut — out-of-tree replay components are first-class consumers of it — so it is
its own coordinated event with its own design, and nothing has been decided.

**Build-time contracts are hard-cut, at the pin.** These are consumed against a
commit the consumer names in its own `git_override`: the macros in
`bazel/wasm/defs.bzl` and `bazel/surface/dist.bzl`, `brenn-guest`'s Rust API,
`brenn-provider-kit`'s Rust API, and `brenn-provider-kit-guest`'s, the
scaffold's generated module shape, `surface/page-harness`, `dsl_cli`'s
subcommands, and the guest alias sets. A cut costs the consumer a migration
when it next bumps that pin, and nothing before, because nothing that is
already deployed reads them. `examples/component/` is the canary that a cut is
complete on brenn's side; a consumer repository's CI at its next pin bump is
where the cut is paid.

A cut of this kind in force now: **`brenn_guest::Error` gained `NotPending`**,
the refusal a provider draws completing a request the host no longer holds. A
kind that matches `Error` exhaustively adds the arm at its next pin bump; one
that only propagates it with `?` is unaffected.

Another cut of this kind in force now: **a `dom.listen` port must be declared `sync`
in the specification, and is reached through the generated `spec::SyncPort`.**
A `dom`-granted kind that calls `listen` adds a `sync <name>;` line per port it
listens on and passes the variant the scaffold emits for it; there is no
hand-spelled port newtype to pass instead, so a port the specification does not
declare has no value to name it. The specification and the artifact travel
together and are hash-bound, so this is a rebuild per kind at the next pin bump,
never a deployment skew. A `dom`-granted kind that never calls `listen` is
untouched.

Another cut of this kind in force now, in the provider kit: **the activation
loop moved into `brenn-provider-kit`, served through
`brenn-provider-kit-guest::serve`.** A provider that ran its own loop over the
kit's rows implements `brenn_provider_kit::Logic` (its `plan` and `advance`)
and calls `serve` from `receive`. `read_api_result`, `Unreadable::render` and
`Unreadable::failure` take the API's display name (and `render` the caller's
size note, where it took a Gmail size estimate); `google_error_detail` and the
`GOOGLE_*` bounds moved to `brenn_provider_kit::google` as `error_detail`,
`MESSAGE_MAX_BYTES` and `STATUS_MAX_BYTES`, beside `API_NAME`. The `InFlight`
row gained `received_at_ms`, so a row written by a build before the cut does
not parse after it. A row of either kind that a later cut leaves unparsable
expires on its age alone: an in-flight row at `INFLIGHT_TTL_MS` after issue, a
backlog entry `BACKLOG_MAX_WAIT_MS` after its request arrived, answered
`internal` then; until then each activation that reads it fails.

Another cut of this kind in force now, in the provider kit: **a nested call's
body is a `Body`, and an answer can be bytes.** `ApiCall.body` is an
`Option<Body>`: `Body::Text(String)` goes on the wire as `body`, exactly as the
`String` did, and `Body::Bytes(Vec<u8>)` as `body_base64` (standard padded
base64, as `api-request` reads it); a call cannot carry both, and a wire form
that does is refused when it is parsed back. A component that set a text body
wraps it in `Body::Text`. `read_api_result` reads a 2xx `body_base64` answer
as `Answer::Bytes { status, body }`, decoded, where it used to fail
`internal`; a `body_base64` under any other status, or one that is not base64,
still fails `internal`. `api-request` answers in `body_base64` only when the
API's bytes are not valid UTF-8, whatever their content type: a plain-text,
CSV or calendar file, or any ASCII payload, arrives as `body` and fails the
JSON read as "a body that is not JSON"; `read_api_payload` reads a payload's
bytes whichever field carried them. A component that matches `Answer` exhaustively adds an arm for `Bytes`
— failing `internal`, if the API it calls never answers in bytes. The `testutil` fixtures gained `binary`, an answer carrying bytes, and
`text_body`, a call's text body.

Another cut of this kind in force now, in the provider kit: **a step carries
attachments.** `Logic::plan` takes a `&Step` and `advance` a `&mut Step`
(*A tool provider on the kit*, below). A hand-written `Host` implements
`Blobs` as a supertrait, and its `call` takes the nested call's body blob
(`Option<(&str, &Blob)>`) and `complete` the completion's attachments
(`&[(String, Blob)]`). `Request`, `Delivered`, `ApiCall`
(`response_attachment`), `InFlight` and `Queued` (`attachments`) gain a field,
so a struct literal of any of them breaks; an `ApiCall` literal adds
`response_attachment: None`. `Body` gains `Attachment(String)`, so an
exhaustive match adds an arm. `MemoryHost`'s `NestedCall` gains `body` and
`Completion` gains `attachments`. Rows are unchanged on the wire for a request
without attachments, and a call that uses neither new form serializes as
before, so a row written before the cut parses after it. A component that uses
no attachment changes signatures and literals and nothing else, keeps calling
`serve`, and keeps its grants. `brenn-guest` gained
`provides::open_request_attachment` (an addition; no WIT changed).

Another cut of this kind in force now: **a request's attachment opens by
pending id.** In `brenn-guest`, `provides::open_request_attachment(activation,
id, name)` is `open_request_attachment(id, name)`: it takes no activation and
calls `blobs.open-request`, so a component that calls it imports that function
and needs a brenn that serves it. `provides::RequestAttachmentError`'s cases
are the interface's — `NotPermitted`, `NotPending`, `NotAttached`,
`QuotaExceeded` — so `RequestAttachmentError::NotInWindow`,
`RequestAttachmentError::Open` and `RequestAttachmentError::Malformed` are
gone. In `brenn-provider-kit`, `OpenError::LeftWindow` is gone and
`OpenError::NotPending` added, so an exhaustive match changes an arm, and a
step that needs an ended request's attachment fails with "the request is no
longer pending; its attachment `<name>` was not sent" where it named the
window, a completion the host drops.
`MemoryHost::request_attachments_left_window` is
`MemoryHost::requests_not_pending`, under which a request's attachment
answers `NotPending` and the kit's failure is recorded as its completion.
`brenn_wasm::ProvidesHost` has a new required method, `request_attachment`,
which a harness that delivers requests with attachments answers from them
while the request is pending. A component served by
`brenn-provider-kit-guest`'s `serve_with_blobs` imports `open-request`, whether
or not its tools take attachments, so it needs a brenn that serves it.

Another cut of this kind in force now: **`MessageEnvelope.body` is a
`MessageBody`**, bytes with an optional content type, re-exported from
`brenn-guest` with `BodyError` and `is_text_content_type`.

- Under the 0.1.0 contract the body is always text: the host never windows a
  non-text body to a 0.1.0 component, and a typed text body arrives as exactly
  its text, without its type. This is enforced at binding: a 0.1.0 component
  with any input or output port on a channel declared `non_text` is refused
  at boot and at reload — a consumer, and a served surface instance bound to
  any channel declared `non_text`, page-local or not. A kind reads it with `body.as_text()` or
  `envelope.text_body()?`, and compares a sentinel as
  `body.as_text() == Some("…")`.
- `brenn_guest::MessageEnvelopeExt` gained `text_body(&self) -> Result<&str,
  Error>`, which answers `Error::MalformedEnvelope` for a body that is not
  text. `json_body` and `webhook_body` keep their signatures and read through
  it.
- `MessageEnvelope` no longer implements `Serialize`. A kind that built
  `envelope-json` text, in a test for instance, calls `to_envelope_json`.
  `Deserialize` reads the text shape and refuses a `content_type` or
  `body_base64` key.
- `RetainedState` was unchanged in signature and behaviour by this cut.

The WIT, the `envelope-json` text and every runtime contract are byte-identical
for the bodies a deployed component can meet, so no deployed artifact is
affected; the cut is paid at the next pin bump.

Another cut of this kind in force now: **`brenn-guest` binds
`brenn:processor@0.2.0`.** The SDK generates the byte-carrying world and no
other, so every component built on it from this pin on is a 0.2.0 artifact.
The one edit every component makes is `Processor::receive`'s reply type; a
component that only publishes and reads text changes little else.

| Before | After |
|---|---|
| `fn receive(activation: Activation) -> Result<Option<String>, Error>` | `fn receive(activation: Activation) -> Result<Option<Body>, Error>`; a text reply is `Ok(Some(text.into()))` |
| `PortWindow::new_raw() -> &[String]`, `context_raw() -> &[String]` (envelope JSON) | `new_messages() -> &[Message]`, `context_messages() -> &[Message]`; there is no string form of an envelope |
| — | `Message`: `header_json() -> &str` (the open JSON header), `body() -> &Body`, `envelope() -> Result<MessageEnvelope, Error>` |
| `new_envelopes()`, `context_envelopes()` | unchanged in signature, read through `Message::envelope` |
| `DeferredEntry::payload() -> &str` | `DeferredEntry::body() -> &Body`; its text is `entry.body().as_text()?` |
| `publish(port, &str)`, `publish_with_urgency`, `publish_deferred`, `repark` | the same names, the body `impl Into<BodyRef<'_>>`; a `&str` or `&String` is untyped text, so text call sites compile unchanged |
| `defer_edit(port, index, Option<&str>, deliver_after)` | `defer_edit(port, index, Option<BodyRef<'_>>, deliver_after)` |
| `calls::CallError`, the generated WIT variant | an SDK enum: `NotPermitted`, `Unwired`, `Refused`, `Failed`, `InvalidPayload(String)`, `InvalidBody(String)`, `TextOnlyPeer`, `QuotaExceeded` |
| `CallPort::try_call(&str) -> Result<Option<String>, CallError>`, `call` likewise | the request `impl Into<BodyRef<'_>>`, the reply `Option<Body>`; `call_json` requires a text reply and answers `MalformedEnvelope` for any other |
| `dom::cancel_reply()`, `proceed_reply() -> String` | `-> Body`, the same JSON as untyped text |
| `RetainedState<T>` | `RetainedState<T, C = Json>`; `RetainedState<T>`, `new` and `around` compile unchanged |
| `RetainedState::load(&mut self, &Activation)` | `load(&mut self, &mut Activation)`, taking the state window out of the activation |
| `build_activation` (public) | gone; the export shim builds the activation |
| `Error` | gains `TextOnlyChannel(String)` and `InvalidBody(String)` |
| — | `MessageEnvelopeExt::mqtt_body() -> Result<MqttEnvelope, Error>`, the wrapped MQTT shape, beside `webhook_body` |
| — | `MessageEnvelopeExt::mqtt_transport() -> Result<&MqttTransport, Error>`, `webhook_transport() -> Result<&WebhookTransport, Error>`: a raw ingress message's `transport` |
| — | `MqttPayloadBody::text() -> Option<&str>`: the wrapper's text payload, `None` for the binary placeholder |

- **Bodies.** `Body` is a body as it crossed the boundary: `content_type()`,
  `as_bytes()`, `into_bytes()`, `is_text()`, `as_text()` (`MalformedEnvelope`
  for a non-text type or bytes that are not UTF-8), `json::<T>()`; built by
  `Body::text(..)` or `Body::typed(content_type, bytes)`, or from a `String`
  or `&str` as untyped text. `BodyRef` is a borrowed body to hand the host:
  `BodyRef::text(&str)`, `BodyRef::typed(content_type, &[u8])`, or from a
  `&str`, `&String` or `&Body`. Bytes go out as
  `publish(port, BodyRef::typed("application/octet-stream", &bytes))`, typed
  text as `BodyRef::typed("text/markdown", s.as_bytes())`.
  `MessageEnvelopeExt::text_body` answers `MalformedEnvelope` for a non-text
  body, which a running guest can now be handed.
- **The JSON conveniences state no content type.** `publish_json`,
  `OutPort::publish`, `CallPort::call_json`'s request, `provides::complete`,
  `tools::call_async_json` and a JSON `RetainedState` publish untyped text,
  byte for byte what they published before. An author who wants
  `application/json` stated publishes `BodyRef::typed`. `mqtt::publish_json`
  keeps MQTT's own `application/json`.
- **Two new refusals.** `Error::TextOnlyChannel` is a publish, park or edit of
  a non-text body onto a channel not declared `non_text`; its message names
  the port, the body's content type and length, and the fix ("declare
  `non_text = true` on the channel, or on the binding where the channel stays
  on the page"). It is structural: no later activation repairs it.
  `Error::InvalidBody` is a malformed content type, or a text-typed body that
  is not UTF-8. Both fail the activation as `processing-failed`. A call keeps
  its own `CallError::TextOnlyPeer` (a non-text request to a peer built on
  0.1.0, which does not run) and `CallError::InvalidBody`. A kind that matches
  `Error` or `CallError` exhaustively adds the arms.
- **Ingress shapes.** `brenn-guest` re-exports `MqttEnvelope`,
  `MqttPayloadBody`, `TransportMeta`, `MqttTransport` and `WebhookTransport`
  beside `WebhookEnvelope`. Each of the four ingress accessors checks the
  message's scheme first, then its shape: `mqtt_body` and `webhook_body`
  answer `Error::ProcessingFailed` for a message carrying `transport`, naming the
  transport accessor, and `mqtt_transport` and `webhook_transport` answer it
  for one without, naming the wrapper accessor. A raw message's body is read
  as any body is, through `Message::body()`.
- **Binary state.** `RetainedState<T, C>` takes a `StateCodec<T>`: a
  `CONTENT_TYPE` (`None` is untyped text), `encode(&T) -> Result<Vec<u8>,
  Error>` and `decode(&[u8]) -> Result<T, Error>`. `Json` is the default and
  publishes what `RetainedState` always did. A byte codec is the author's own
  `impl StateCodec<MyState> for MyCodec` with a non-text `CONTENT_TYPE`, bound
  by port name: `RetainedState::<MyState, MyCodec>::on(spec::port::STATE)` and
  `around_on(activation, spec::port::STATE, loss, body)`. A stored body whose
  content type is not exactly the codec's is unreadable state, handled by
  `StateLoss` as any version skew is. The worked example is
  `brenn-wasm/components/processor-sdk-bytes-test`.
- **A binary state needs a `non_text` channel.** On a surface the binding says
  so — `io state { push_depth = 0; retain_depth = 1; non_text = true; }` — and
  on the backend the channel does. Without it the first store fails the
  activation with `Error::TextOnlyChannel` under either `StateLoss`; nothing
  is persisted and nothing is silently lost. A state body is one message, so
  it is bounded by the channel's `max_body_bytes` (65,536 B by default,
  applied on a page to page-local publishes too) and by the 4 MiB of
  publishes one activation may make.
- **`surface/page-harness` drives 0.2.0 kinds only.** `Page::published` and
  `parked` record `Published { port, body, deliver_after }` with a harness
  `Body { content_type, bytes }`; `published_on(port)` still returns the text
  bodies and panics on a non-text one; `bodies_on(port)` returns the records.
  Activation builders return `bus` records (`bus::Activation` and
  `bus::ReceiveError` in place of `types::…`); `sync_call(port, request)`
  takes the request body text; `delivery_of(port, context, new, dropped)`
  delivers `Body` values; `seeding_state_body(Body)` sits beside
  `seeding_state(&str)`; `call_returning` and `receive` return the reply as a
  `Body`. A transcript line for an untyped text body is unchanged; any other
  body is rendered as its content type and length.

A component built at any pin from this cut on is a 0.2.0 artifact. 0.2.0 is
stable (*Contract evolution*, above), and because its shapes have not changed
since the contract first appeared, an artifact built at a pin before the
declaring release is under the promise as one built after it is. A component
stays on 0.1.0 for the rest of that contract's removal period by keeping the
older pin or by writing raw 0.1.0 bindings.

Freezing the macro vocabulary of a one-operator ecosystem buys nothing a pin
does not already buy, which is why the two are split rather than held to one
promise. `CHANGELOG.md` announces a cut to component authors in an entry
headed **BREAKING for component authors:**; additive changes for authors sit
in an entry's "For WASM component authors" sentence. The git history of the
files named above is the record of what cut when.

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
loads an instance of a class the module declares. The components roots are the
`components/` trees of the declared mounts; exactly one of them may hold the
directory (*Mounts*, below).

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
| `v` | Record schema version. This host reads `2` and refuses anything else — with a boot panic under every mount, including a bundle's. Unlike the surface record above, the backend record has no withholding policy yet; `TODO(stale-package-record-withheld)` is the entry for giving it one. |
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
   anything. A name with no directory under any mount's `components/` tree is a
   panic naming every tree searched and the instantiation. A configuration
   may import any module the module root ships, but only a component the
   release ships as a backend package is top-level loadable — a surface kind
   ships its module and no package, and this is where instantiating one lands.
   A name present under two mounts is a panic naming both mounts; so is a host
   whose declared mounts offer no `components/` tree at all.
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

### A tool provider on the kit

A component that serves `provide`d tools by nested `api-request` calls can
take its activation loop from brenn's provider kit. It lists, beside the three
guest labels above,

```
"@brenn//brenn-provider-kit:brenn-provider-kit_guest",
"@brenn//brenn-wasm/components/provider-kit-guest:brenn-provider-kit-guest",
```

and names neither in its own `Cargo.toml`. The kit's serde is the guest
instance, so its `Value`s are the ones `brenn-guest` hands over.
`brenn-provider-kit` names no guest SDK: it holds the loop (behind a `Host`
trait), the rows a request keeps, the backlog, the call and answer types, and
the argument and settings readers, and its loop issues at most `CALL_BUDGET`
nested calls per activation and builds at most `BUILD_BUDGET` bodies,
queueing the rest for later activations.
`brenn-provider-kit-guest` is the SDK glue: one `serve::<L: Logic>(&activation,
&settings)` per activation, where `L` is the component's `plan` and `advance`.

`:brenn-provider-kit_guest` carries no platform constraint, so the consumer's
host `logic_test` links it the way `profile-switcher`'s `report_test` links the
guest `serde` on the host; `:brenn-provider-kit-guest` is wasm32-only, like
`brenn-guest`. The `testutils` feature — on by default through
`@brenn//bazel/features:testutils`, cleared by
`--@brenn//bazel/features:testutils=False` — carries the schema parity checker
(`schema`), the step fixtures and the in-memory `MemoryHost` (`testutil`). The
select resolves against `@brenn` from a consumer's build, which is the
situation `@brenn//brenn-envelope` is already in; `brenn-fastmail`
(`https://github.com/rnortman/brenn-fastmail`) builds its provider against
`@brenn//brenn-provider-kit:brenn-provider-kit_guest` and runs its host
`logic_test` over `testutil`, so an out-of-tree build has proven it. A logic
test that fails to compile naming `schema` or `testutil` is that flag.
`@brenn//brenn-provider-kit:brenn-provider-kit` (against the host hub's serde)
is the crate's own test build and not the contract. The labels are the crates'
own targets, as `@brenn//brenn-envelope:brenn-envelope_wasm` is for
`profile-switcher`; the guest alias set is for third-party crates only.

A step reads a nested call's answer with one of two readers. `read_api_result`
is for an API that answers JSON: a 2xx body is parsed as JSON, or read as
`Answer::Bytes` when it is not UTF-8, or as `Answer::NoBody` when empty.
`read_api_payload` is for any other 2xx body and for the response headers —
XML, iCalendar, vCard, a download, an `ETag` — and answers a `Payload` of the
status, the headers (names lower-case, `header` matching without regard to
case) and the body's bytes, whichever of `body` and `body_base64` carried them
(`text` gives them as a string when they are UTF-8). Both give the same relay,
cut, undelivered and error handling, with the same texts, and both fail a 3xx
as `internal`: `304 Not Modified` included, so a component cannot read the
answer to a `GET` or `HEAD` sent with `If-None-Match` or `If-Modified-Since`.
A payload is bounded
by the provider's `max_response_bytes` and its ceiling and by the bus cap, and
arrives as `Unreadable` when it exceeds either. The `testutil` fixture
`payload` fakes one, carried in `body` or `body_base64` by the rule the host
uses. A DAV component sets its own `Content-Type` (`api-request` defaults a
body's to `application/json`), and sends a collection's path with its
trailing `/`, as the server's href gives it. A body sent from an attachment
takes the blob's content type instead, unless the call sets its own.

A component issues `MKCOL`, `COPY` and `MOVE` as any other method. On `COPY`
and `MOVE` it sets `headers["Destination"]` to the destination's absolute
path, percent-encoded within each segment as `path` is, and `api-request`
sends the absolute URI on the request's host; the destination must be
admitted by a clause for that method as the request path is. `read_api_payload`
reads a `201` or `204` as a `Payload`, a `207` answering a partly failed
`COPY` or `MOVE` as a `Payload` carrying the multi-status body, and a `409`,
`412` or `423` as `PayloadAnswer::Error`.

**Attachments.** A step reaches blobs through its `Step`, and only for its own
duration: a `Blob` is a handle the host adapter keeps for one step, released
when the step's outcome is carried out, so an activation serving many requests
stays inside the host's 64 live handles as long as one step does.

- **Receiving.** `request_attachments()` lists the attachments the caller gave
  (`Attached { name, size, content_type }`), and `open_request(name)` opens one.
  A tool takes them only if its `provide` statement says
  `accepts_attachments = true`.
- **Forwarding one as a nested call's body.** `Body::Attachment(name)` sends
  the request's attachment of that name as the body, no byte copied; on the
  wire it is `api-request`'s `body_attachment`. A name the request does not
  carry is the component's fault and fails the activation.
- **Building a nested call's body.** `Body::Built` has the component build
  the call's body when the call is issued, in `Logic::body`, which a
  component that returns it defines (the default panics). `body` is handed
  the call as a backlog row holds it — `Queued`: the tool, the caller's
  arguments, `derived`, the call with its step, and the request's attachment
  headers — and a `Step` that reaches the request's attachments and builds
  blobs, and returns the sealed blob from `append`. It runs in whichever
  activation issues the call: the one that planned or advanced it, or a
  later one that drains it from the backlog, so a value an earlier answer
  taught reaches it through `derived`. The blob goes as the call's
  attachment `BUILT_BODY_ATTACHMENT` (`"body"`), and its content type is
  what `api-request` sends when the call sets no `Content-Type`. Nothing is
  built for a queued call; the call's message alone holds the blob once it
  is sent. A failed build answers the request with its failure and attempts
  no call.
- **Asking for a blob answer.** `ApiCall::answer_as_attachment()` sets
  `response_attachment` to `ANSWER_ATTACHMENT` (`"body"`). `read_api_blob`
  reads the answer as a `BlobAnswer`: `Blob { status, headers, bytes,
  attachment }` for a 2xx, `TooLarge { limit, bytes }` when the body passed
  the host's largest blob, `StoreFull` when the blob pool is full, and
  `Relay`, `Unreadable` and `Error` as the other readers read them. A 2xx
  answered inline though an attachment was asked is `internal`.
  `answer_attachments()` and `open_answer(name)` reach the answer's blob.
- **Reading by range.** `read(blob, offset, len)`, or `pieces(blob)`, the blob
  in order in pieces of the host's `max_read_bytes` (1 MiB).
- **Building a blob.** `append(content_type)` answers an `Append` that
  buffers to `max_write_bytes` per host call; `finish` seals it. At most one
  read piece and one write buffer are in memory, so a blob larger than guest
  memory is handled whole.
- **Completing with attachments.** `attach(name, blob)` names a blob as an
  attachment of the request's completion; `Next::Complete` carries what the
  step attached, `Next::Fail` drops it (an error answer carries none), and
  `Next::Call` after an attach is the component's fault.

Every blob error converts to an `internal` `Failure` naming the bound, so a
step with nothing better to say writes `?`.

The entry point is the grant. `serve` serves no blobs: every `Step` blob
method answers `Unavailable`, an `internal` failure naming `serve_with_blobs`,
and the artifact references no function of the SDK's `blobs` module, so the
class keeps `requires` without `blobs` and grant parity proves it.
`serve_with_blobs` serves them through the SDK, and the class requires
`blobs`. A component served by `serve` that asks for an attachment answer has
its nested call answered `denied` by `api-request`, which `read_api_blob`
reads as every reader reads a denied call. `processor-kit-file-test` is the
worked example of both directions and of a built body (`KitBodyPut`); `google-gmail-read`'s `GmailAttachment` is
one that reads an answer by range and builds the blob it completes with, and
its `GmailBody` one that holds an answer whole, under a bound it checks on
the blob's size before reading, and builds the blobs it completes with.
`google-gmail-compose`'s `GmailDraft` is the one that builds a nested call's
body around the request's attachments: it returns `Body::Built`, opens each
file by name in `Logic::body` and writes the uploaded message a piece at a
time, under bounds on the files' count and bytes it checks in `plan`.
`google-calendar-write`'s `CalendarCreate` and `CalendarUpdate` also return
`Body::Built`, built from the arguments alone: `Logic::body` parses the
stored arguments again and writes the event's JSON, so no field of the event
rides the nested call's arguments or their cap.

Four limits are the component's to design for:

- **A request's attachments last as long as the request.** A request's
  attachment opens at any step — a first step, a queued first step, a later
  step — while the host holds the request pending, wherever its message sits
  in the provider's request inbox. A step that needs one after the request
  has ended — its deadline passed, its caller gone, a restart — answers the
  request `internal` ("the request is no longer pending; its attachment
  `<name>` was not sent"), which the host drops, without spending the call
  budget.
- **An activation's blob bounds are shared** by every step it runs: the 64 MiB
  it may write, its 4,096 blob calls and the fuel attachments add. A refused
  write is `WriteError::Quota`. `max_concurrency` bounds the requests the
  host holds pending, not the answers an activation delivers: a nested call
  admitted late can answer after the host has given up its request and a
  newer one has taken the slot. The kit asks the host whether it still
  holds the request before it reads the answer's in-flight row; an answer to
  a request the host has given up is dropped unread, so it runs no step and
  shares no bound with a live one. The kit has no cutoff of its own. What
  remains is a request that ends while its step runs: that step still runs,
  its completion is dropped by the host and its next call by the kit.
  `GmailAttachment` and `GmailBody` each declare 1.
- **An answer rests after it is read.** An answer asked for as an attachment
  rests on the component's `brenn:tool-results/<slug>` for that inbox's
  window after the component has read it, and so does a completion's
  attachment on its caller's inbox. Size `blob_budget_bytes` at or above the
  sum, over the inboxes that receive attachment answers, of the inbox's depth
  × `max_blob_bytes` (`docs/config-dsl.md`, "The OAuth tools").
- **A request's bytes bring fuel while the host holds it.** A request's body,
  at 256 fuel a byte (`PROCESSOR_FUEL_PER_BODY_BYTE`), and, for a component
  granted `blobs`, its attachments, at 256 a byte, bring fuel to the
  activation that delivers the request and to every later activation of the
  provider while the host holds the request pending; an answer's bytes bring
  fuel to the activation that delivers it. The kit builds a body only for a
  request that is new or still pending, in whichever activation issues its
  call — on arrival, out of the backlog, at a later step — so a build is paid
  for by its request's bytes wherever it happens, and so is the settle of a
  pending request, whose in-flight row holds the arguments whole. A component
  therefore sizes a build and a settle by what they cost a byte against the
  rates at the margin, not by a sum of steps on the host's 50M floor. The
  body rate was sized on the kit's arriving and draining activations, for
  the byte-carrying contract every kit component is on, and on arguments
  that are strings; a component whose passes over a request's bytes cost more
  a byte than the rate covers measures its bound against it.

  A tool that declares an array or object argument admits requests that cost
  more a byte to parse than the rate grants — about 212 fuel a byte for an
  array of `0`s, 163 for an array of empty arrays, 654 for an object of
  distinct three-character keys — and that take several times their bytes in
  guest memory once parsed, every new request of an activation being parsed
  before any is handled. No bound a component declares holds before that
  parse: the host checks a conversation's call for its top-level keys and
  their JSON types and reads no size keyword (`maxItems`, `maxLength`), it
  checks a component's call for nothing but the 64 KiB arguments cap, and the
  kit's own bound runs in `plan`, after every new request has been parsed. A
  declared bound still limits what the component itself then works through,
  and is declared as before. The parse is bounded only by the deployment's
  `[messaging] max_body_bytes`, which has no ceiling, for a conversation's
  call, and by 64 KiB for a component's; so such a component measures its
  fuel and its memory with dense arguments at the bus cap its guide tells a
  deployment to set, the default where it names none, and says in that guide
  that a higher cap is outside what it measured
  (`dense-request-arguments-outgrow-guest` in `TODO.md`).

  What the rule leaves on the floor, the envelope term alone: a late
  answer's own parse — an answer to a request the host has given up, which
  the kit drops unread; the rows the sweep and a scan read for requests that
  have ended; and bodies past the cap — new and pending bodies together are paid for up
  to 16 MiB an activation, so a component whose Σ `max_concurrency` × the
  bus cap can pass that holds more than it is granted for.

  At most `BUILD_BUDGET` (4) bodies are built per activation, a failed build
  included. `max_concurrency` bounds neither the answers nor the ended
  requests' messages an activation is handed, so a component can still reach
  `BUILD_BUDGET` and queue whatever its Σ, and a queued build is paid for
  when it is drained. The kit drops, unbuilt and unissued, the call of a
  request that has ended, the next call of a request that ends while its
  answer is advanced included, so those spend neither budget. `plan` refuses everything `body` could refuse on the
  arguments, so that `body` fails only on what it meets at issue time (the
  blob pool, the activation's bound on bytes written, a request no longer
  pending), because a failed build spends the budget like any other. Every
  build and every other blob write of an activation share its bound on bytes
  written (64 MiB), and a build that bound refuses answers its request rather
  than waiting for a later activation: where bodies together pass it, the
  build that crosses it and every build after it in that activation fail,
  though each alone would have been built.
  A body built from a request's attachment is bounded by the attachment rate
  and the write bound, in whichever activation builds it. A trap while
  draining still rolls the store back, so it repeats on each later
  activation until the entries have waited `BACKLOG_MAX_WAIT_MS` and the
  sweep answers them unbuilt.

`MemoryHost` serves `Blobs` in memory for a host test: `seed_request` and
`seed_result` give a request or a result an attachment's bytes; the limits,
an activation's write and call budgets, a switch under which every
request's attachment answers `NotPending` (`requests_not_pending`; calls and
completions are still recorded, so a test sees the failure the kit gives,
which the host drops) and scripted `create` and `write` refusals
stand in for the host's; and each nested call's body and each completion's
attachments are recorded with their bytes.

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
    help = "//demo-panel:help.md",          # optional
    schema = "//demo-panel:schema.json",    # optional
)
```

It transpiles the component with brenn's own pinned jco — the consumer states
nothing about node or jco, and the version recorded in the emitted record is
the version of the binary that ran — and emits one directory,
`processor/<kind>/`, holding the transpiled JS tree, `<kind>.component.wasm`,
the packaged specification `<kind>.spec.brenn`, and the `manifest.json` that
binds them. `grant_parity_test` arrives with it, exactly as it does for a
backend package.

`help` and `schema` are the kind's documentation sidecars, the files the server
publishes on its help channel. Each is staged beside `processor/`, not in it, as
`brenn_<k>.help.md` / `brenn_<k>.schema.json`, where `<k>` is the kind with every
hyphen an underscore (`demo-panel` → `brenn_demo_panel.help.md`): the server
reads a kind's sidecars from the root that serves it under that stem, and the
record's file list stays exactly `processor/<kind>/`. A kind given no `help`
boots with a warning and a generated stub page; a `schema` that is not JSON is a
boot refusal, so a kind that generates its schema pins the committed copy with a
parity test of its own.

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
  target's directory copied whole. No kernel; flat sidecars only as
  `brenn_<k>.help.md` / `brenn_<k>.schema.json` for a kind `k` staged in the
  same bundle, copied from that kind's own stage to `surface/`. Exactly one
  surface root holds the kernel and that one is brenn's; anything else beside
  `processor/` in a stage fails the assembly.
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
`Harness::new(artifact, spec, page)` reads the class's whole vocabulary off the
specification the artifact was built from — the grants it links, and the ports
it admits — so nothing about the profile is typed at the call site.

What it then refuses falls into two groups, each refusal a panic naming the
port. These mirror what the hosts refuse:

- a publish — plain, urgent or deferred — on a port the specification does not
  declare `out` or `io`;
- a `defer-cancel` or `defer-edit` on such a port;
- a `dom.listen` on a port it does not declare `sync`;
- a scripted activation whose sync cause is neither the reserved mount port nor
  a declared `sync` port.

They are read off the same `brenn_dsl::resolved::PortDir` predicates lowering
writes into the hosts' configuration, so the harness cannot drift from the
hosts on them.

These are the harness's own checks on the script, with no runtime
counterpart — the real hosts window from the document's bindings, and a binding
on a port the class does not declare in that direction is refused when the
document resolves:

- a scripted window on a port the specification does not declare `in` or `io`;
- `retaining_state` on a port that is not `io`.

Two things are not checked. `calls` is not a capability the harness links —
`Kind::compile` refuses a class that grants it — so a `call` port is carried
in `Declared::call_ports` as vocabulary only, and no `calls.call` behaviour is
exercised here. And a deferred op's `index` is recorded, not held to a window.

So a component reaching for a capability or a port its specification does not
declare fails in the author's own suite rather than working in a test and
refusing at boot. `Kind::compile(artifact, Declared)` is the same thing one
level down, and `Declared` is hand-constructible for a fixture that has no
`.brenn` at all.

Both hosts take the artifact as a path, and the page harness takes the
specification as a second one, which is what a `rust_test` has to be told:

```
rust_test(
    name = "tests",
    data = [
        "//demo-counter:component",
        "//demo-panel:component",
        "//spec:demo-counter.brenn",
        "//spec:demo-panel.brenn",
    ],
    env = {
        "DEMO_COUNTER_SPEC": "$(rootpath //spec:demo-counter.brenn)",
        "DEMO_COUNTER_WASM": "$(rootpath //demo-counter:component)",
        "DEMO_PANEL_SPEC": "$(rootpath //spec:demo-panel.brenn)",
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

That is the tree a deployment's mount offers as `modules/` and its
`use @<name>::…` imports resolve against (`config-dsl.md`, *Packaged-module imports*), so the
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

## Mounts

brenn's release installs its packages and modules into one components root and
one module root, and the installer empties both on every deploy — each is
exclusively that release's. A component built elsewhere cannot land in either:
the next brenn deploy deletes it. So the host takes more than one of each, and
learns the whole list from one place.

A **mount** is one directory holding up to four trees plus a `VERSION` file,
which the operator has *declared* to the host. The declaration is the act of
consent: what is under a declared mount may be resolved, loaded and served; what
is not declared is invisible. Nothing discovers a mount — no directory scan, no
installer — so the only way a tree becomes readable is that someone wrote a line
naming it. brenn's own release is a mount like any other.

| tree | what it holds | read by |
|---|---|---|
| `components/` | one `<name>/` package directory per backend component, plus `components/deployed-components.txt` and the `scripts/manifest_names.sh` an installer execs to read it | the WASM loader |
| `surface/` | `processor/<kind>/` per page-hosted kind — the transpiled tree, the component bytes, the packaged spec, the record binding them | the HTTP server |
| `modules/` | the authored module of every one of them, flat | the compiler |
| `config/` | the mount's own deployment text, entry `main.brenn` | the compiler, at document compile |

The first three are *offered*: what is under them is resolved, loaded and served
when a configuration names it. `config/` is different — it is **compiled**, as
part of the deployment document, under the ceiling the mount is declared
`under`. A mount carrying it is a **config-carrying mount**, and its author
writes deployment statements the host runs (`config-dsl.md`, *Mounted
documents*).

A mount ships the trees its release has and no others; a mount holding none of
the four is refused, which is what a path pointed one directory off looks like.
`modules/` is present and empty when a bundle owes it nothing: a replay-world
package ships no specification, so a bundle whose packages are all replay-world
is imported by no configuration and named instead by a `replay_protection`
block's `component =`.

### The mounts document

Mounts are declared in a small `.brenn` document of their own, named by one flag
on the unit:

```
brenn --config /home/brenn/config/brenn-prod.brenn \
      --mounts /home/brenn/mounts.brenn serve
```

```
mount brenn { path = "/home/brenn/brenn/release"; }
mount caser { path = "/home/brenn/brenn/bundles/caser"; }
mount automations under assistant-automations {
  path = "/home/brenn/.brenn-prod/repos/automations";
}
```

The document admits `mount` and `const` statements and nothing else; it imports
nothing, and a `mount` in the deployment document is refused just as firmly.
Every `path` is absolute, no two are the same, and no one nests inside another —
a nested mount would make one release's tree part of another's. Each declared
path must be a directory holding a readable `VERSION` and at least one of the
four trees, or the host refuses to start and a reload refuses to converge.
`VERSION` is required of a config-carrying mount like any other; its content is
that repository's own release counter.

The optional `under` clause names a `principal` of the deployment document and
is what makes the mount config-carrying. It and the `config/` tree are required
of each other: a mount offering `config/` and `under` no one is refused, and so
is a mount `under` a principal that offers no `config/`.

The paths are canonicalized once per read, which is what makes the symlink
install scheme below atomic from the host's side.

`brenn mounts --mounts FILE` prints one tab-separated line of four fields per
declared mount — name, declared path, either `ok:<version>:<trees>`, `missing`
or `fault:<message>`, and the ceiling as `under:<principal>` or empty — and
exits on the document's own validity rather than the filesystem's, so an
installer can read the declaration of a mount it is about to create. The fourth
field is written on every line, empty tail included, so the field count does not
depend on the mount.

### One name, one mount

The host treats the union of the mounts' trees as one namespace with a rule: **a
name may appear under exactly one mount.** A module basename present in two
`modules/` trees, a package directory name present in two `components/` trees,
or a surface kind present in two `surface/` trees is a refusal naming the name
and both roots, whether or not the configuration imports or instantiates it — a
broken install is refused independently of what today's configuration happens to
touch. Identical bytes under two mounts are still refused: two releases shipping
one module means one of them is stale the moment the other updates. The module
scan runs when the configuration loads; the package scan runs immediately after,
before anything is served. Both run again on every reload, so a mount installed
since boot is checked against the ones that were already there.

No kernel bundle; flat sidecars only as `brenn_<k>.help.md` /
`brenn_<k>.schema.json` for a kind `k` the same mount ships, which is where the
server reads that kind's documentation. Exactly one `surface/` tree holds the
kernel, and that one is brenn's.

### Bundles

A **bundle** is the release of a component repository — a tree in the mount
shape, carrying the subdirectories its build ships. The repository is the store
of record; the bundle is what its CI builds from a pinned ref, the way brenn's
tarball is built from brenn's. `component_bundle` stages it and pairs it with
the same contract gate brenn's own tarball passes, so a record that does not
bind the bytes beside it fails the bundle's build rather than the target host's
next boot.

The recommended layout on a host makes an install atomic: the declared mount
path is a **symlink** to a versioned sibling.

```
/home/brenn/brenn/bundles/caser        -> caser.v1.4.2/
/home/brenn/brenn/bundles/caser.v1.4.2/
/home/brenn/brenn/bundles/caser.v1.4.1/
```

The installer stages the whole tree into a fresh directory, renames it into
place, and swaps the symlink; the host canonicalizes on read, so a reload
running concurrently sees either the old tree or the new and never a half-copy.
One previous version is kept, so a rollback is a re-pin rather than a rebuild.

A mount is declared **before** its first install, and the declaration takes
effect at the next reload — not at a restart. Between adding the line and
running the install the document names a path that does not exist, which a
reload refuses; that window is the operator's own and is closed by running the
install.

The exclusivity rule brenn's installer checks among its own install directories
covers mount paths too: the deploying side refuses a layout in which a mount
path equals or nests inside one of brenn's install directories. Without that, a
mount placed inside one of brenn's sync directories is deleted by brenn's next
deploy, and the service then fails with the "not an installed package directory"
refusal naming the missing package and every root searched. Give each mount a
directory of its own. A bundle whose package name collides with a brenn package
is refused as a cross-mount duplicate; the name is the author's to change.

One shared root with per-release ownership manifests was considered and
rejected: two bundles shipping one package name would overwrite each other
silently, and a single root has no second copy for the host to compare. Separate
mounts give the host the collision, and it refuses.

### Upgrading a bundle under a running host

Installing a bundle is staging its tree, swapping its mount, and reloading. On a
deployment that declares the reload facility (`docs/message-bus.md` §2.8) no
bounce is needed for the case that matters most — a new build of a component
whose configuration did not move. The roots are re-derived from the mounts
document on every reload, so a mount declared since boot is in reach without a
restart, and one whose line went away takes its packages with it: a consumer
whose package no mount holds any more is a refusal, and the process keeps
running the bytes it has.

The host does not take the document's word for what is running. Each running
consumer is held against the package record on disk — artifact hash and spec hash
both — so a bundle whose artifact changed under an unmoved document is a *changed*
consumer: the old instance is retired, and the new bytes are loaded, verified and
started in its place. The alternative would be a process that keeps executing
bytes no installed root holds any more.

Two things about the upgrade path are worth knowing before relying on it:

- **A bundle whose `surface/` tree moved bounces the pages that mount its
  kinds.** Surfaces converge, and a kind whose installed bytes moved promotes
  every surface mounting it into the surface delta: those pages are closed with
  the reconfigured code and reload onto the successor, and every other surface
  is untouched. The one thing under `surface/` that does not converge is the
  kernel: every page loads it, so a kernel root that moved is a restart.
- **A bundle upgrade that withdraws a kind a fragment places refuses the
  reload.** The asset scan runs over the whole candidate surface list, so a kind
  no installed mount offers any more is caught before anything is swapped, with
  the old document still running and the refusal naming the kind and the surface
  that would mount it. For a kind placed from a config-carrying mount's
  `extend surface` block, that is the same accepted denial-of-change a fragment
  that stops compiling is: the author's block and the bundle's contents are one
  author's, and reconciling them is theirs.
- **The trees can move while a reload is preparing** if the install is an rsync
  into a live directory, which is not atomic. The versioned-tree-plus-symlink
  layout above is the answer: the swap is one rename and the host resolves the
  symlink once. Installing over a live tree instead, verify first and ask for
  the reload after; a reload that reads a half-synced tree refuses on a hash
  mismatch, which is the correct outcome but not a pleasant way to learn the
  rsync had not finished.

### The vocabulary a page stamps: chrome and self-description

The bindings of brenn's own page vocabulary reach a deployment as packaged
modules a document imports, not as text it transcribes.

A surface must hold exactly one chrome, and the block that wires the reserved
`local:brenn/*` control planes plus `io toast-tick` used to be written
out per page — an unrecognised reserved name is refused, a *missing* plane is
not, so a page whose chrome omitted one booted and went quiet. It is
`use @chrome::*;` and one stamp per page now:

```
use @chrome::*;

new demo_chrome: StandardChrome(slug = "demo", layout = demo_layout) {
  grants = [dom, log, page-dom, ports, takeover];
  surfaces = ["demo"];
}
```

The assembly's body is an `extend surface` block, so the stamp sits *beside* the
surface rather than inside it, and the layout channel is a parameter because one
channel may drive several pages and belongs where its publishers are. A page
with no layout publisher declares an `ephemeral:` channel of its own, which
nothing publishes to, and renders the default layout; the page's `in layout`
binding is a transportable subscribe, so such a page states `subscribe` among
its surface grants.

Write that placeholder `ephemeral:` and not `local:`. A `local:` address on a
surface binding names a *page* ring the kernel mints from the binding itself,
while a `local:` channel declaration names a *server* ring only backend
bindings join; the two share a spelling and exchange no message. Handing a
`local:` declaration to `layout` therefore compiles, boots and renders exactly
as an unbound port does — and invites the edit that never works, where an
operator later publishes a layout document onto the declared channel and the
page never sees it. The `subscribe` grant is what an honest placeholder costs.

**What is one-place and what is not.** The bindings, the depths and the reserved
plane set are the module's, and a change to them is one edit. The ceiling body
is not: the grant words a stamp spells are exactly the words this assembly's
body reaches, and the slug is spelled twice, so a grant word added to the chrome
is still an edit to every stamp in every repository. That is deliberate — the
words and the placement a packaged arrangement holds are the stamping
document's act of consent, and a ceiling that said "whatever the body asks for"
would consent to nothing. The takeover pair is unconditional for the same
reading: every page stamping `StandardChrome` holds `takeover`, and a page that
must not reach that plane writes its own chrome block.

The self-description assemblies — `SurfaceCommons`, `SurfaceDescription(slug)`
and `KindDescription(kind)` — ship the same way, as a library module (*Library
modules*, above) imported with `use @surface-description::*;`.

Each of the three self-description assemblies has its own arity, and a
deployment whose stamps are missing is refused with every missing channel named
at once. `config-check` makes that refusal — so
the bundle installer's pre-stop check does too — and the fit test does not, since
it compiles without lowering. `SurfaceCommons` is stamped **once per
deployment**; `SurfaceDescription` **once per surface slug**;
`KindDescription` **once per processor kind, by the document that ships it** —
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

**Whose line a `KindDescription` is.** The channels are resolved through the
directory and the plan does not care which document declared them, so the rule
is that the kind's shipper describes it. brenn's own kinds, and the kinds of a
bundle that carries no `config/` tree, are the deployment's line. A kind shipped
by a bundle that *does* carry a `config/` tree is described in that fragment,
under a ceiling reaching `prefix "brenn:<prefix>.kind."`. Both describing one
kind is an address collision at compile with both sites named; neither is the
plan refusal naming both missing addresses. Between them the two diagnostics
make the rule self-enforcing, which is what lets a bundle ship a new kind, place
it on a display from its own fragment, and need no edit to the deployment's
text.

A bundle repository that still carries a longhand chrome block is a copy no
longer, and the pin bump that first installs a brenn release shipping
`StandardChrome` is when it is replaced; `brenn-component-demo`'s two copies are
pre-migration until that bump.

## Surface packages

Surface-placed components are bound the same way, with a different carrier. A
surface kind is not one file: it ships as a jco-transpiled directory. So the
record takes the shape that already affords. The vocabulary is the backend's — a
packaged verbatim copy of the author's specification, a content hash of it in a
versioned `deny_unknown_fields` record, and byte-hash equality checked at boot —
and only the carrier differs.

`processor/<kind>/manifest.json` is v3: `source_sha256`, `jco_version`,
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
