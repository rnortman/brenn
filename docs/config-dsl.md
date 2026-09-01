# The Brenn config DSL

Brenn's deployment is described in `.brenn` documents: a root file plus the
modules it imports. This page is the prose reference for the language. The
annotated grammar (`brenn-dsl/grammar/brenn.fltkg`) remains the syntax
reference; this page is about how to use the language well.

## A tour of the language

### The shape of a document

A document is one **root file** and the **modules** it imports. The root file's
directory anchors the tree, and a module key is a path under it: `use
config::bar::*;` reads `config/bar.brenn`, relative to wherever the root file
lives. Nothing in the language can escape that directory — the grammar admits
neither `..` nor an absolute path segment — so a whole tree can be moved or
copied and still means the same thing. The one import that reaches outside the
tree says so in its syntax (*Packaged-module imports*, below).

```
use config::bar::*;
use config::surfaces::*;
use @chrome::*;
```

- One file is one module. Reaching the same file under two keys is refused, and
  so is an import cycle, which is reported with every member of the cycle named.
- A module cannot name the root. Anything a module-hosted class or assembly
  reaches *by handle* must therefore live in a module too. String-addressed
  references — ACL `prefix`/`exact` strings, `endpoint`, `topic_filter` — resolve
  no names and force nothing.
- `use a::b::*;` imports everything module `a::b` declares; `use a::b::Thing;`
  imports one name. Names are file-scoped: two modules may each declare a
  `Panel`, and an instance gets the one its own file's imports reach.

`//` is a comment and reaches nothing. `///` is a **doc comment**, attaches to
the declaration that follows it, and is carried into the resolved config, so it
is where the reasoning behind a declaration belongs.

### Packaged-module imports

A second import form reaches *outside* the tree, to a module a component's
author shipped with the component:

```
use @processor-demo::*;
use @chrome::Chrome;
```

The `@` sigil says the name is not a path under the root's directory. It names
one file, `<module root>/<name>.brenn`, in a directory named on the command
line:

```
brenn --config brenn.dev.brenn --modules config/specs serve
```

The module root is deliberately not in the document. Where the authored modules
live is an environment fact: on a workstation it is a source checkout, on a
deployment host it is the tree the release installed. The same document must
mean the same thing in both places, so the document names the module and the
invocation names the root. A document with a `@` import and no `--modules` is
refused naming the flag, and a `--modules` that is not a readable directory is
refused whether or not anything imports.

A packaged module is one level deep — `use @a::b::C;` is refused — and the
first segment is the whole module name. Everything else about a module holds:
a glob binds what the module declares, a named import checks the name exists,
one file is one module across both roots, and cycles are refused.

**A packaged module declares vocabulary and instantiates nothing.** It may hold
component classes (any number, including none), assemblies, constants, and
further `@` imports — and nothing else. No `new`, no `channel`, no `surface`, no
agent, no attacher, no `acl`, no `uuid_pins`, and no import of a deployment's
own tree, which it can know nothing about. Loading a module runs its top-level
statements, so this is what keeps an author-owned file from adding instances or
channels to a deployment that consented to neither.

Assemblies are welcome there precisely because declaring one effects nothing:

```
// Shipped by the author, in @processor-demo:
assembly DemoLoop(slug: String, source: Channel) {
  channel out at f"ephemeral:{slug}.out" { push_depth = 8; retain_depth = 8; }
  new consume: ProcessorDemo {
    grants = [ports];
    in in <- source;
    out out -> out;
  }
}

// Written by the deployment:
new demo: DemoLoop(slug = "demo", source = feed);
```

The author ships the arrangement; the deployment chooses to stamp it. The `new`
is where consent lives, and it is always the deployment's line.

**Stamping a packaged assembly accepts everything its body declares**, including
authority the deployment never spells out. An assembly body admits `grant` and
`surface` alongside its channels, links and instances, and the declarations-only
discipline does not look inside one — it refuses those forms at a packaged
module's top level, where they would take effect on import, and admits them
inside an assembly, where nothing happens until someone writes `new`. So a
deployment's one-line `new` may confer grants, and open a surface, that appear
only in the author's file. Read a packaged assembly before stamping it, the way
you would read any other thing you are about to authorize.

### Settings sections

The root carries the deployment's own knobs as sections — an identifier, an
optional name, and a body of `key = value;` entries:

```
server {
  bind_address = "127.0.0.1:3000";
  static_dir = ".bazel-bin/frontend/dist";
  secure_cookies = false;
  public_url = "http://127.0.0.1:3000";
}

integration graf {
  command = "graf";
}
```

The kindword (`server`, `integration`, …) is data, not a keyword: what a section
means is decided after the parse. Both the kindword set and each section's key
set are closed, and both refusals are positioned and name the legal set, so
there is no list to memorize — write what you meant and read the error. A few
sections nest (`alerting { mail { … } }`); most nest nothing, and a block under
one that does not is refused as a kindword typo rather than parsed and dropped.

A second section under one kindword-and-name is two answers to one question and
is refused with both sites shown.

### Values and constants

Values are strings, f-strings, raw strings (`"""…"""`), integers, floats,
booleans, lists, inline tables, matchers, and references to names. An f-string
interpolates a dotted path in braces:

```
const secrets_dir = "/var/lib/brenn/secrets";
const alice_env = { DATA_DIR = "/srv/alice/data",
  ENV_FILE = "/etc/pfin.env"
};

pwa_push {
  keypair_file = f"{secrets_dir}/vapid.json";
}
```

A `const` is **front-end only**: it cannot move the lowered config, so binding
one never changes what a document means. It holds literals only — a reference or
an f-string inside a constant is refused, recursively through lists and tables —
which is why a value that must be built from another constant is built at the
use site, not in the constant. Constants are file-scoped wherever in the file
they are written; the convention is to keep them together at the foot of the
module.

A dotted reference reads into a table (`alice_env.DATA_DIR`) or into an
instantiated assembly (`alice-desk.layout`).

### Channels

A channel is a rolling window over the last N messages; `docs/message-bus.md` is
the semantics, and this section is only how to write one. Two forms:

```
/// Bar layout documents. Latest-wins on push; the window is 16 docs of undo.
channel bar_layout at "brenn:bar-layout" {
  description = "Bar layout documents (latest-wins)";
  push_depth = 1;
  retain_depth = 16;
  standing_retain_depth = 16;
}

channel at prefix "mqtt:broker:alice/" { push_depth = 4; retain_depth = 16; }
```

The first **declares** a channel and binds a handle to it: `bar_layout` is now a
name other statements can reach. The second is a **tuning** — no handle, a
matcher over a system-minted family, possibly a prefix — which says something
about channels the system creates rather than creating one. A handle and a
literal address are disjoint spellings: a channel that is declared is referred
to by its handle, never by re-spelling its address.

`standing_retain_depth` is not an optional tuning: it is the disk reaper's
frontier, so every durable (`brenn:`) channel must state it and every other
scheme is refused for stating it. On a non-durable channel `retain_depth` is the
whole retention story.

The address prefix is the contract: it says the blast radius without consulting
any subscriber table. Which prefixes exist, and what each one promises about
durability and reach, is `ChannelScheme` in `brenn-envelope` — the enum the
compiler holds every branch against — described for a reader in
`docs/message-bus.md` §2.1. This page does not restate the set.

Every durable (`brenn:`) channel's uuid is carried, not derived, because a
durable channel whose uuid changes orphans its whole retained history:

```
uuid_pins {
  "brenn:bar-layout" = "81c6ecc1-9694-4f99-8a71-86a38151e9b8";
}
```

Pins are collected document-wide over every loaded module, including the ones an
assembly stamps, and an **unused pin is a compile error** — which is what keeps
the block honest against the declarations rather than becoming a graveyard.

### Links

A **link** wires ports to each other with no channel in between:

```
/// Frames the collector hands the indexer.
link telemetry;

new collector: Sensors { out readings -> telemetry; }
new indexer: Indexing { in feed <- telemetry { push_depth = 8; retain_depth = 8; } }
```

A link is declared wherever a `channel` is — at the top level or inside an
assembly, where it stamps per instantiation — and bindings target it exactly as
they target a channel handle. It has no address, no body, no ACL matchers and no
doctype of its own. The channel is minted at boot: its name is a uuid over the
endpoint set, and its scheme comes from where the endpoints live — all-backend
or all-on-one-surface gives a `local:` ring, anything spanning the wire gives an
`ephemeral:` one. Its depths fold from the endpoints' own windows; everything
else comes from the `messaging` defaults. A channel that needs `sink`,
`send_rate`, `wake_min` or its own `noise` has outgrown a link — declare the
channel.

Binding a port to a link *is* the authorization for it: the transport capability
and the channel matcher each endpoint needs are injected at boot, so neither the
holder nor its surface writes an `acl` entry or a grant word for the link.

Four rules follow from a link being nothing but its endpoint set, and all four
are refused at compile time:

- a link no port binds is nothing;
- a link one port binds connects nothing — a lone `io` wants the free `io` form
  instead, and a lone `in` or `out` wants a channel;
- the endpoints must include at least one publisher (`out` or `io`) and at least
  one subscriber (`in` or `io`);
- an `in` or `io` bound to a link must state `push_depth` and `retain_depth` in
  its tail. There is no `channel` block to carry them, and no default to fall
  back on.

Doctypes agree across a link the way they agree across a channel: every doctyped
port bound to one link must name the same document.

### Component classes: the specification

A `component` class is a component's specification. It states the artifact ABI,
the ports, the document type on each port where there is one, which ports an
instance may leave unwired, and the capabilities the component needs:

```
/// The page chrome: layout, theme, takeover stack, banner, toasts, and
/// overlay-holdership reporting. A surface with no layout channel renders the
/// default layout, and one with no takeover plane has no overlay to hold, so
/// those three ports are optional.
/// Draws the connection banner and the toast container, so it holds `dom`; it
/// also arranges every other instance's wrapper and stamps the page's theme and
/// takeover state, which is what `page-dom` is for.
component Chrome {
  abi = processor;
  requires = [ports, log, dom, page-dom];
  optional = [takeover];
  optional in layout;
  in theme: "brenn.surface.theme@1";
  optional in takeover: "brenn.surface.takeover@1";
  in link-state: "brenn.surface.link-state@1";
  in surface-state: "brenn.surface.surface-state@1";
  in toast: "brenn.surface.toast@1";
  optional out overlay-state: "brenn.surface.overlay-state@1";
  io toast-tick;
}
```

- `abi` is `processor`, the one artifact shape both hosts load. Where an
  instance runs is decided by where it is placed and what it is granted, not by
  the word.
- A port is `in` (the component receives on it), `out` (it publishes on it), or
  `io` (both).
- `optional` before the direction is the author saying an instance may
  legitimately leave this port unwired. Every port without it must be bound by
  every instance, at every placement; the resolver refuses the instance
  otherwise, pointing at the `new` statement and at the port's declaration.

  An unwired optional `out`/`io` port is a live port at run time, not a missing
  one: the component publishes on it and is told ok, and the message is dropped.
  That is the bus model applied to ports — an unwired port is a sink with no
  channel, and publishing to nowhere is as legal as publishing to a channel with
  no subscribers. Wiring is the deployer's business and the component is not
  shown it. A publish to a name the class does **not** declare at all is the
  opposite case: the artifact is hash-bound to the specification, so the host
  ends the activation.
- `: "<tag>"` on a port is its **doctype** — the document contract a binding
  must agree with. See below.
- `requires` and `optional` are the capability lists. A class states every
  capability it reaches through a WIT import in `requires`; only a word with no
  interface behind it may be optional. See *Authority* below.

The class carries the contract; **instances never restate it**. Neither does the
deployment restate where the artifact lives: a top-level instance is resolved
from the package its class's module names, and a surface-placed instance from
its kind, which names the transpiled tree the page instantiates.

**A top-level instance's class comes from a packaged module.** A consumer is
loaded from an installed component package, and the package is the module the
class was declared in — so a class declared anywhere in the deployment's own
tree cannot be instantiated at the top level, and the refusal says to declare it
in a module imported as `use @<name>::*;`. The declaring module is the one that
counts: an assembly in one package over a class another package declares yields
the declaring package, which is the one that ships the artifact. Surface
placements are untouched — a surface-placed component is served by kind and
resolves against no package.

### Doctypes

A doctype is a nominal tag, `<dotted name>@<version>`, compared whole: `@2` is a
different tag, and no version arithmetic happens anywhere. Compile-time only —
nothing lowers, and no runtime consumer reads it yet.

The rules are small:

- Every doctyped port bound to one channel must declare the same tag.
  Disagreement is one diagnostic naming the channel and citing each distinct
  tag's port declaration.
- A channel may state its own expectation, and the tags that reach it are
  checked against it:

  ```
  /// Utterances from the pod host.
  channel utterance at "brenn:alice-pod.out.utterance" {
    description = "What the pod host heard.";
    push_depth = 8;
    retain_depth = 128;
    standing_retain_depth = 128;
    doctype = "brenn.pod.utterance@1";
  }
  ```

  A channel doctype with no doctyped port bound to it is legal and inert — the
  point of the attr is to catch a *future* binding. It is refused on a channel
  *tuning*, which names a family rather than an identity and so names no single
  document contract.
- An untagged port binds to anything, tagged or not, and is never even a
  warning. No port and no channel is ever forced to declare.
- `local:` names are private per ring, so two surfaces (or a page and the
  backend) using the same bare `local:` name are two different channels and are
  not checked against each other. A declared channel's own `doctype` still
  arbitrates every realm that binds its address.
- Agent `subscribe` statements, remotes, and webhook wiring carry no doctypes.
  Tags constrain component ports only.

Adopt a tag only where the payload contract is verifiable at both ends against
the schema the two ends compile against. A guessed tag is worse than an
untagged port. The control-plane bodies take `brenn.surface.<plane>@1`, whose
version is `CONTROL_PLANE_VERSION`; a test over the shipped configs holds the
two equal, since nothing else would notice the day that constant moves.

### Instances and bindings

One statement instantiates everything — components, agents, assemblies:

```
new p1: Protobar {
  grants = [ports, log];
  in messages <- bar_a;
  io tick { push_depth = 1; retain_depth = 2; }
}
```

A binding names a port, an arrow, and a channel: `in port <- chan`,
`out port -> chan`, `io port <-> chan`. The channel is a handle where a
declaration exists and a literal address where none does (`local:` planes,
system-minted `webhook:`/`mqtt:` channels). The trailing block tunes *this
subscription* — `push_depth` is what wakes this instance, `retain_depth` what it
can see — and both are held at or under the channel's ceiling: its
`standing_retain_depth` on a durable channel, its `retain_depth` on every other
scheme.
A deeper binding is not clamped, it is refused at boot, naming the channel
(`docs/message-bus.md` §2.1, §2.2).

`io port { … }` with no arrow is a **free io** form: it claims the port and tunes
it without connecting it, which is how a timer port gets an anonymous
page-local ring. A claimed port counts as bound for the optionality check.

A component placed inside a `surface` runs in the page. The same class placed at
the top level runs in the backend, and is called a **consumer**:

```
new consume-demo: ProcessorDemo {
  grants = [ports];
  activation_burst = 60;
  activation_min_period_ms = 1000;
  in in <- f"{push_endpoint}" { push_depth = 50; retain_depth = 10; noise = alarm; }
  out out -> demo_out;
}
```

Each placement admits its own body keys — a consumer states its store and
activation budget; a surface-placed instance states its send budget
and whether it is the page's chrome — and an unknown key is refused naming the
set that placement admits.

### Tool grants

A participant that invokes registry tools names each one in a `tool` statement.
Both participant kinds write it the same way — inside an agent body, or inside a
top-level instance body:

```
new puller: Syncer {
  grants = [ports, tools];

  tool git-repo-pull {
    allow { repo = "ws"; }
    allow { repo = "notes"; }
    rate_limit { burst = 2; sustained_per_minute = 10; }
  }
}
```

Each `allow` block is one clause: the requirements inside it are ANDed, the
blocks are ORed, and `"*"` is the sole wildcard. **A `tool` statement with no
`allow` block admits every invocation of that tool** — which is what a tool
taking no ACL wants, and is why an empty `allow {}` is refused as the other
spelling of the same thing. The clause keys are the tool's own resource
attributes, a registration fact no document can see: the language carries them
unexamined and the registry refuses a key the tool does not declare at boot.

`rate_limit` is optional, and states both of its counts or neither. Each is at
least one; a bucket that holds nothing or refills by nothing throttles the grant
to nothing.

On a component instance the word and the statements are one configuration:
`tools` in `grants` iff at least one `tool` statement, refused in both
directions. A word with no statement reaches nothing, and a statement with no
word is authority nobody granted. `tools` is backend-only — the surface host
links no tools interface — so a surface-placed instance is refused at the word
and at the statement alike. An agent needs no word: its tool authority is the
statements themselves.

### Surfaces

A surface is a page: transport grants for the wire, a skin, and the component
instances that make up the page.

```
surface bar {
  grants = [subscribe, publish];
  skin = "bench";
  acl subscribe [prefix "brenn:alice-desk."];
  acl publish [prefix "brenn:alice-desk.out."];
  new p1: Protobar { grants = [ports, log]; in messages <- bar_a; io tick; }
  new chrome: Chrome {
    grants = [ports, log];
    chrome = true;
    in theme <- "local:brenn/theme";
    in link-state <- "local:brenn/link-state";
    in surface-state <- "local:brenn/surface-state";
    in toast <- "local:brenn/toast";
    io toast-tick;
  }
}
```

### Agents

An agent class is an application: an LLM conversation with a sandbox, mounted
repos, MCP servers, and bus subscriptions.

```
agent PersonalAssistant {
  name = "Personal Assistant";
  description = "Alice's personal assistant";
  icon = "🤖";
  grants = [publish, subscribe, pwa_push, dynamic_subscribe];
  singleton = true;
  integrations = ["graf", "pfin"];
  container = "sandbox";
  allowed_users = ["alice"];
  mount ws-alice { working_dir = true; }
  mount src-brenn { access = read-only; }
  start_hooks {
    container = [f"PFIN_DATA={alice_env.DATA_DIR} pf rebuild"];
  }
  mcp_server graf;
  mcp_server pfin { command = "pf"; args = ["mcp"]; env = alice_env; }
  subscribe pa_alice { push_depth = 1000; }
  acl subscribe [exact pa_alice, prefix "brenn:surface."];
  acl publish [exact pa_alice];
}

new alice-pa: PersonalAssistant;
```

Five statement forms live in an agent body beside its attrs: `mount` (a
reference to a `repo` declaration, tail per use), `mcp_server` (bare reference,
or the language's one inline-definition form, which exists because a server body
may name class parameters), `subscribe`, `acl`, and named sub-blocks like
`start_hooks`. Agents have no port bindings, so **every plane an agent reaches is
authored** — nothing is derived from wiring.

### Assemblies

An assembly is a parameterized group of entities, stamped once per deployment of
the pattern. It may stamp channels, a surface, instances, and cross-principal
grants — not definitions, and not an `acl` (which would have no enclosing
principal).

```
assembly Deskbar(slug: String, driver: Agent) {
  channel layout at f"brenn:{slug}.in.chrome.layout" {
    push_depth = 1;
    retain_depth = 16;
    standing_retain_depth = 16;
  }
  surface bar {
    slug = f"{slug}";
    grants = [subscribe, publish];
    new chrome: Chrome {
      grants = [ports, log];
      chrome = true;
      in layout <- layout;
      in theme <- "local:brenn/theme";
      in link-state <- "local:brenn/link-state";
      in surface-state <- "local:brenn/surface-state";
      in toast <- "local:brenn/toast";
      io toast-tick;
    }
  }
  grant driver subscribe prefix f"brenn:{slug}.";
  grant driver publish prefix f"brenn:{slug}.in.";
}

new alice-desk: Deskbar(slug = "alice-desk", driver = alice-pa);
```

Parameters are typed — `String`, `Int`, `Bool`, `Table`, `Channel`, `Agent`,
`Repo` — and may carry defaults. An entity parameter is what lets a stamping
carry its whole footprint, authority included, instead of leaving half of it in
the agent class. An assembly earns its place at the **second** stamping within
one document; a single stamping stays longhand.

### Attachers and ingress

Five more declarations, each a body of attrs:

- `remote <name> { … }` — a native daemon that attaches to the bus over the
  wire, with a token file, transport grants, and ACLs.
- `webhook <name> { … }` — an HTTP ingress endpoint: mount path, content type,
  and named sub-blocks for `signature`, `key`, and `replay_protection`. The
  replay block names its guard by installed package —
  `component = "replay-generic";` — and, unlike a consumer's, that name is
  anchored by no import: a replay component ships no module, so a typo surfaces
  at boot rather than at compile.
- `mqtt_client <name> { … }` — a broker connection; `mqtt:<client>:<topic>`
  addresses name channels through it.
- `repo <name> { remote = …; }` — a git remote plus a slug, mounted by whichever
  agents want it.
- `mcp_server <name> { … }` — an MCP server every app that wants it references
  by name.

### Authority

Authority is written in two layers, and both are deny-by-default.

**Layer 1, `grants`** — what a principal holds, in the vocabulary its own
entity type states.

A component instance's grants name capabilities: `ports`, `store`, `log`,
`alert`, `config`, `mqtt`, `takeover`. Most of them select whether a WIT
interface is linked into the artifact at all; `takeover` names no interface and
is gated at the binding instead. Which of them a host can implement is fixed — a
page has no `store` and no `mqtt`, a top-level consumer has no page to take
over — and the illegal word is refused by name at the instance rather than left
out of the vocabulary.

A `surface` or a `remote` — the two attach-route principals, a browser page and
a native daemon — writes **plane** words instead: `subscribe`, `publish`, and
`alert`. A plane is one word here and one right per address scheme in the config
it lowers to, so `subscribe` covers the durable and the ephemeral scheme at
once. The scheme-compound spellings that lowered config uses
(`ephemeral_publish`, `mqtt_subscribe`, …) are refused by name, pointing at the
plane word to write instead. An agent states the two planes plus
`dynamic_subscribe` and `pwa_push`. What a page's components may do *within* the
page is a component grant, never a wire right.

**Layer 2, `acl`** — what that capability reaches, as matchers over addresses:

```
acl subscribe [
  exact pa_alice,
  prefix "brenn:surface.",
  endpoint f"{push_endpoint}",
  topic_filter f"{mqtt_cal_delta}"];
```

`exact` and `prefix` take a channel handle or an address string. Write a
`prefix` that ends at a segment boundary (`.` or `/`) so it cannot reach past
the family it names — a channel *tuning* prefix is required to, and an ACL
prefix that does not is a wider grant than it looks. A matcher may carry an
inline-table tail (`prefix "…" { push_depth = 4, retain_depth = 64 }`) where the
subscription's depths belong with the scope. `grant <principal> <plane> <matcher>;` is the cross-principal
form — authority written *about* another principal, which is what an assembly
needs to wire its driver.

A `client` entry on a top-level component's publish plane may carry a tail too,
and it tunes the MQTT egress sink that entry mints:

```
acl publish [client "mqtt:broker" { publish_per_activation = 2.0, publish_capacity = 4.0 }];
```

Both keys are optional and both are token counts, spelled as the `out` binding
spells the same two knobs; an entry with no tail leaves the sink on the
runtime's default budget. One client is one sink, so two budgeted entries naming
one client are refused, and only a top-level component holds a sink of its own —
the tail on any other principal's `client` entry is refused too.

A component states no plane at all: its transport rights are read off its
bindings and ACL entries. Two of its capability words therefore pair with
wiring, and the pair is checked both ways — a component that sends and grants no
`ports` is refused, and so is one granting `ports` that neither binds an output
nor states a publish entry, because the interface then reaches nothing. `mqtt`
pairs with an `mqtt_publish` entry the same way at the top level. Those two are
the pairs the DSL itself refuses; most of the remaining words pair as well,
checked when the document is loaded rather than when it compiles: `config` with a
`config` map, `store` with a `store_path` (top level, where the store exists),
`alert` with the enclosing surface's own `alert` grant, and `takeover` with a
binding to a takeover-plane channel. Each is checked in whichever directions
carry a meaning — a map no component may read and a grant that reads an empty map
are both refused, and so are a takeover binding without the grant and a
`takeover` grant that binds no takeover plane. `log` is the one word that pairs
with nothing.

**The spec fit check** joins the two ends of the component story. A class states
what it needs; an instance states what it is given; both directions are checked:

- every word in the class's `requires` must appear in the instance's `grants`;
- every granted word must appear in `requires ∪ optional` — a grant the class
  never asked for is refused, because the spec is the vocabulary.

`requires` is written by every class: a component that needs nothing writes
`requires = [];`. **Only a word with no interface behind it may be optional.** A
component reaches a capability through a WIT import, and the host links that
interface only where the instance was granted the word; an artifact cannot
import conditionally. So an optional *interface* grant is unexercisable in both
directions — an artifact that imports the interface panics at load under an
instance that omits the word, and an artifact that does not import it is
unchanged by holding it. The word is refused where it is written, and the fix is
to move it into `requires` or drop it from the spec. `takeover` is the exception
that shows the rule: it names no interface, being consent to a binding the page
gates, so nothing links conditionally and a surface where no component requests
the overlay hands its chrome no takeover plane to bind.

A class requiring `store` placed on a surface refuses twice, once for host
legality and once for the missing grant. Both diagnostics are true and point at
the same contradiction.

## Ownership

**The component specification is owned by the component's author, in full.** The
abi, the ports, their doctypes, which ports are optional, and the capabilities
the component needs are statements about the artifact, and only the person who
builds the artifact is in a position to make them. A deployment does four things
and no more: it **instantiates**, **wires**, **grants**, and **consents**. The
first of those carries the other three whenever it stamps a packaged assembly:
the wiring and the grants may be written in the author's file, and the
deployment's `new` is the consent to all of it (*Packaged-module imports*).

That division is why a deployment does not hold the specification at all. It
**imports the author's module** — `use @<kind>::*;` — from the module root its
invocation names, which on a deployment host is the tree the release installed
beside the components themselves. There is nothing to restate and nothing to
keep in step: the file the configuration compiles against is the file the author
wrote.

What binds that file to the artifact is content hash. A shipped component
travels with the author's specification and a record binding the two
(`component-packages.md`). At boot the host compares the hash of the
specification the running configuration compiled against with the hash that
record carries, and refuses to start on any difference. So a module root holding
another release's modules is caught at boot, including the case no earlier check
can see, where the module is a faithful copy of *some* release and the installed
artifact is from another one.

This holds at **both placements**. A backend component's carrier is the package
beside its `.wasm`; a surface-placed component's is the record its build emits
into the surface asset tree, per kind. One practical consequence,
also at both placements: a class declared inline in a deployment's own
configuration cannot drive a component instance, because its file is not the
author's file byte for byte.

## Authoring conventions

These rules apply to every `.brenn` tree, in this repo and in a deployment's
own config directory. They are canonical here: a deployment document keeps at
most a pointer to this page plus whatever delta is local to it.

- A root document is the deployment manifest: settings sections, `uuid_pins`,
  deployment-specific channels, and the `new` statements that say what runs.
  Definitions live in modules beside it.
- One module per subsystem, not per syntax kind. Component specifications are
  the exception and get one class per file — a spec is owned by the component's
  author and travels as a unit, imported with `@` rather than kept in the tree
  (see below).
- An assembly earns its place at the second stamping within one document; a
  single stamping stays longhand.
- A shape lives in a shared module only if it is identical for every root that
  imports it. Depths and grants that differ between documents stay in the
  documents.
- The rationale for a shape is written once, on the declaration that defines
  it, and never repeated at a stamping.
- A class name folds to its wire kind: `EchoStub` is `echo-stub`, `ModeClock`
  is `mode-clock`.

### Component specifications

What a specification *says* is *Component classes* and *Doctypes* above; who
owns it is *Ownership*. What is left is where the file goes:

- One class per file, `<kind>.brenn` under the authoring repo's `config/specs/`,
  named for the wire kind the class folds to — a spec travels as a unit, so it
  is never grouped with others, and the filename is the name importers spell
  after the `@`.
- A deployment imports exactly the modules whose components it instantiates. An
  installed module nobody imports is never loaded; it is inert declaration text,
  not an orphan to prune.
- The rationale for a port, an `optional` mark, or a required capability is a
  `///` comment on the class, written by its author and read by everyone who
  imports it.

## Superseded conventions

Reading a document from before these changed, or a commit message that
describes the old shape, the three rules below were true and are not any more.

- **"A component class unifies as the superset: an unbound port is legal."** It
  was, and instances were free to bind whichever ports they used. Ports are now
  required unless the class marks them `optional`, so the class states which
  ports an instance may skip rather than leaving it to each instance.
- **`component_path`, on the class and then on the instance.** The artifact path
  used to be a class attr, which made a specification deployment-specific — the
  one fact in it that cannot be an author's statement. It moved to the consumer
  instance, and then off the vocabulary entirely: the package name is the whole
  reference and the host resolves it against the root `serve --components`
  names. A document that still states it is refused as an unknown key.
- **Component grants were a backend-only notion.** A surface-placed component
  held no capability list of its own; its page's transport grants were the whole
  authority statement, and the grant vocabulary differed between the two ABIs.
  Every component instance now carries a required `grants` list in one
  vocabulary at both placements, which is what makes a class-level `requires`
  checkable everywhere a class can be placed.

## Checking a document

```
brenn config-check <root.brenn>     # compile the tree; refusals are positioned
brennfmt --check <file>             # canonical formatting
```

`make check` in this repo runs both over every shipped document. The formatting
gate globs every `.brenn` file in the tree and the compile gate globs the roots,
so a new root joins both by existing; a module joins the compile gate by being
imported, and a module no root imports is formatted and never compiled. A deployment runs the same two commands over its
own tree, in its own CI, against the `brenn` binary it intends to run.

Compilation is fail-closed and reports the whole document: a refusal is a
positioned diagnostic, often with related sites in other files, and a document
that does not compile is a boot panic rather than a degraded start.
