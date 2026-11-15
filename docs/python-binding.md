# The Python binding: `brenn_bus`

`brenn_bus` lets a Python program attach to a brenn server as a remote
attacher, publish, subscribe and read. It is a CPython extension module over
`brenn-attach-client`, so the wire is spoken by the same Rust code every other
attacher uses; the module only maps it onto Python.

- Source: `attach/python` (the module) over `attach/blocking` (the blocking,
  thread-safe client it maps), with the type stub `attach/python/brenn_bus.pyi`.
- Interpreter: CPython 3.13 or later. The module is built against CPython's
  limited API (`abi3`) with 3.13 as its floor, so one build loads under every
  later CPython 3.
- Platform: built and tested on linux x86_64.
- Build: Bazel only, by this repository or by another Bazel workspace that pins
  it by commit (see "Building from another Bazel workspace").

## A first program

```python
import brenn_bus

with brenn_bus.attach(
    "wss://bus.example.com/remote/alice/ws",
    ident="alice-lab-runner",
    token_file="/etc/lab/alice.token",
    ca_file="/etc/lab/lab-ca.pem",
    timeout=30,
) as client:
    print(client.facts().protocol_version)        # 5

    answer = client.subscribe("ephemeral:lab.status", push_depth=8,
                              retain_depth=8, timeout=10)
    match answer:
        case brenn_bus.Subscribed(replay_count, gap):
            pass
        case brenn_bus.Unavailable():
            raise SystemExit("no such channel")

    match client.publish("ephemeral:lab.commands", brenn_bus.Body.text("start"),
                         timeout=10):
        case brenn_bus.Ok():
            pass
        case other:
            print("refused:", other)

    while (event := client.recv(timeout=1.0)) is not None:
        match event:
            case brenn_bus.Delivery(channel=channel, body=body, dropped=dropped):
                print(channel, body.as_text(), "lost before this:", dropped)
            case brenn_bus.Detached(loss, last_dial_failure):
                print("connection lost:", loss.reason)
```

Every timeout is in seconds, keyword-only, with no default; it must be finite
and not negative. The publish outcomes, the events, `Subscribed`,
`AttachmentFacts`, `Loss` and `DialFailure` support `match` by position, in
the field order this page writes them. `Body` and the exceptions do not: read
their fields as attributes (`except CredentialRejected as e: e.status`).

## Attaching

```python
brenn_bus.attach(url, *, ident, timeout, token=None, token_file=None, ca_file=None) -> Client
```

- `url` is the remote route in full, `ws://` or `wss://`:
  `wss://<host>/remote/<slug>/ws`, where `<slug>` is the `remote` block's name
  in the server's configuration. The binding does not compose it.
- `ident` is a free-form name for this attacher, shown to the server.
- Exactly one of `token` (the bearer token) and `token_file` (a path to it).
  A token file is read once; one final newline is stripped; it is refused if
  it is empty, if any other whitespace surrounds the token, or if it is
  readable by group or others (mode `0600` or stricter, as the server holds
  its own copy).
- `ca_file`: see "Trust".

`attach` validates its arguments before anything is dialled and raises
`ValueError` for a bad one: a scheme other than `ws://` or `wss://`, `ws://` to
a host that is not loopback, both or neither token, a bad token file, a
`ca_file` with no certificate in it, a `token_file` or `ca_file` that cannot be
read (missing, not permitted, a directory; the message names the argument and
its path). It then dials, reconnecting with backoff,
until the first attachment, and returns the `Client`. If that takes longer than
`timeout` it raises `AttachTimeout`; if the first attachment ends for good
first (a rejected token, no protocol version in common) it raises that
`AttachmentEnded` subclass.

`ws://` is accepted to loopback only: `127.0.0.0/8` and `localhost`. IPv6
loopback (`ws://[::1]`) is refused, a known defect of the client's cleartext
guard. Anything off the machine is `wss://`.

`client.facts()` returns the most recent attachment's `AttachmentFacts`:
`protocol_version`, `participant_id` (the server's name for this attacher, the
`sender` of what it publishes), `session_id` (new at every attachment),
`max_body_bytes` (the server's body cap).

### One attachment, supervised

A `Client` owns one background thread, which holds the connection: it checks
liveness, reconnects with backoff after a drop (0.5 s doubling to 30 s, each
dial bounded at 15 s), and re-subscribes every held channel at each new
attachment. Every method may be called from any thread. `close()` ends the
attachment and joins the thread; it is idempotent, a `with` block's exit
calls it, and so does the client's collection, which therefore waits for the
thread as `close()` does.

`client.status()` never blocks. It returns `Attached(facts)` or
`Detached(loss, last_dial_failure)`, and raises the `AttachmentEnded` subclass
once the attachment is over.

### Interrupts

Every blocking call releases the GIL, so other Python threads run while it
waits. On the main thread a wait also runs the interpreter's signal handlers
at least every 100 ms, so Ctrl-C (or any handler that raises) interrupts it:
the handler's exception is raised, and a publish or subscribe it abandoned is
cancelled as a timeout cancels it. On any other thread CPython runs no signal
handler, so only the call's own timeout ends a wait. `close()` is not
interrupted on any thread.

## Publishing

```python
client.publish(channel, body, *, timeout, urgency=Urgency.NORMAL) -> PublishOutcome
```

A `Body` is bytes plus an optional content type:

- `Body(data: bytes, content_type: str | None = None)`, and `Body.text(text)`
  for a UTF-8 text body.
- `data`, `content_type`, `is_text`, and `as_text()`, which returns the text
  or `None` for a body that is not text.
- Construction applies the bus's own rules and raises `ValueError` naming the
  rule broken: an untyped or text-typed body must be UTF-8, and the content
  type must be one the bus accepts.

The server's answer is returned as a value, never raised. One class per
outcome, each a subclass of `PublishOutcome`:

| Outcome | Meaning |
|---|---|
| `Ok()` | Accepted onto the channel. |
| `Failed()` | The server took the publish and it failed on a path the server reports instead of treating as fatal. |
| `RateLimited()` | The remote's server-side send budget refused it (`docs/message-bus.md` §2.1.3). |
| `BodyTooLarge(len, max)` | The body is `len` bytes; the server's cap is `max`. |
| `TextOnlyChannel()` | A body that is not text, to a channel that is not `non_text`. |

`Urgency` is `VERY_LOW`, `LOW`, `NORMAL` or `HIGH`.

A publish that raised `AnswerTimeout` or `ConnectionLost` may or may not have
landed. The binding retries nothing.

## Subscribing and reading

```python
client.subscribe(channel, *, push_depth, retain_depth, timeout, cursorless=False)
    -> Subscribed | Unavailable
client.unsubscribe(channel) -> None
client.recv(*, timeout) -> Event | None
```

`subscribe` asks for the channel with brenn's two depths (`docs/message-bus.md`
§2.2): `push_depth`, how many messages the server pushes, and
`retain_depth`, how many it keeps for a resume. Both zero is a `ValueError`.
The server's ACL for the remote caps each. The answer:

- `Subscribed(replay_count, gap)`: held. `replay_count` messages from the
  window follow as deliveries. `gap` is `None`, `Gap.EPOCH_CHANGED` or
  `Gap.BEYOND_RETAINED` when the server could not cover the subscription's
  cursor.
- `Unavailable()`: the channel is inside the remote's grant but the server has
  no such channel. Nothing is held.

A channel is held from the server's acknowledgement until it is unsubscribed,
the server answers a re-subscribe `Unavailable`, or the client ends. A held
channel is re-sent at every new attachment with its cursor, so the server can
resume where it left off; `cursorless=True` asks for no resume. One hold per
channel: subscribing a held channel again raises `AlreadySubscribed`.

`unsubscribe` drops the hold in any state, attached or not, and waits for no
answer from the server, so it takes no timeout. Once it has returned, no read
yields an event of that channel until it is subscribed again. It raises
`NotSubscribed` for a channel not held (one whose first subscribe is still
unanswered included). It never raises `NotAttached` or `ConnectionLost`.

`recv` returns the next event, or `None` once `timeout` seconds have passed;
`timeout=None` waits indefinitely and `timeout=0` returns at once. Each event
is a subclass of `Event`:

| Event | Meaning |
|---|---|
| `Delivery(channel, message_id, sender, publish_ts, reply_to, urgency, body, seq, dropped)` | One message. `message_id` is the UUID as a string; `sender` the publisher's participant id; `publish_ts` an aware UTC `datetime`; `reply_to` a channel or `None`; `seq` the channel's sequence number; `dropped` how many messages this subscription lost since the previous delivery. |
| `Resubscribed(channel, replay_count, gap)` | A held channel's settlement after a reattach. |
| `SubscriptionDropped(channel)` | A held channel the server answered `Unavailable` on re-subscribe. The hold is gone. |
| `Attached(facts)` | A new attachment after a drop. |
| `Detached(loss, last_dial_failure)` | The attachment dropped; reconnection is under way. |

Subscription events (`Delivery`, `Resubscribed`, `SubscriptionDropped`) arrive
in wire order and none is skipped. Connection state (`Attached`, `Detached`) is
coalesced: only the latest state not yet seen is shown, after the subscription
events ahead of it. A drop and a reattach that both happened between two reads
show as one `Attached`. The first attachment is not an event; `attach` returns
on it.

`Loss(kind, code, reason)` says why an attachment dropped: `kind` is
`LossKind.LIVENESS_TIMEOUT` or `LossKind.TRANSPORT_CLOSED`; `code` is the
peer's close code or `None`; `reason` is text to show, never to parse.

### A subscriber must read

The bus reports loss; it does not prevent it. A channel is a rolling window
and nothing waits for a slow reader (`docs/message-bus.md`, §1 and §2.2). The
binding adds no buffer that loses a message silently:

- Subscription events wait in a buffer of 256. While it is full, the client
  stops reading the socket and keeps serving calls.
- A stalled reader therefore stalls its own connection. The server's bounded
  queues drop for this attacher and the server eventually closes the
  connection; the client reattaches and resumes from its cursor, and the loss
  arrives as `dropped` on the next delivery or `gap` on the settlement.
- A publish made while the buffer is full is not answered until the reader
  drains it, and so ends in `AnswerTimeout`.

A client that holds a subscription must call `recv`. A client that only
publishes fills nothing.

## Failures

Every exception the bus or the client raises is a subclass of `BusError`. An
argument the binding refuses is a `ValueError`. An argument Python's own
conversion refuses is not: one of the wrong type is a `TypeError`, and an
integer out of range (a negative depth) is an `OverflowError`.

| Exception | Raised by | Cause |
|---|---|---|
| `AttachTimeout(last_failure)` | `attach` | No attachment within `timeout`. `last_failure` is the most recent `DialFailure`, or `None`. |
| `NotAttached` | `publish`, `subscribe` | No live attachment at the moment of the call (between two attempts, or during one). |
| `ConnectionLost(loss)` | `publish`, `subscribe` | The attachment dropped before the server answered. A publish may or may not have landed; a subscribe holds nothing. |
| `AnswerTimeout` | `publish`, `subscribe` | No answer within `timeout`. The request is cancelled: a subscribe that timed out holds nothing, even if the server's answer arrives later. |
| `AlreadySubscribed` | `subscribe` | The channel is already held, or its first subscribe is unanswered. Also a `ValueError`. |
| `NotSubscribed` | `unsubscribe` | The channel is not held. Also a `ValueError`. |
| `AttachmentEnded` | `attach`, then every call | The attachment is over for good; see below. |
| `ValueError` | any | A bad argument: a timeout that is negative, NaN or infinite, both depths zero, a `local:` channel (the binding has no local router), or one of `attach`'s refusals. |

`AttachmentEnded`'s subclasses. Once one is raised, every later call and read
raises it too (after any subscription events still buffered have been read):

| Subclass | Cause |
|---|---|
| `CredentialRejected(status)` | The server refused the token at the upgrade (`401` or `403`). Not retried: re-dialling a rejected credential would trip the server's abuse defences. |
| `IncompatibleVersions(ours, theirs)` | No attach protocol version in common; each is a `(min, max)` pair. |
| `ProtocolError(detail)` | The server sent something this client cannot accept. |
| `Futile(attachments)` | 3 attachments in a row sent something and were answered nothing: the server is no longer allowing a held subscription. |
| `ClientClosed` | `close()` was called, or the `with` block exited. |
| `SupervisorPanicked(message)` | A bug in the binding killed its thread. The process lives; the attachment is dead. |

`DialFailure(kind, status, detail)` says why one dial reached no attachment, in
`AttachTimeout.last_failure` and `Detached.last_dial_failure`. `kind` is one of
`DialFailureKind.IO` (name resolution or the TCP connect failed: "nothing
listening"), `TLS` (the handshake failed, an untrusted certificate included),
`UPGRADE_REJECTED` (the server answered the upgrade with HTTP `status`),
`HANDSHAKE` (another websocket handshake failure) and `TIMED_OUT` (the dial
outlived its 15 s bound: "no answer"). `detail` is text to show. No message
the binding produces contains the token.

### A statement the server does not allow

A subscribe or publish outside the remote's ACL is a protocol violation. The
server answers it by closing the connection with no close code, so the client
sees what a network drop looks like:

- the call raises `ConnectionLost` with `loss.code` `None`, and every other
  publish in flight on that attachment does the same;
- the client reattaches and re-subscribes its held channels; the refused
  statement is not repeated;
- the server records a security event for each such statement, so a program
  that retries one in a loop will be banned by the operator's defences.

A distinct "refused" answer needs a close code from the server, which it does
not send today.

### A server that stops reading

Not handled yet. When the server stops reading while the connection stays
open (its host has left the network without closing the connection, or the
server process is alive and stuck) and the client has enough unsent data that
a network write blocks (one large body, or a run of publishes; how much depends
on both kernels' socket buffers), the client's background thread waits on that
write. Until TCP gives up (about fifteen minutes with the kernel's default
retries) or the server's end of the connection goes away:

- `publish` and `subscribe` raise `AnswerTimeout` at their timeout;
- `status()` still returns `Attached`, and no `Detached` arrives; `recv`
  returns by its timeout as usual (it reads the client's buffer, not the
  connection);
- `unsubscribe`, which takes no timeout, does not return. Ctrl-C interrupts it
  on the main thread, and nothing interrupts it on any other thread;
- `close()` and a `with` block's exit do not return, and Ctrl-C does not
  interrupt them on any thread. The client's collection closes it, so letting
  the last reference go blocks the same way, and so does a program's exit
  that frees a client still open, an exception or `sys.exit` unwinding the
  frame that holds it included. Only killing the process (or `os._exit`)
  ends it.

Below that amount of data the writes complete into the kernel, the liveness
check runs, and the client reports `Detached` as usual. A stuck server on the
same machine can be killed, which closes the connection and frees the client.
The repair, a deadline on every write in the attach client, is tracked as
`errhandling-blocked-write-stalls-supervisor-and-close` in `TODO.md`.

## The server side: what a remote needs

The binding attaches as a `remote` (`docs/config-dsl.md`, "Attachers and
ingress"; `docs/security-posture.md`). A minimal block:

```
channel status at "ephemeral:lab.status" { push_depth = 8; retain_depth = 8; }
channel frames at "ephemeral:lab.frames" { push_depth = 8; retain_depth = 8; non_text = true; }

remote alice {
  token_file = "/etc/brenn/alice.token";
  grants = [subscribe, publish];
  acl subscribe [prefix "ephemeral:lab." { push_depth = 8, retain_depth = 8 }];
  acl publish [prefix "ephemeral:lab."];
}
```

- The token file holds the bearer token the binding sends, mode `0600`.
- `grants` names what the remote may do; each ACL lists the channels, and for
  subscribe the depth ceilings.
- A channel carries bodies that are not text only if it is declared
  `non_text = true`; a byte body to any other channel is `TextOnlyChannel`.
- The binding always attaches at attach protocol v5, which carries every body,
  text or not, and is served a `non_text` channel.
- The route is `/remote/<name>/ws` on the server's listener.

## Trust

`wss://` uses rustls with the `aws-lc-rs` provider; nothing is installed
process-wide, so the binding does not disturb any other TLS user in the
process.

- With `ca_file`, a PEM file of one or more certificates, exactly those
  certificates are trusted and nothing else. This is the case for a server on
  a private network whose certificate a private authority signed, or a
  self-signed one.
- Without it, the platform's trust store is read at the first `wss://` dial.
  A store that yields no certificate fails the dial as `DialFailureKind.TLS`.

A certificate the client does not trust fails each dial as `TLS`; `attach`
raises `AttachTimeout` with that `last_failure`.

## Protocol version

The binding offers attach protocol v5 exactly. v5 carries every body; v6 adds
blob transfer, which the binding does not offer, and a v5 client is delivered
what it was before. A server that does not speak v5 ends the attachment with
`IncompatibleVersions`.

## Building from another Bazel workspace

Pin this repository by commit through `rules_rust`'s `crate_universe`, and
build the extension module in the consuming workspace. The pinned crate is
`brenn-attach-python`; Cargo takes its path dependencies (the blocking
client, the attach client, the protocol and envelope crates) from the same
commit. In `MODULE.bazel`, with `rules_rust` and `rules_python` already
declared and a Rust and C toolchain registered:

```starlark
BRENN_COMMIT = "<commit>"

crate = use_extension("@rules_rust//crate_universe:extensions.bzl", "crate")
crate.spec(
    git = "https://github.com/rnortman/brenn.git",
    package = "brenn-attach-python",
    repositories = ["brenn_crates"],
    rev = BRENN_COMMIT,
)

# PyO3's build scripts run whatever python3 is on PATH unless told not to.
crate.annotation(
    build_script_env = {"PYO3_NO_PYTHON": "1"},
    crate = "pyo3",
    repositories = ["brenn_crates"],
)
crate.annotation(
    build_script_env = {"PYO3_NO_PYTHON": "1"},
    crate = "pyo3-ffi",
    repositories = ["brenn_crates"],
)

# The hand-written stub ships in the crate's directory.
crate.annotation(
    additive_build_file_content = 'exports_files(["brenn_bus.pyi"])',
    crate = "brenn-attach-python",
    repositories = ["brenn_crates"],
)
crate.from_specs(
    name = "brenn_crates",
    cargo_lockfile = "//:Cargo.lock",
    lockfile = "//:cargo-bazel-lock.json",
)
use_repo(crate, "brenn_crates", "brenn_crates__brenn-attach-python-0.1.0")
```

Create the two lock files empty (`{}` in the JSON one) and generate them with
`CARGO_BAZEL_REPIN=1 bazel mod deps`. Then, in a `BUILD.bazel`:

```starlark
load("@rules_python//python:defs.bzl", "py_library")
load("@rules_rust//rust:defs.bzl", "rust_shared_library")

rust_shared_library(
    name = "brenn_bus_cdylib",
    srcs = ["brenn_bus.rs"],
    crate_name = "brenn_bus",
    edition = "2024",
    deps = ["@brenn_crates//:brenn-attach-python"],
)

# CPython imports an abi3 module by `<module>.abi3.so`.
genrule(
    name = "brenn_bus_so",
    srcs = [":brenn_bus_cdylib"],
    outs = ["brenn_bus.abi3.so"],
    cmd = "cp $(location :brenn_bus_cdylib) $@",
)

genrule(
    name = "brenn_bus_pyi",
    srcs = ["@brenn_crates__brenn-attach-python-0.1.0//:brenn_bus.pyi"],
    outs = ["brenn_bus.pyi"],
    cmd = "cp $< $@",
)

py_library(
    name = "brenn_bus",
    data = [
        ":brenn_bus_pyi",
        ":brenn_bus_so",
    ],
    imports = ["."],
)
```

`brenn_bus.rs` is one line, `pub use brenn_attach_python;`, which links the
crate and with it the module's init symbol. A `py_test` or `py_binary` that
imports `brenn_bus` depends on `:brenn_bus` and sets `legacy_create_init = 0`,
so no generated `__init__.py` shadows the module.

This has been built and attached to a server on linux x86_64, under CPython
3.14, with a hermetic LLVM toolchain and sysroot. It has not been tried on
linux aarch64.

## What is not offered

- **Blob transfer.** A Python attacher cannot upload a blob, attach one to a
  publish, or open and read one a delivery names
  (`python-binding-no-blob-transfer` in `TODO.md`).
- **Installation outside Bazel.** No wheel, no source distribution, no `pip
  install`; Cargo builds are unsupported in this repository
  (`python-binding-bazel-only-install` in `TODO.md`).
- **Parts of a delivered message:** its blob attachments, `source`, `impetus`,
  `transport` and deferral fields.
- **Alerts, publish batches and deferred publishes.**
- **`local:` channels.** The binding has no local router.
- **An asyncio surface.** The API is blocking and thread-safe.
- **A distinct answer for a refused statement**, and **a bounded wait when
  the server stops reading**: see "Failures".
- **IPv6 loopback over `ws://`**: see "Attaching".
