# The Python binding: `brenn_bus`

`brenn_bus` lets a Python program attach to a brenn server as a remote
attacher, publish, subscribe and read, and move blobs. It is a CPython extension module over
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
    print(client.facts().protocol_version)        # 6

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
`AttachmentFacts`, `BlobFacts`, `Attachment`, `Loss` and `DialFailure`
support `match` by position, in
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
`max_body_bytes` (the server's body cap), `blobs` (the attachment's blob
door, a `BlobFacts`, or `None` when the server keeps no blob store; see
"Blobs").

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
the handler's exception is raised, and a publish, subscribe or blob call it
abandoned is cancelled as a timeout cancels it. On any other thread CPython runs no signal
handler, so only the call's own timeout ends a wait. `close()` is not
interrupted on any thread.

## Publishing

```python
client.publish(channel, body, *, timeout, urgency=Urgency.NORMAL, attachments=None)
    -> PublishOutcome
```

`attachments` maps each attachment's name to a `Blob` this client holds (see
"Blobs"). A name is 1 to 64 bytes, the first an ASCII letter or digit, the
rest letters, digits, `.`, `_` or `-`; another name, or another client's
`Blob`, is a `ValueError`.

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
| `AttachmentNotHeld()` | A blob the publish names was closed, gone with an earlier attachment, or never sealed. Nothing was stored. |

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
| `Delivery(channel, message_id, sender, publish_ts, reply_to, urgency, body, seq, dropped, attachments)` | One message. `message_id` is the UUID as a string; `sender` the publisher's participant id; `publish_ts` an aware UTC `datetime`; `reply_to` a channel or `None`; `seq` the channel's sequence number; `dropped` how many messages this subscription lost since the previous delivery; `attachments` a tuple of `Attachment(name, size, content_type)`, in name order, empty for a message that names no blob. |
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
- A blob call is not answered while the buffer is full either, and ends the
  same way; blob bytes themselves never enter the buffer.

A client that holds a subscription must call `recv`. A client that only
publishes fills nothing.

## Blobs

A blob is bytes a message names by reference, beside its body
(`docs/message-bus.md`, §2.1.2a). A client moves blobs through its
attachment's **blob door**, which `facts().blobs` describes as
`BlobFacts(max_blob_bytes, max_chunk_bytes, max_handles, max_bytes_held,
max_inflight_reads, upload)`:

- `max_blob_bytes`: the largest blob the server stores;
- `max_chunk_bytes`: the most bytes one wire chunk carries;
- `max_handles`: the most blobs this attachment holds at once, uploads in
  progress included;
- `max_bytes_held`: the most blob bytes this attachment holds at once;
- `max_inflight_reads`: the most read answers in flight; a read is asked for
  `max_chunk_bytes × max_inflight_reads` bytes at a time;
- `upload`: whether this remote may upload.

`blobs` is `None` when the server keeps no blob store; an upload, an open,
and a publish that names attachments then raise `BlobsUnavailable` with
nothing sent.

### A blob is held

```python
client.upload(data, *, content_type, timeout) -> Blob
client.upload_file(path, *, content_type, timeout) -> Blob
client.open_attachment(channel, message_id, name, *, timeout) -> Blob
client.read_blob(blob, offset, length, *, timeout) -> bytes
client.download_blob(blob, path, *, timeout) -> None
blob.close() -> None
```

A `Blob` is a blob this client holds: one it uploaded, or an attachment it
opened. It has `size` and `content_type`, and it lasts until `close()` (or
the end of a `with` block, whose exit calls it), or until the attachment
drops: **nothing held survives a reconnect**, and a `Blob` from an earlier
attachment raises `BlobGone`. Collecting a `Blob` releases nothing. Each
held blob counts against `max_handles` and `max_bytes_held` until it is
closed, so close what you are done with. Closing a blob already closed or
gone succeeds. A `Blob` belongs to the client that made it; passing it to
another client is a `ValueError`.

- `upload` sends `data`, a `bytes`, and returns once the server has sealed it.
  `upload_file` sends the regular file at `path`, as long as it is when
  opened, a chunk at a time; a file that is shorter by the time a chunk is
  read raises `OSError` and the upload is abandoned. Any other kind of file
  (a pipe, a device, a directory) raises `OSError` before anything is sent:
  its length is not what it yields. Bytes from a pipe go through `upload`. `content_type` is a media type, by
  the same rule as a `Body`'s.
- `open_attachment` holds the attachment `name` of the message `message_id`
  (a delivery's `message_id`) on `channel`. The message must still be in the
  channel's window when it is opened; once opened, the blob stays readable
  after the message is evicted.
- `read_blob` returns `length` bytes from `offset`. A range past the end is
  `ReadRefused` with `ReadRefusal.OUT_OF_RANGE` before anything is sent.
  `download_blob` writes the whole blob to a new file at `path`; an existing
  file is `FileExistsError`, and on any failure the file is removed.
- To **publish** a blob, name it in `publish(…, attachments={"name": blob})`.
  To **forward** an attachment, open it and name the opened blob in a
  publish: no byte is read or sent again.

```python
with client.upload_file("run.wav", content_type="audio/wav", timeout=60) as wav:
    client.publish("ephemeral:lab.runs", brenn_bus.Body.text("run 7"),
                   timeout=10, attachments={"capture": wav})

match client.recv(timeout=10):
    case brenn_bus.Delivery(channel=channel, message_id=message_id,
                            attachments=attachments):
        for attachment in attachments:
            with client.open_attachment(channel, message_id, attachment.name,
                                        timeout=10) as blob:
                client.download_blob(blob, attachment.name, timeout=60)
```

Every `timeout` bounds the whole call, however many chunks it moves. An
upload or open that times out or is interrupted is cancelled and its handle
closed, so the server keeps nothing for it. A read that times out leaves the
blob held. An upload or open whose attachment drops first raises
`ConnectionLost` and holds nothing: the server lets go of everything the
attachment held, and no publish can have named a blob the call never
returned.

### What a remote must be granted

- `publish` in the remote's `grants` to upload (`facts().blobs.upload`);
  without it an upload raises `UploadNotGranted`.
- publish authority on a channel (`acl publish`) to publish attachments to
  it.
- subscribe authority on a channel (`acl subscribe`) to open an attachment
  of its messages. An open outside it is a statement outside the grant, and
  the server closes the connection as for any other ("A statement the server
  does not allow"): the call raises `ConnectionLost`.

### Refusals

The server's typed refusals, and the client's own checks against the door's
bounds, are raised as `UploadRefused`, `OpenRefused` and `ReadRefused`, each
with a `reason` from one enumeration.

`UploadRefused(reason, size, max, offset, len)`: the upload made no blob.
The fields a reason does not carry are `None`.

| Reason | Meaning |
|---|---|
| `UploadRefusal.BLOB_TOO_LARGE` | `size` is over the per-blob cap `max`. |
| `UploadRefusal.BUDGET_EXHAUSTED` | The server's blob store cannot reserve the size. |
| `UploadRefusal.HANDLE_LIMIT` | This attachment holds `max_handles` blobs: close one. |
| `UploadRefusal.BYTES_HELD_LIMIT` | The size would pass `max_bytes_held`: close a blob. |
| `UploadRefusal.STORAGE_FAILED` | The server could not create the blob's file. |
| `UploadRefusal.INCOMPLETE` | `[offset, offset + len)` was not written, a server-side write failure. Upload again. |

`OpenRefused(reason, size)`: the open made no blob. `size` is set for
`BYTES_HELD_LIMIT`.

| Reason | Meaning |
|---|---|
| `OpenRefusal.NOT_RETAINED` | The message is not in the channel's window now, is parked, or the channel is gone. |
| `OpenRefusal.NO_SUCH_ATTACHMENT` | The message names no attachment of that name. |
| `OpenRefusal.TOOL_CHANNEL` | The channel is the tool substrate's, whose attachments are never opened over the bus. |
| `OpenRefusal.HANDLE_LIMIT` | This attachment holds `max_handles` blobs: close one. |
| `OpenRefusal.BYTES_HELD_LIMIT` | The blob's `size` would pass `max_bytes_held`. |

`ReadRefused(reason, size)`: the read ended before its last byte. `size` is
set for `OUT_OF_RANGE`.

| Reason | Meaning |
|---|---|
| `ReadRefusal.OUT_OF_RANGE` | The range passes the blob's `size`. |
| `ReadRefusal.UNKNOWN_HANDLE` | The server holds no such blob for this attachment. |
| `ReadRefusal.NOT_SEALED` | The server found the blob an unsealed upload. |
| `ReadRefusal.TOO_MANY_READS` | The server had `max_inflight_reads` answers in flight. |
| `ReadRefusal.STORAGE_FAILED` | The server could not read the blob's file. |
| `ReadRefusal.CLOSED` | The blob was closed, by another thread, during the read. |

## Failures

Every exception the bus or the client raises is a subclass of `BusError`. An
argument the binding refuses is a `ValueError`. An argument Python's own
conversion refuses is not: one of the wrong type is a `TypeError`, and an
integer out of range (a negative depth) is an `OverflowError`.

| Exception | Raised by | Cause |
|---|---|---|
| `AttachTimeout(last_failure)` | `attach` | No attachment within `timeout`. `last_failure` is the most recent `DialFailure`, or `None`. |
| `NotAttached` | `publish`, `subscribe`, `upload`, `upload_file`, `open_attachment` | No live attachment at the moment of the call (between two attempts, or during one). |
| `ConnectionLost(loss)` | `publish`, `subscribe`, the blob calls | The attachment dropped before the server answered. A publish may or may not have landed; a subscribe, upload or open holds nothing. |
| `AnswerTimeout` | `publish`, `subscribe`, the blob calls | No answer within `timeout`. The request is cancelled: a subscribe, upload or open that timed out holds nothing, even if the server's answer arrives later. |
| `BlobsUnavailable` | `publish` naming attachments, `upload`, `upload_file`, `open_attachment` | The attachment has no blob door: the server keeps no blob store. Nothing was sent. |
| `UploadNotGranted` | `upload`, `upload_file` | The remote may not publish, so may not upload. Nothing was sent. |
| `UploadRefused(reason, size, max, offset, len)` | `upload`, `upload_file` | The upload made no blob; see "Refusals". |
| `OpenRefused(reason, size)` | `open_attachment` | The open made no blob; see "Refusals". |
| `ReadRefused(reason, size)` | `read_blob`, `download_blob` | The read ended before its last byte; see "Refusals". |
| `BlobGone` | `read_blob`, `download_blob` | The blob was closed, or the attachment that held it has ended. |
| `AlreadySubscribed` | `subscribe` | The channel is already held, or its first subscribe is unanswered. Also a `ValueError`. |
| `NotSubscribed` | `unsubscribe` | The channel is not held. Also a `ValueError`. |
| `AttachmentEnded` | `attach`, then every call | The attachment is over for good; see below. |
| `ValueError` | any | A bad argument: a timeout that is negative, NaN or infinite, both depths zero, a `local:` channel (the binding has no local router), a malformed content type, attachment name or message id, a negative offset or length, another client's `Blob`, or one of `attach`'s refusals. |
| `OSError` | `upload_file`, `download_blob` | A local file could not be opened, read, created or written: the subclass its `errno` names (`FileNotFoundError`, `FileExistsError`, …), with `filename` set; for a path `upload_file` is given that is not a regular file, `OSError` itself, with no `errno`. |
| `RuntimeError` | `upload`, `upload_file` | A bug in the binding broke an upload; the upload made no blob, the attachment is still up, and an upload again meets the same bug. |

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

- `publish`, `subscribe`, `upload`, `upload_file`, `open_attachment`,
  `read_blob` and `download_blob` raise `AnswerTimeout` at their timeout;
- `status()` still returns `Attached`, and no `Detached` arrives; `recv`
  returns by its timeout as usual (it reads the client's buffer, not the
  connection);
- `unsubscribe` and a `Blob`'s `close()` and `with` block's exit, which take
  no timeout, do not return. Ctrl-C interrupts them on the main thread, and
  nothing interrupts them on any other thread;
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
- The binding always attaches at attach protocol v6, which carries every body,
  text or not, is served a `non_text` channel, and carries blobs. What a
  remote must be granted to move them is under "Blobs".
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

The binding offers attach protocol v6 exactly: every body, and blob
transfer. A server that does not speak v6 ends the attachment with
`IncompatibleVersions`, whose `ours` is `(6, 6)`.

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

- **Installation outside Bazel.** No wheel, no source distribution, no `pip
  install`; Cargo builds are unsupported in this repository
  (`python-binding-bazel-only-install` in `TODO.md`).
- **Parts of a delivered message:** its `source`, `impetus`, `transport` and
  deferral fields.
- **Alerts, publish batches and deferred publishes.**
- **`local:` channels.** The binding has no local router.
- **An asyncio surface.** The API is blocking and thread-safe.
- **A distinct answer for a refused statement**, and **a bounded wait when
  the server stops reading**: see "Failures".
- **IPv6 loopback over `ws://`**: see "Attaching".
