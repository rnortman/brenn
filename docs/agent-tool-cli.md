# `brenn-tool`: your Brenn tools as commands

This guide is for agents running inside Brenn. It describes `brenn-tool`, a
command that runs the Brenn tools you already have from a shell, a script, a
loop or a pipeline instead of as one tool call at a time.

Use it when a tool call would make you retype something large (a 40 KB scene
is a file, not an argument), when you need the same call many times, or when
you need to call one tool, inspect the answer, and call another within the same
turn without a round trip through the model for each step.

## What it is, and what it is not

- **It is your tools, with your permissions.** `brenn-tool call BrennSend …`
  runs the same code, with the same checks, as the `BrennSend` tool call:
  channel ACLs, the send rate and the per-conversation send budget, schema
  validation, the registry's per-tool limits. `call` can do nothing the tool
  call could not. The two exceptions are `save` and `send`
  ([Moving bytes](#moving-bytes-save-and-send)), which move a blob's bytes
  between a file in your environment and the bus, something no tool call does.
- **There is no credential.** `brenn-tool` talks to a socket that exists only
  inside your conversation's environment (`$BRENN_TOOL_SOCKET`); being able to
  reach it is what makes a call yours. Nothing secret goes on a command line.
  Subagents share your environment and so your permissions.
- **It is not a scheduler.** Your container stops whenever the conversation
  goes idle; nothing you start in it outlives the turn. A script runs for
  seconds within a turn, bounded by the Bash tool's own timeout. Durable work
  is `AutoCreate`, or a `BrennSend` with a delivery time — both of which are
  themselves commands.
- **Start, idle and post-pull hook containers have no socket.** Only the
  agent's own container does.

## Discovering tools

```bash
brenn-tool list --pretty            # every tool, whether it is a command, and why not
brenn-tool describe BrennSend --pretty   # one tool, with its input schema
```

`list` prints `[{"name", "description", "available", "reason"?}]`. `describe`
adds `input_schema`, which is exactly the schema the tool call uses: a
command takes the same input object as the tool. A tool a WASM component
provides is listed and called like any other registry tool.

Some tools are listed as not available, with a `reason`:

- `conversation_bound` — the tool's effect is on this chat or this session
  (showing a file, requesting compaction, the tool cards that ask the user to
  choose). From a script it would mean nothing. Call these as tools.
- `requires_approval` — the tool call is not auto-approved: it goes through
  Brenn's approval flow (`GitRepoCommitAndPush`, `GitRepoRun`, `ExportUsage`,
  registry tools not marked auto-approve). A command has no approval step, so
  these are refused. Call them as tools.

**A raw `git push` from Bash is not `GitRepoCommitAndPush`.** It works, but it
skips Brenn's push handling: sibling clones of the repository learn of the push
only at their next poll, and your own conversation is not spared the
"repository updated" notification for a push you made. Prefer the tool call.

## Grammar

```
brenn-tool list [--pretty]
brenn-tool describe <Tool> [--pretty]
brenn-tool call <Tool> [BASE] [--set K=<json>]... [--str K=<text>]...
                       [--str-file K=<path>]... [--json-file K=<path>]...
                       [--timeout <secs>] [--pretty]
brenn-tool save <message_id> (--attachment <name> | --body) --out <path>
                [--timeout <secs>] [--pretty]
brenn-tool send [BASE] [--set K=<json>]... [--str K=<text>]...
                [--str-file K=<path>]... [--json-file K=<path>]...
                --file <name>=<path>... --file-type <name>=<content type>...
                [--timeout <secs>] [--pretty]
```

- `BASE` is the input object: a JSON literal, `-` to read it from stdin, or
  `@<path>` to read it from a file. Absent, it is `{}`. Nothing is guessed from
  whether stdin is a terminal.
- The field options add one field each to `BASE`:
  - `--set K=<json>` — the value is JSON (`--set limit=8`, `--set 'tags=["a"]'`);
  - `--str K=<text>` — the value is the text, verbatim, as a string;
  - `--str-file K=<path>` — the value is the file's contents as a string (the
    file must be UTF-8). This is how a large body goes in without escaping it;
  - `--json-file K=<path>` — the value is the JSON parsed from the file.
- A key given twice — by two options, or by an option and `BASE` — is a usage
  error, never an override. Fields need `BASE` to be an object.
- `--timeout` (default 300 s) bounds the wait for the answer — for `save` and
  `send`, the whole exchange, bytes included. It does **not** cancel the call:
  see below.
- The whole request is assembled before the connection opens, and is capped at
  1 MiB. For `send` that cap bounds the request line (the `BrennSend` input
  and the file declarations), not the files, which stream after it.

## Output and exit codes

stdout carries exactly one JSON value. stderr is unused. `--pretty` indents.

| Exit | stdout |
|---|---|
| 0 | the tool's result — what the tool call would have returned |
| 1 | the tool ran and failed: the tool's own error JSON, as the tool call would have returned it |
| 2 | `{"error": <kind>, "detail": <text>}` |

Kinds with exit 2:

- `usage` — bad arguments (unreadable file, bad JSON, key given twice, …);
- `no_socket` — `BRENN_TOOL_SOCKET` is unset: you are not in an agent's
  environment;
- `connect`, `protocol` — the socket could not be reached, or answered
  something unreadable; a `save` whose bytes stopped short of the size the
  server announced is `protocol`;
- `file` — `save`'s output file, or one of `send`'s input files, could not be
  opened, written or renamed, or an input file changed size while it was
  sent;
- `transfer_aborted` — the server ended a `send`'s byte phase (the connection
  stalled past the server's deadline, or the blob store failed a write);
  nothing was published;
- `unknown_tool` — no such tool in your configuration (also the answer for a
  tool removed by a configuration reload since you listed it);
- `not_available` — the tool is not a command; `detail` is the reason;
- `invalid_args` — the input fails the tool's schema;
- `invalid_request` — the request is malformed or over the cap;
- `not_ready` — the conversation is not ready to serve calls;
- `busy` — too many calls are open at once. **Run calls sequentially; do not
  retry in a loop.** The bound is generous for any sequential script with a
  little fan-out;
- `timeout` — no answer within `--timeout`.

**A call that returned no result may still have happened.** After `timeout`,
or if your container stops mid-call, the server finishes what it started. A
send that printed nothing may have been sent: check the channel before sending
again.

Tools have no result schemas. What a command prints is what the tool returns;
`describe` shows only the input side. Read a tool's output once and match on
what you see.

## Moving bytes: `save` and `send`

A message you read may carry attachments: you are shown each one's name, size
and content type, never its bytes. A message whose body is not text shows a
placeholder instead of the body. These two commands move such bytes between
the bus and a file in your environment without putting them through your
context. They are commands, not tools: `list` does not show them and there is
no tool call that does what they do.

**Naming a blob.** You name an attachment by the `message_id` of the message
that carries it and the attachment's name, both as the message shows them;
you name a body by the `message_id` alone. There are no blob ids to handle.
A name is admitted only if both hold of that one message:

- **you were handed it** — by a push delivery, `MessageChannelGet`,
  `BrennPendingList`, an edit's answer, or the answer to your own send — since
  your conversation started, since its last compaction, since a user last sent
  it a turn that began with `/`, and since the server last restarted. "Handed"
  means the host gave it to you; output you threw away (`> /dev/null`) counts.
- **it is still retained** on a channel your app may read: inside the
  channel's window (the whole ring on an `ephemeral:`/`local:` channel; the
  channel's standing retain depth on a durable one), not parked, and not
  evicted.

The rule is about the message, not the blob: an attachment of an evicted
message is refused even if another retained message names the same blob, until
you read that other message.

**A tool result can carry attachments.** A registry tool that answers with
blobs is shown to you — as a tool call and from `brenn-tool call` alike — as

```json
{"result": <the tool's value>,
 "message_id": "<uuid>",
 "attachments": {"<name>": {"size": 12345, "content_type": "application/pdf"}}}
```

and a result without attachments is the tool's value alone, as always. Name
those attachments by that `message_id`: `brenn-tool save <message_id>
--attachment <name>`, `BrennSend`'s `attachments`, `MqttSend`'s
`attachment`, or another tool call. They
cannot be named after your context is reset — a restart, a compaction, a user
turn that began with `/` — and the message cannot be read again: call the tool
again.

**Handing a tool attachments.** A tool that accepts attachments has an
`attachments` argument in its `describe` schema and says so in its
description. It takes the same form as `BrennSend`'s:
`{"<name the tool sees>": {"message_id": "<uuid>", "name": "<attachment
name>"}}`, under the rule above. A refused name refuses the call before the
tool runs, with the refusal below. A tool without the argument takes no
attachments; passing it is `invalid_args`.

**Publishing an attachment to MQTT.** `MqttSend` takes one attachment in
place of `body` or `body_base64`: its `attachment` argument is a single reference,
`{"message_id": "<uuid>", "name": "<attachment name>"}`, under the rule
above, and the attachment's bytes are published with its content type as the
MQTT Content Type.

```bash
brenn-tool call MqttSend --str to=mqtt:home:cam/snapshot \
    --set 'attachment={"message_id": "0b6a5c2e-7d1f-4e8a-9c3b-2f4d6e8a10e4", "name": "snap.png"}'
```

A call gives exactly one of `body`, `body_base64` and `attachment`, and no
`content_type` beside an attachment. To publish a file of
your own, `send` it to a channel first and name the attachment from that
send's answer. `docs/message-bus.md` §2.7.2 has the whole contract.

**`save`** writes an attachment's bytes, or a body's, to `--out`:

```bash
brenn-tool save 0b6a5c2e-7d1f-4e8a-9c3b-2f4d6e8a10e4 --attachment scan.pdf --out /tmp/scan.pdf
brenn-tool save 0b6a5c2e-7d1f-4e8a-9c3b-2f4d6e8a10e4 --body --out /tmp/body.bin
```

It prints `{"path", "size", "content_type"}` (`content_type` is `null` for an
untyped text body). The bytes go to a temporary file beside `--out` and are
renamed onto it only once every byte has arrived, so `--out` never holds a
partial file; an existing file there is replaced. If the command is killed
mid-transfer the temporary file (`.<name>.<random>.part`) may be left beside
the target. Once the server has admitted the name the save completes from its
own hold on the bytes, even if the message is evicted meanwhile.

**`send`** is `call BrennSend` with files attached. `BASE` and the field
options build the `BrennSend` input exactly as `call` does; forwarded
attachments ride in its `attachments` field.

```bash
brenn-tool send --str to=brenn:inbox.out --str body="Scanned receipt." \
    --file receipt.pdf=/tmp/scan.pdf --file-type receipt.pdf=application/pdf
```

- Each `--file NAME=PATH` needs a `--file-type NAME=TYPE`; nothing is guessed
  from an extension. At least one `--file` is required; a send without files
  is `call BrennSend`.
- Each file must be a regular file. Its size is declared before any byte is
  sent; the server refuses the whole send before a byte crosses if a name,
  type, count or size is wrong. A file that grows or shrinks while it is sent
  ends the connection, exit 2 `file`, and nothing is published.
- It prints the `BrennSend` answer. Every refusal `BrennSend` makes (the ACL,
  the send budget, the rate) applies, after the files have crossed; a refused
  send leaves no blob behind.

**Bounds.** A file is at most the host's largest blob; one send's files
together are at most that same size; a message carries at most 32
attachments, forwarded and files together; the host's blob budget bounds
everything. Each is a refusal you can read — nothing waits for room.

**Refusals** come back as the tool's own error JSON, exit 1:
`{"ok": false, "reason": <kind>, "error": <text>}`. The kinds are
`malformed_message_id`, `malformed_attachment_name`, `not_read`,
`channel_not_readable`, `not_retained`, `no_such_attachment`,
`tool_channel_attachment`, `too_many_attachments`,
`duplicate_attachment_name`, `malformed_content_type`, `no_files`,
`blob_too_large`, `send_files_too_large`, `blob_budget_exhausted`,
`blobs_unavailable` and `payload_too_large` (an `MqttSend` payload over the
MQTT client's `outbound_payload_cap_bytes`). `not_read` — after a restart, a compaction, or a user
turn that began with `/` — is cured by reading the message again
(`MessageChannelGet`) and retrying; for a tool result, by calling the tool
again. `tool_channel_attachment` is final: the message is on a channel of
the tool substrate (`brenn:tools/…`, `brenn:tool-requests/…`,
`brenn:tool-results/…`) that your app may watch, and a tool call's
attachments are for the tool and its caller — you are shown their names,
sizes and content types only. Its body can still be saved.

**Without a shell tool there is no `save` or `send`.** Both run through your
shell. A conversation of an app that lists `Bash` in `disabled_tools` can
still forward an attachment by reference with `BrennSend`'s `attachments`, and
can neither save one nor attach a file.

## Worked example: play a reward and wait for its receipt

A reward is a stored scene with a text slot. Publishing it as a tool call means
retyping the whole scene as an argument; as a command it is a file.

```bash
# Fill the text slot of a stored scene, publish it, wait for its receipt.
# One turn, no retyping. Every receipt carries the play's id, which is the
# message_id BrennSend returned; a "deferred" receipt is not final.
sed "s/{{TEXT}}/Inbox zero. Finally./" rewards/confetti.json > /tmp/play.json
id=$(brenn-tool call BrennSend --str to=brenn:r-deskbar.in.reward.play \
       --str-file body=/tmp/play.json \
     | sed -n 's/.*"message_id":"\([^"]*\)".*/\1/p')
[ -n "$id" ] || exit 1
for i in $(seq 1 30); do
  brenn-tool call MessageChannelGet --pretty \
      --str address=brenn:r-deskbar.out.reward.receipts --set limit=8 \
    | grep -F "$id" | grep -qv deferred && break
  sleep 1
done
```

- `--pretty` puts each message on its own lines, which is what lets `grep`
  work per receipt. Where your image has a JSON parser (`jq`, `python3`), use
  it instead of `sed`/`grep`; do not assume one is installed.
- The channel names here are illustrative. Use the ones the deployed component
  documents on its help channel.
- The loop is bounded (30 s), and the Bash tool's timeout bounds it again.
- The example ends at the receipt. What the screen does afterwards is the
  component's business.

## Verifying a deployment

On a host running containerized agents, before relying on the socket:

1. From a conversation, run `brenn-tool list`, then
   `brenn-tool call MessageChannelList`. Success proves the container's
   security label and user-namespace mapping admit the connection.

   A `connect` error with `Permission denied` from a container that can see
   the socket file is an SELinux `connectto` denial on the listener. On the
   host, grep `/var/log/audit/audit.log` for `connectto` and `tool.sock`
   (`ausearch -ts today` has been seen to return nothing while the records
   were there). The remedy is `tool_socket_selinux_context` on the
   `container` definition. The AVC's `scontext` is the container's domain;
   the value is that context's `user:role:type` at level `s0` — for
   `scontext=system_u:system_r:container_t:s0:c510,c623`, it is
   `system_u:system_r:container_t:s0`. Set it and restart; boot refuses the
   value if the server's domain cannot create sockets under it. Leave the key
   unset until the AVC has been seen: a guessed value stops the server at
   boot, an unset key is this diagnosable failure. With the key set, Brenn's
   log shows `tool socket bound` with the context for each conversation, and
   the audit log gains no `connectto` denial for `tool.sock`.
2. `podman exec` into one of the app's hook containers and confirm
   `BRENN_TOOL_SOCKET` is not set there.
