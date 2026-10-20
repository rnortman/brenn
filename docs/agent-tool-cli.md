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
  validation, the registry's per-tool limits. A command can do nothing the
  tool call could not.
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
command takes the same input object as the tool.

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
- `--timeout` (default 300 s) bounds the wait for the answer. It does **not**
  cancel the call: see below.
- The whole request is assembled before the connection opens, and is capped at
  1 MiB.

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
  something unreadable;
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
2. `podman exec` into one of the app's hook containers and confirm
   `BRENN_TOOL_SOCKET` is not set there.
