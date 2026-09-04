# Claude accounts: profiles and switching

Operator guide. What a Claude account is to Brenn, how to add one, how to point
an agent at a different one, and what Brenn will and will not catch when the
account it thinks it is using is not the account Claude Code actually uses.

The config surface itself — every attribute and every refusal — is in
[the config DSL reference](config-dsl.md#claude-accounts). This document is the
flows.

## What an account is

An account is an environment variable: `claude setup-token` mints a long-lived
OAuth token, and Claude Code reads it from `CLAUDE_CODE_OAUTH_TOKEN`. A
**profile** is a name bound to one such token file. Nothing in the config root
is swapped, symlinked, or moved
([reference](config-dsl.md#claude-accounts)), so session transcripts, settings,
plugins and history are shared across accounts — which is why a conversation
survives a change of account: Brenn resumes it by session id under the new
token.

The consequence is that an account is fixed for the life of a Claude Code
process. Changing one means replacing the process, which Brenn does at a turn
boundary — never in the middle of a turn.

## Adding an account

1. On any machine with a browser — it does not have to be the Brenn host — run
   `claude setup-token`, approve, and copy the token it prints. Claude Code does
   not save it; if you lose it, mint another.
2. On the Brenn host, write it to the token file, readable by nobody but the
   server's user:

   ```
   ( umask 077
     read -rs TOKEN
     printf '%s\n' "$TOKEN" > /home/brenn/.brenn-secrets/claude-profile-spare.token )
   ```

   One line, mode 0600. Write it, do not edit it in place: an editor pointed at
   the token file leaves siblings — `…token~`, `.…token.swp`, a renamed temp —
   at whatever the umask allows, and Brenn checks the permission bits of the
   `token_file` path only. A world-readable copy beside a 0600 original boots
   clean. The startup refusals around the token file — permission bits, empty,
   missing — are in [the reference](config-dsl.md#claude-accounts).

3. Declare it, and say which agents may use it:

   ```
   claude_defaults {
     profile_token_dir = "/home/brenn/.brenn-secrets";
   }

   claude_profile main;
   claude_profile spare { expires = "2027-09-01"; }

   agent PersonalAssistant {
     claude_profiles = ["main", "spare"];
   }

   new alice-pa: PersonalAssistant;
   ```

   With `profile_token_dir` set, a body-less `claude_profile <name>;` finds its
   token at `<profile_token_dir>/claude-profile-<name>.token`. A profile
   somewhere else states `token_file` instead.

   The `new` statement is what turns the class into a running app, and its name
   — `alice-pa` here — is the **app slug** the rest of this document's address
   convention is built on. The class name is not the slug.

4. Restart the server. Tokens are read once, at startup; there is no reload.

Listing an agent's accounts in preference order is the whole configuration for
an agent that never switches, and an agent with no `claude_profiles` is
untouched by any of this — both rules, and what happens to entries that are not
declared profiles, are in [the reference](config-dsl.md#claude-accounts).

## Rotating or replacing a token

Overwrite the file with the same `umask 077` form used above, update `expires`
if you record it, and restart. If you did edit the file in place at any point,
list the secrets directory and delete whatever your editor left beside the
token; nothing in Brenn looks at those. Revoking the old token at the Anthropic console is a separate act and
Brenn cannot see it; a revoked token surfaces as failing turns for the agents
using it.

`expires` is a date you write down, not something Brenn reads out of the token.
Its one effect is a Warning alert at startup when the recorded date is past or
within 30 days; omitting it is fine and costs you that warning
([reference](config-dsl.md#claude-accounts)).

## Switching an agent to another account

Switching is a publish. Each switchable agent names a **goal channel** whose
latest message is the account it should run under:

```
/// Which account the assistant runs under. Latest wins.
channel cc_profile_alice_pa at "brenn:cc-profile.alice-pa" {
  description = "Claude profile goal for the assistant (retained, latest-wins)";
  push_depth = 1;
  retain_depth = 1;
  standing_retain_depth = 8;
  doctype = "brenn.cc-profile.goal@1";
}

uuid_pins {
  "brenn:cc-profile.alice-pa" = "…";
}

agent PersonalAssistant {
  claude_profiles = ["main", "spare"];
  claude_profile_goal = exact cc_profile_alice_pa;
}

new alice-pa: PersonalAssistant;
```

The channel has to be durable and retain exactly one message, and the message
body is the profile name and nothing else — the contract for anything that
publishes goals, in-tree or out, stated in full in
[the reference](config-dsl.md#claude-accounts). `doctype` is optional and
nothing in Brenn reads it today; declare `brenn.cc-profile.goal@1` anyway,
because it is the tag a policy component's goal-publishing port will name, and a
deployment that spelled it some other way fails the doctype-agreement check the
day such a component is installed.

The `brenn:cc-profile.<app slug>` address is a convention rather than a rule,
but it is worth following: the `.` is a segment boundary, so
`prefix "brenn:cc-profile."` grants a future policy component every agent's goal
at once while `exact` grants exactly one. Agents that should move together name
the same channel.

### Doing it by hand

There is no CLI and no UI for this. Until a policy component exists, the way to
switch is to let an agent publish its own goal:

```
agent PersonalAssistant {
  claude_profiles = ["main", "spare"];
  claude_profile_goal = exact cc_profile_alice_pa;
  grants = [publish];
  acl publish [exact cc_profile_alice_pa];
}

new alice-pa: PersonalAssistant;
```

Then ask it, in chat, to send `spare` to that channel; it will use its messaging
send tool.

Grant the `exact` form on the agent's own goal channel, and not more. The
principal you are granting is an LLM conversation whose inputs include tool
results, fetched pages and repository contents, so a successful prompt injection
publishes whatever it likes to that channel. What one publish reaches is **every
agent bound to that channel, onto any account in *that* agent's
`claude_profiles`, for all of that agent's conversations** — a goal is per app,
not per conversation, and acceptance is checked against each bound agent's own
list, never against the publisher's. So a publisher allowed only `main` sharing
a channel with an agent allowed `main` and `reserve` can put that other agent on
`reserve`.

Two rules follow. Do not share a goal channel between an agent that may publish
to it and an agent whose allowed set is wider: give sharers identical
`claude_profiles`, or give the publisher a channel of its own. And keep to
`exact` — `acl publish [prefix "brenn:cc-profile."]` hands account selection for
*every* agent to that same injectable principal, including onto an account you
were holding in reserve or one whose token has expired; nothing in Brenn
rate-limits goal changes. It belongs only on an agent whose tool surface is
narrowed for the purpose.

### What happens next

- If the conversation is idle, the swap starts immediately. If a turn is in
  flight, it finishes first and the swap runs at the turn boundary.
- The conversation shows `Connecting` for the few seconds the replacement takes,
  then `Idle`. A message typed during that window is delivered to the new
  process. If the replacement fails to start (below), a message typed during the
  window is shown in the history but never reached any Claude Code process —
  resend it once the conversation is back.
- The model picker refreshes: which models are offered is a property of the
  account's plan, so the replacement's reported model set replaces the cached
  one.
- A conversation that is not running switches on its next spawn; nothing is
  started early to apply a goal.
- The server log line for the attempt is `swapping CC session onto a new Claude
  profile`, carrying `from`, `to`, `app_slug` and `conversation_id`. That is the
  line to grep for "did it switch, and from what". The later
  `CC session initialized` line carries `cc_profile`, which is the account the
  new process actually started under.
- Not every idle-looking conversation swaps, and the ones that do not log
  nothing at all. A conversation whose process is already scheduled for
  replacement — a compaction drain is pending, or the server is shutting down —
  is left alone, as is one whose goal moved again in the meantime or whose
  process has already died. The goal still stands in every case: the next spawn
  carries it, and `CC session initialized`'s `cc_profile` is where that shows.
  So an absent `swapping CC session` line is not evidence the goal was not
  applied.
- If the replacement fails to start, the conversation goes to `Error` with one
  Warning alert and the bridge is torn down; the next message you send starts a
  fresh process under the new account, with the usual respawn backoff.

A body naming an account the agent may not use, or an empty body, is rejected
for that agent — one Warning alert per process per agent and reason, and that
agent's previous goal stands
([reference](config-dsl.md#claude-accounts)). When several agents share a
channel and their allowed sets differ, one may accept what another rejects; the
alert names the agent.

## Removing a profile

Delete the `claude_profile` block and the agents' references to it, then
restart. If a retained goal still names it, that goal is rejected once at
startup with a Warning and the agent seeds to the first entry of its
`claude_profiles`. The stale retained message stays on the channel until
something publishes over it, which is harmless.

Deleting a profile whose token file remains on disk leaves a live credential
lying around; remove the file too.

## When the account is not the account

Claude Code ranks several credentials above `CLAUDE_CODE_OAUTH_TOKEN`. If one of
them is present, Claude Code uses it, silently, and the profile a conversation
claims to run under is a lie — the spend lands on some other account. This
section is the one prose home for that list; the set Brenn checks for is the
`OUTRANKING_CREDENTIAL_VARS` constant in `brenn-cc-profile`, and it should be
the same six names:

`ANTHROPIC_API_KEY`, `ANTHROPIC_AUTH_TOKEN`, `CLAUDE_CODE_OAUTH_TOKEN`,
`CLAUDE_CODE_USE_BEDROCK`, `CLAUDE_CODE_USE_VERTEX`, `CLAUDE_CODE_USE_FOUNDRY`.

Brenn refuses the cases it can see:

- **Server environment.** If any agent that runs *bare* (no container) has
  `claude_profiles` and the server's own environment carries one of those six,
  startup stops naming the variable. Bare children inherit the server's
  environment.
- **Integrations.** An integration that hands one of those variables to an agent
  with profiles is treated as a bug rather than as a spawn error: building the
  spawn config panics, and the panic hook raises a Critical `Brenn PANIC` alert
  naming the variable and the app. Read that alert as your config, not as a
  Brenn defect — this is the one refusal of the three that fires at spawn rather
  than at startup, because integration environments are assembled per spawn.
- **`--bare`.** `cc_extra_args` containing `--bare` together with
  `claude_profiles` is a config error: under `--bare` Claude Code ignores the
  token entirely.

Two it cannot see, which are yours to keep clean:

- `apiKeyHelper` in the `settings.json` of the agent's home.
- A credential variable baked into a container image.

Neither produces any signal from inside Brenn. Check them when you build a home
or an image, not when the bill arrives.

The token's exposure inside the sandbox — CC's own tools can read their process
environment — is an accepted risk with its reasoning written out in
[the security posture](security-posture.md), §7.1a. The short version: treat a
profile's token file as a durable secret, because unlike a `/login` credential
it is minted for a year and revoked only out of band.
