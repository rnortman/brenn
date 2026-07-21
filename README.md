# Brenn

Yet another agentic harness? Yes, but not just that.

## What Brenn is

I'm going to give you the infrastructure-nerd answer first, because honestly if
you're not an infrastructure nerd, this project is probably too early-stage for
you anyway:

Brenn is a self-hosted application framework for the LLM era: a
capability-sandboxed WASM component host and a pub/sub message bus, written in
Rust, where LLM agents are first-class bus citizens with the same grants, ACLs,
and rate limits as compiled code. It runs at personal/household/small-team
scale, and it panics rather than do the wrong thing.

Oh, it's also a slick UI on various screen surfaces (laptop, phone, touchscreen
on your wall or kitchen counter) plus some smart home/voice assistant stuff.

Fine, here's a version of that for people who want to know what you can do with
it rather than hear about the cool box of tools I built:

- It runs LLM agents (currently via Claude Code only, later other harnesses or
  API direct).
- It has a chat interface (desktop-friendly plus phone PWA).
- It can talk to the outside world (webhooks, MQTT, your smarthome, and more
  coming like Discord/Telegram/Matrix).
- It also runs non-LLM (traditional source code) automations, and those and the
  LLM agents can interact/talk to one another (the automations support the LLMs
  and vice versa).
- It has [to-do lists and a markdown-based graph knowledge
  base](https://github.com/rnortman/graf) (as required by law).
- It also has a [personal finance
  integration](https://github.com/rnortman/pfin).
- More integrations? I'm building a bunch for myself. You can build your own. Or
  your Brenn assistant can build them for you.

It's good at:

- Personal assistant-type applications
- Assistant applications with customized UI so that not everything is typing in
  a text box
- Automations of all sorts
- Fixing your life? Maybe? It's helped me and my family.

It's bad at:

- Being an agentic coding environment. I mean, it *can* be that, for sure, but
  wouldn't you rather use Claude Code?
- Being the basis of your new web-scale social media AI take-over-the-world
  agentic swarm harness. I mean it, this thing *will not scale* beyond
  family/workgroup/small-team levels. (That said, it *will* federate across
  instances, if you like. But it's built for self-hosting, not for being the
  next unicorn startup.)

## Going deeper

Picking that apart, let's start with WASM: Brenn can run WASM components on both
the server side and in the UI. Every component is sandboxed with specific
capability grants and ACLs, and they only talk to the outside world through
Brenn's message bus (with access controls). And: *much of Brenn itself runs in
WASM*. (Currently working on porting more and more of Brenn's own guts to be
self-hosted WASM.)

And that's part of why it's "for the LLM era". In this era, an awful lot of code
is written by LLMs, and casually reviewed by humans. Sandboxing helps contain
bugs. This is true also of human-written code, which is why you see security
breaches in the news a lot, and why you saw them even before LLMs. The idea is:
lock it down, don't trust it, no matter who wrote it.

If you really want to understand *why*, imagine this: Your Brenn-hosted
personal assistant wants to write an automation for you (maybe related to smart
home stuff, or filtering email, or fetching weather reports). The assistant is
an LLM; it can write code. You do not want to review that code; the point of an
assistant is to make your life easier. You're going to run that code with
minimal review if any.

So Brenn lets your Brenn assistant run its own code in a WASM sandbox with
specific ACLs. Less splash damage from bugs. More confidence. The assistant
writes the backend code and the frontend code and deploys both (with your
authorization) and everything runs in a sandbox with ACLs. (You can review the
ACLs or just trust. That's a policy decision you make.)

An awful lot of Brenn itself is written by LLMs. (Not this README, if you can
believe it.) Even extremely hardened and well-tested code is less dangerous if
it's sandboxed. So that's why Brenn hosts its own guts, sandboxed, except in
cases where performance is critical or providing a needed capability to WASM
components is more trouble than it's worth.

I've thought a lot about security during architecture and development. Have a
look at [the formal security posture document](docs/security-posture.md) if you
like. (Full disclosure: Opus 4.8 wrote most of that, and it shows.)

## Why

The philosophy is basically: LLMs are changing what it means to be a software
application, but "just send everything to an LLM" is an expensive and risky
proposition. Brenn is a way to let the LLM author and orchestrate entire
application networks, under the assumption that most pieces of that network are
vibe-coded and untrustworthy. The networks are a combination of traditional code
(does not use expensive LLM tokens) and AI (does anything, but in a sandbox).

There's a UI philosophy too: The fact that you can do *anything* via a text or
voice chat interface does not mean other UIs are dead. Brenn adds app-specific
UI on top of the LLM chat interface, because it's easier to click "done" on a
to-do list item than to tell the LLM to mark it done. But! The LLM can also mark
it done for you. Everything is integrated. So on the go with your voice
interface, you can just tell your Brenn assistant to create a task or mark one
complete or add a note. And non-LLM automation components can do the same thing,
based on signals they get from elsewhere (webhook, MQTT, email, etc).

## What's real and what isn't (status, July 2026)

No vaporware in the pitch, so:

- **The engine is mature.** Backend, the messaging bus, the WASM host and
  capability model, grants/ACLs/rate limiting, MQTT/webhook/Push ingress and
  egress, auth, the Claude Code integration — real, tested, in daily use.
- **The WASM-in-the-browser "surface" UI is young.** The framework under it is
  solid (frozen wire protocol, its own security model, well tested). The
  *applications* on top are thin — right now the flagship demo is a little bar
  touchscreen monitor that sits on my desk under my main monitor and has exactly
  two "apps" that run on it, and already that's made my life way better.
- **Smart home stuff is the current development frontier/focus.** This is my
  highest priority right now for myself: Bring up a Brenn-connected [smart
  speaker/voice assistant](https://github.com/rnortman/brenn-pod), plus other
  sensor pods: mmWave radar (presence sensors) and a camera pipeline, plus Home
  Assistant interfaces.
- **Getting-started docs are not done.** You can build and run it (`make build`,
  `make launchdev`), but standing up a real configuration is currently more
  archaeology than onboarding. It's not ready for anybody who doesn't know what
  they're doing to host it yet. I'd just rather not pretend it's turnkey. So if
  you were looking for the install guide, there isn't one. (Your LLM can figure
  it out though, even if you can't.)

## A note about Brenn vs Home Assistant

In my own home, I have both Brenn and Home Assistant working and managing my
various smart home devices. But I am *not* using HA's built-in LLM intents
system, and mostly not using HA's automations. Brenn and HA communicate via
MQTT, and I have both LLM apps and WASM "hard-coded" automations working
on the MQTT stream. Honestly, HA is mostly there for its unbeatable library
of device drivers/integrations. But at the end of the day, that integration
library is basically a way to get thousands of different devices that speak
different protocols all speaking one language: MQTT+JSON with a bunch of
conventions about topic names.

HA's approach to automations, scenes, etc. is basically this: Set
up automations and scenes without writing code. You can use the UI to
create them, which generates Yaml under the hood, or you can directly author
Yaml. The Yaml is a kind of simple DSL (domain-specific language) for smart
home automations/scenes. It is *great* for what it is: a low-code (or zero
code if you don't think of Yaml as code, but you should really think of Yaml
as code) customizable smart home system.

What most people discover with HA after a while is that the Yaml-based DSL
has limitations. There are a lot of things it cannot do. So then they start
writing Python scripts instead of Yaml. There goes your low-code system! And
as you start to accumulate more and more Python scripts, the thing starts
to get difficult to manage.

You can 100% use Brenn with HA and keep all of HA's automations, Python
scripts, etc. No problem. The way I've personally chosen to approach it
is different: Why do we need low-code at all in the age of LLMs? No, what
we really need is a way to semi-safely execute LLM-authored code. This is
Brenn's lane.

So my own smart home setup uses HA as the device driver library, which makes
my entire smart-home surface accessible via MQTT. Brenn takes it from there,
with pre-built WASM components handling the routine automations, and LLMs
stepping in for stuff that requires intelligence.

I'm also building my own [smart speaker and sensor
pod](https://github.com/rnortman/brenn-pod) and countertop/wall
touchscreen kiosk hardware, and I am not using HA's speech pipeline for
that. I'm DIYing it. Again, because the HA LLM integration and intents system
is *good* (once they abandoned Wyoming protocol anyway), but it's *limited*. I
don't like limits, and I don't like unnecessary latency in my real-time
speech interactions, so I'm building my own real-time speech pipeline,
and using Brenn-hosted WASM to process the speech when the speech can be
pattern-matched to a simple "lights on" type of command, and Brenn-hosted
LLMs (with the full set of tools/capabilities/stored knowledge and context
of my Brenn personal assistant) when the pattern matching doesn't work.

## How Brenn itself was developed

Brenn is mostly LLM-authored, at the source code level. But the original author
(me) has 30+ years of software engineering experience in multiple languages and
multiple domains, including safety-critical and real-time systems, large-scale
data processing/analysis, web applications, and even embedded systems and a
little hardware engineering. I wouldn't call my workflow "vibe coding". It's
true that I didn't hand-write most of the code and I don't even review every
line of code. I have a workflow where I mostly interact at the level of
requirements, design, and reading implementation reports. I *often* intervene at
these levels because code smells tend to percolate into designs when the
designer finds bad code and writes a design that works around it. I see the
workaround and dig in.

I have an [adversarial multi-step code review
workflow](https://github.com/rnortman/claude-plugins) where LLMs do the
line-by-line review. I tend to watch the sorts of things the reviewers are
finding and the way implementer agents respond, and I often intervene at
that level also.

But still, this is risky. I do not allow code to merge at my job (where the
code is safety-critical) without reading every line of it myself, and then
having at least one other human do the same (often more than one). I don't
apply that level of rigor to Brenn, because frankly I don't have time to.

I mitigate the risks of LLM-authored code in Brenn both with that rather
structured workflow, and also by running as much of Brenn as possible in
the same WASM sandboxes that Brenn uses for maybe-sketchy code. Because if
we're being honest, at the time of this writing, LLM-authored code—even
with a structured adversarial-review workflow and lots of CI checks—is
a bit sketchy. Maybe that'll be different next year, but I can tell you
in the course of writing Brenn I've found LOTS of sketchy code that got
through multiple layers of adversarial LLM review. Usually this is code
that follows the letter of the requirements but does something obviously
idiotic. (FWIW, as a lens onto what next year may bring: Once I started using
Claude Fable as the designer and adversarial reviewer, the frequency of that
kind of can't-see-the-forest-for-the-trees mistake went way down. But not
to zero. This note authored circa July 2026.)

## The Brenn ecosystem

Other repos that are part of, or orbit, Brenn:

- [pfin](https://github.com/rnortman/pfin) — personal finance.
- [graf](https://github.com/rnortman/graf) — graph-structured Markdown
  knowledge base *and* to-do list in one.
- [brenn-pod](https://github.com/rnortman/brenn-pod) — nascent smart home
  sensor pod / voice surface.
- [brenn-mcp](https://github.com/rnortman/brenn-mcp) — used internally by Brenn
  to implement MCP tools, but usable outside of Brenn.
- [claude-plugins](https://github.com/rnortman/claude-plugins) — the
  adversarial review workflow Brenn is developed with.

## License

Apache 2.0
