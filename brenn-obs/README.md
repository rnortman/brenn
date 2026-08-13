# Observability

Everything in Brenn that touches the outside world — the browser, Claude Code, the filesystem — needs to be observable. This module is the foundation. It exists so that when something goes wrong at 2am, there's a clear trail to follow, and when something goes wrong that can't wait until morning, your phone buzzes.

## Three Log Streams

Not one log file. Three, because they serve different audiences.

**The diagnostic log** (`logs/brenn.log`) is for you, the developer, after the fact. It captures everything at DEBUG level and above. When a CC session goes sideways or a request fails in a way you didn't expect, you grep this file. It's human-readable, rotated daily, and you never have to worry about losing it because it's written through a non-blocking appender with a guard that flushes on shutdown.

**The security log** (`logs/security.log`) is for fail2ban. It's JSON, one object per line, and only contains events that are explicitly marked as security-relevant — auth failures, schema violations from the browser, unrecognized URLs, rate limit hits. The format is designed so fail2ban can match on `"security_event":true` with a simple string filter instead of fragile regex. If it's in this log, it came from the browser, and it's suspicious.

**The CC transcript** (`logs/cc_<session>.log`) is the raw NDJSON protocol stream between Brenn and Claude Code. Every message in, every message out, timestamped with direction markers (`<<<` for received, `>>>` for sent). This is not a tracing layer — it's a separate writer, because CC protocol messages are a fundamentally different kind of data than application log events. The spike proved that having these transcripts is the difference between "I have no idea what happened" and "ah, CC sent an unrecognized message type at 14:32 and we didn't handle it." One transcript file per CC session, managed by whoever owns the CC subprocess lifecycle.

## Alerting

Some things can't wait for you to read a log file. The alerting system is for those things: panics, dead CC processes, patterns that suggest something is fundamentally broken.

Alerts go through ntfy (a dead-simple HTTP POST to a topic URL). There's a rate limiter so a cascade of failures doesn't melt your phone — but every event is still logged regardless of whether the alert was rate-limited.

The whole thing is non-blocking. Callers push alerts onto a channel; a background task drains the channel, applies rate limiting, and sends. The panic hook uses this too — it does a synchronous channel send (which is why it's non-blocking) and then lets the background task handle delivery. Panic alerts are best-effort; the diagnostic log is the reliable record.

If the alert background task dies, the dispatcher panics on the next alert attempt. That's deliberate — a broken alerting system is an invariant violation, not something to log and ignore.

## The Panic Hook

A custom panic hook that does two things: logs the panic with full location info via tracing (so it hits the diagnostic log), and fires a critical alert. Then it calls the default hook so you still get the backtrace on stderr.

## How It's Wired Together

`obs::init(config)` builds the three-layer tracing subscriber (console + diagnostic file + security file), installs it as the global subscriber, and returns a guard. The guard holds the non-blocking writer handles — drop it and pending writes flush. Hold it in `main()`.

The alerting system and panic hook are separate from `init()` because they have a different lifecycle. You create an `AlertDispatcher`, then pass it to `install_panic_hook`. In development, the alerter is a no-op. In production, it's the ntfy alerter with a real topic URL.

## What This Module Is Not

It's not a metrics system. It's not a distributed tracing system. It's not an enterprise observability platform. It's structured logging with file rotation, a security event stream for fail2ban, raw protocol transcripts for debugging, and phone alerts for emergencies. That's exactly what a two-user application needs.
