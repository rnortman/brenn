<!-- AUTO-GENERATED from this component's src/help.rs. Do not edit; run `make regen-surface-help`. -->
Meeting-notice panel that shows time-to-next-meeting and escalates ambient → takeover → critical → overdue, computing every threshold locally from the wall clock.

Publish a full upcoming-meetings snapshot via BrennSend to the instance's `agenda` channel (latest-wins; use a retained channel so it replays on reconnect). Body:

```json
{
  "v": 1,
  "meetings": [
    {
      "id": "<opaque string, the ack join key>",
      "start": "<RFC3339>",
      "title": "<string>",
      "end": "<RFC3339, optional, display only>",
      "escalation": {
        "takeover_secs": 120,
        "critical_secs": 60,
        "overdue_secs": 60
      }
    }
  ]
}
```

`escalation` is an optional per-meeting override, shown above with the defaults it takes when absent. All three values must be `>= 0`, `takeover_secs > critical_secs`, and `overdue_secs < 3600` (an `overdue_secs` at or past the retire cap below would retire the meeting while it is still `critical`); an override breaking any of those is ignored and the defaults used. Unknown fields are ignored, and an empty `meetings` list is a valid idle state. A malformed snapshot (bad JSON, missing id/start/title, unparseable time, duplicate id) is ignored and the last snapshot kept.

An undismissed meeting retires 3600 s after its start: it stops escalating and leaves the panel, so a morning meeting nobody dismissed does not alarm all afternoon.

The panel publishes dismiss/snooze acks to its `acks` channel and subscribes to the same channel so all devices converge. A dismissal is permanent:

```json
{"action":"dismiss","meeting_id":"standup-2026-07-12","start":"2026-07-12T15:00:00+00:00","v":1}
```

A snooze suppresses the occurrence until its `until`, then re-manifests at whatever rung applies; the panel's own Snooze button uses 300 s:

```json
{"action":"snooze","meeting_id":"standup-2026-07-12","start":"2026-07-12T15:00:00+00:00","until":"2026-07-12T15:05:00+00:00","v":1}
```

`start` is the acked meeting's `start`, copied verbatim from the snapshot, and it
scopes the ack to that one occurrence: a `meeting_id` reused tomorrow, or the same
id rescheduled to a different `start`, is not suppressed by today's dismissal. An
ack with no parseable `start` names no occurrence, so it is dropped with a warning
and suppresses nothing.

To cancel an alarm from the agent side, drop the meeting from the next snapshot (or publish a dismiss ack). At the `takeover` threshold (120 s before start by default) the panel publishes a takeover request on its `takeover` output port (bound to `local:brenn/takeover`); chrome pushes a fullscreen overlay, granted only on a takeover-granted surface. The kernel's router stamps the publishing instance onto the request, so a component cannot request or release another's overlay.
