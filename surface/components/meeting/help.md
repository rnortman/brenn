Meeting-notice panel that shows time-to-next-meeting and escalates ambient →
takeover → critical → overdue, computing every threshold locally from the wall
clock.

Publish a full upcoming-meetings snapshot via BrennSend to the instance's
`agenda` channel (latest-wins; use a retained channel so it replays on
reconnect). Body:

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
        "takeover_secs": "<int>",
        "critical_secs": "<int>",
        "overdue_secs": "<int>"
      }
    }
  ]
}
```

`escalation` is an optional per-meeting override; all values must be `>= 0` and
`takeover_secs > critical_secs`, else it is ignored with defaults 120/60/60.
Unknown fields are ignored, and an empty `meetings` list is a valid idle state. A
malformed snapshot (bad JSON, missing id/start/title, unparseable time, duplicate
id) is ignored and the last snapshot kept.

The panel publishes dismiss/snooze acks to its `acks` channel and subscribes to
the same channel so all devices converge; an ack is:

```json
{ "v": 1, "meeting_id": "<id>", "action": "dismiss|snooze", "until": "<RFC3339, required for snooze>" }
```

To cancel an alarm from the agent side, drop the meeting from the next snapshot
(or publish a dismiss ack). At the takeover threshold the panel publishes a
takeover request on its `takeover` output port (bound to `local:brenn/takeover`);
chrome pushes a fullscreen overlay, granted only on a takeover-granted surface.
The kernel's router stamps the publishing instance onto the request, so a
component cannot request or release another's overlay.
