Publish content to the instance's content channel via BrennSend. The body is
either bare text (rendered as one plain paragraph), or a JSON object:

```json
{
  "text": "<string>",
  "priority": "<very-low|low|normal|high|never, default normal>",
  "expires_at": "<RFC3339 timestamp, optional>",
  "format": "<plain|markdown, default plain>"
}
```

Unknown fields are ignored. The panel keeps one live slot per priority level and
displays the highest-priority slot whose expiry has not passed (ordering
low→high: very-low, low, normal, high, never). When the displayed slot expires
the panel autonomously falls back to the next-highest unexpired slot — no new
message needed. Set `expires_at` to auto-dismiss a slot after a deadline. To
retract a slot before its deadline, republish that same priority with an
`expires_at` already in the past. There is no explicit dismiss message.
