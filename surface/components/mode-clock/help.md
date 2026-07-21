Headless clock component (renders nothing) that drives the surface dark/light
theme by watching the wall clock.

Publish a config body via BrennSend to the instance's config channel — use a
retained channel so the last config replays on reconnect. The body is a JSON
object:

```json
{
  "mode": "<auto|dark|light>",
  "schedule": { "light_start": "<HH:MM>", "dark_start": "<HH:MM>" }
}
```

Unknown fields are ignored. `mode` is required. In `auto` the theme follows the
schedule: light during the half-open local wall-clock interval [light_start,
dark_start) with midnight wraparound, dark otherwise; `schedule` is optional and
defaults to 07:00→19:00 light / 19:00→07:00 dark. `dark` and `light` are fixed
overrides that ignore the schedule. A malformed body (bad JSON, unknown mode,
unparseable time, or equal boundaries) is ignored and the last config kept. The
theme axis only affects skins that ship a light variant (bench); dark-only skins
are unaffected.

The component's only output is a `ThemeBody` (`{ "v": 1, "theme": "dark" |
"light" }`) published on its `theme` output port. Bind that port to the reserved
`local:brenn/theme` plane with a `[[surface.output]]` block; chrome consumes the
plane and writes the resulting `data-theme` on `<body>`.
