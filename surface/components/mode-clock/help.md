<!-- AUTO-GENERATED from this component's src/help.rs. Do not edit; run `make regen-surface-help`. -->
Headless clock component (renders nothing) that drives the surface dark/light
theme by watching the wall clock.

Publish a config body via BrennSend to the channel bound to the instance's `config` port — use a retained channel so the last config replays on reconnect. The body is a JSON object:

```json
{
  "mode": "<auto|dark|light>",
  "schedule": {
    "light_start": "<HH:MM>",
    "dark_start": "<HH:MM>"
  }
}
```

Unknown fields are ignored. `mode` is required and is one of:

- `auto` — day/night switching by the schedule below
- `dark` — fixed dark; the schedule is ignored and nothing is scheduled
- `light` — fixed light; the schedule is ignored and nothing is scheduled

`schedule` is optional; omitted, it resets to the default light 07:00, dark 19:00.

In auto mode the theme follows the schedule: light during the half-open local
wall-clock interval [`light_start`, `dark_start`) with midnight wraparound, dark
otherwise. A malformed body (bad JSON, unknown mode, unparseable time, or equal
boundaries) is ignored and the last config kept. The theme axis only affects
skins that ship a light variant (bench); dark-only skins are unaffected.

The component's only output is a `ThemeBody` — `{"v":1,"theme":"dark"}`, where `theme` is `dark` or `light` — published on its `theme` output port. Bind that port to the reserved `local:brenn/theme` plane with a `[[surface.output]]` block; chrome consumes the plane and writes the resulting `data-theme` on `<body>`.
