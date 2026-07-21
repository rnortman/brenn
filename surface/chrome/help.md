# chrome

The in-tree default chrome component. Chrome owns the page's layout, connection
banner, theme axis, takeover overlay, and toasts. It is an ordinary contract-v1
`dom` component: the kernel activates it like any other, and it learns everything
it renders from the ports bound to it — never from DOM queries or a side channel.

Exactly one component per surface is the chrome (`chrome = true`). Chrome places
every *other* mounted instance into a layout section; it never places itself.

## Inputs (bind these on the chrome instance)

Chrome reads six ports. Bind each to the channel it carries:

| Port | Channel | Carries |
|---|---|---|
| `layout` | a `brenn:` layout channel (retained, depth ≥ 1) | the layout doc (below) |
| `theme` | `local:brenn/theme` | `{ v, theme }` — `theme` is `"dark"` or `"light"` |
| `link-state` | `local:brenn/link-state` | `{ v, state }` — the connection banner |
| `surface-state` | `local:brenn/surface-state` | the mounted-instance set chrome arranges |
| `takeover` | `local:brenn/takeover` | a component's fullscreen request/release (needs the surface `takeover` grant) |
| `toast` | `local:brenn/toast` | transient notices (live-only, retains nothing) |

A surface with no `layout` binding renders the default layout: the first three
mounted instances in configured order, laid out by count (1 → single, 2 →
columns-2, 3+ → columns-3).

## The layout doc

A JSON document naming which instance fills each slot of a layout kind:

```json
{
  "v": 1,
  "kind": "columns-2",
  "ratio": 0.6,
  "panels": {
    "a": { "instance": "left-panel", "label": "Inbox" },
    "b": { "instance": "right-panel" }
  }
}
```

- `kind` is one of `single` (slot `a`), `columns-2` (`a`,`b`), or `columns-3`
  (`a`,`b`,`c`). Every slot the kind names must be present in `panels`.
- Each panel's `instance` must be a mounted, arrangeable instance (not chrome
  itself). `label`, if present, renders as a text header above the panel.
- `ratio` is an optional split fraction exposed to skin CSS as `--surface-ratio`.

Chrome keeps the **last valid** layout on screen: a doc that fails to parse or
names an unknown instance is dropped and reported, never partially applied, and
never blanks the surface. A doc published while a takeover overlay is up is
stored and applied when the overlay pops.
