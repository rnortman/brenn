//! DOM-free mode-clock state machine.
//!
//! Every branch here is host-tested. The wasm glue converts each `Err` into a
//! panic (operator misconfig or shell/proto skew), which the module's panic hook
//! turns into an error card. A well-formed delivery whose *body* violates the
//! config convention is a semi-trusted publisher fault: it keeps the current
//! config, bumps a page-lifetime counter, and is reported to the operator log —
//! never a panic. Same posture as protobar's malformed body.
//!
//! The config channel is a full snapshot, latest-wins: each accepted message
//! fully replaces the effective config (an omitted `schedule` resets to the
//! default). No stored theme state can diverge from the wall clock — every
//! recompute derives the theme from the current wall time, so a suspend/resume,
//! NTP step, or DST transition self-corrects on the next boundary recompute.

use brenn_surface_contract::PortWindow;
use brenn_surface_schema::{THEME_DARK, THEME_LIGHT};
use serde::Deserialize;

use brenn_surface_component_support::parse_delivery;
pub use brenn_surface_component_support::{ContractViolation, FaultReport};

use crate::spec::port::CONFIG;

/// Minutes in a wall-clock day. Membership and boundary math are done in
/// minutes-since-local-midnight, so no timezone arithmetic is ever needed.
const MINUTES_PER_DAY: u16 = 24 * 60;

/// The default auto schedule: light 07:00, dark 19:00 — the product default for
/// a fresh install with no retained config.
pub(crate) const DEFAULT_LIGHT_START: u16 = 7 * 60;
pub(crate) const DEFAULT_DARK_START: u16 = 19 * 60;

/// The computed theme. The wire strings come from the shared `proto::THEME_*`
/// constants, so the `ThemeBody.theme` values cannot drift from chrome's parser.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Theme {
    Dark,
    Light,
}

impl Theme {
    /// The `ThemeBody.theme` wire value.
    pub fn as_wire_str(self) -> &'static str {
        match self {
            Theme::Dark => THEME_DARK,
            Theme::Light => THEME_LIGHT,
        }
    }
}

/// The effective operating mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Mode {
    /// Day/night switching by [`Schedule`].
    Auto,
    /// Always dark; no boundary, so nothing is scheduled.
    Dark,
    /// Always light; no boundary, so nothing is scheduled.
    Light,
}

impl Mode {
    /// Every variant, in the order the help sidecar lists them. The single
    /// source both [`parse_config`] and the help generator read, so the
    /// documented vocabulary is the accepted one.
    pub(crate) const ALL: [Mode; 3] = [Mode::Auto, Mode::Dark, Mode::Light];

    /// The `mode` wire value.
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Mode::Auto => "auto",
            Mode::Dark => "dark",
            Mode::Light => "light",
        }
    }

    /// Parse a `mode` wire value, or `None` for one this component does not
    /// implement.
    fn parse(s: &str) -> Option<Self> {
        Mode::ALL.into_iter().find(|mode| mode.as_str() == s)
    }
}

/// An auto-mode day/night schedule, in minutes-since-local-midnight. The light
/// span is the half-open interval `[light_start, dark_start)` with wraparound,
/// which is total and well-defined for any distinct pair (including a light span
/// that wraps past midnight, i.e. `light_start > dark_start`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Schedule {
    light_start: u16,
    dark_start: u16,
}

impl Default for Schedule {
    fn default() -> Self {
        Schedule {
            light_start: DEFAULT_LIGHT_START,
            dark_start: DEFAULT_DARK_START,
        }
    }
}

impl Schedule {
    /// Whether `now` (minutes since local midnight) is in the light span.
    fn is_light(self, now: u16) -> bool {
        if self.light_start < self.dark_start {
            self.light_start <= now && now < self.dark_start
        } else {
            // Light span wraps midnight (light_start > dark_start; equal is
            // rejected at parse, so the two are always distinct here).
            now >= self.light_start || now < self.dark_start
        }
    }

    /// Minutes from `now` to the next schedule boundary (strictly after `now`);
    /// a boundary landing exactly on `now` is a full day away.
    fn minutes_until_next_boundary(self, now: u16) -> u16 {
        forward_delta(now, self.light_start).min(forward_delta(now, self.dark_start))
    }
}

/// Cyclic minutes from `now` forward to `boundary`, strictly positive: a
/// boundary equal to `now` is a full day (`MINUTES_PER_DAY`) away, never `0`, so
/// a recompute at a boundary schedules the *following* boundary rather than
/// busy-firing on the current instant.
fn forward_delta(now: u16, boundary: u16) -> u16 {
    let raw = (boundary + MINUTES_PER_DAY - now) % MINUTES_PER_DAY;
    if raw == 0 { MINUTES_PER_DAY } else { raw }
}

/// The effective config. Rebuilt wholesale from each accepted snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Config {
    mode: Mode,
    schedule: Schedule,
}

impl Default for Config {
    /// No retained config ever published → `auto` with the default schedule, so
    /// a fresh system gets day/night switching out of the box.
    fn default() -> Self {
        Config {
            mode: Mode::Auto,
            schedule: Schedule::default(),
        }
    }
}

/// The outcome of an accepted `config` delivery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigOutcome {
    /// Body parsed and replaced the effective config.
    Accepted,
    /// Body violated the config convention. Config untouched; the report carries
    /// what the DOM glue needs for a `COMPONENT_LOG` error.
    Malformed(FaultReport),
}

/// What folding one `config` window left for the glue to log. Both are operator
/// errors: one names a binding whose `push_depth` is wrong, the other a
/// publisher whose body is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigNote {
    /// The window presented more than one new config document, which a
    /// latest-wins port never should. Carries the ready-to-log line.
    Misconfigured(String),
    /// The applied document violated the config convention; config untouched.
    Malformed(FaultReport),
}

/// The result of a recompute: what to dispatch now and when to wake next.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TickPlan {
    /// The theme to dispatch, or `None` when it is unchanged since the last
    /// dispatch. The first recompute always dispatches (so the shell converges
    /// from the page default).
    pub dispatch: Option<Theme>,
    /// Minutes until the next scheduled recompute, or `None` when there is no
    /// next one to schedule (fixed dark/light modes have no boundary).
    pub next_in_minutes: Option<u16>,
}

/// Milliseconds in a minute — the unit the schedule counts in and the release
/// instant is stated in.
const MS_PER_MINUTE: u64 = 60_000;

/// How far ahead a wake is ever parked, in minutes.
///
/// A count of local minutes is not a count of elapsed milliseconds: a zone shift
/// between now and the boundary moves them apart by the whole shift, so a wake
/// aimed at a boundary hours away over a DST transition lands an hour off. The
/// horizon bounds how long that arithmetic is trusted — past it the chain wakes,
/// re-reads the local clock, and re-aims. An intermediate wake dispatches nothing
/// (the theme is unchanged, and `last_dispatched` says so), so the only cost is
/// the wake itself.
const MAX_PARK_MINUTES: u16 = 15;

impl TickPlan {
    /// When the next recompute is due, epoch milliseconds UTC, given the instant
    /// this plan was computed at. `None` when there is no next one.
    ///
    /// The boundary is a whole number of minutes from the *floored* minute, so the
    /// release is this minute's start plus that many — targeting the boundary
    /// instant itself rather than up to 59 seconds past it. Minute boundaries fall
    /// at the same instant in local time as in UTC (zone offsets are whole
    /// minutes), so the flooring is done in UTC even though the schedule is local.
    ///
    /// A boundary further out than [`MAX_PARK_MINUTES`] is parked at the horizon
    /// instead; the recompute that wake causes re-derives the rest from a fresh
    /// local reading.
    ///
    /// Always strictly after `now_ms`: both the minute count and the horizon are
    /// strictly positive, so a recompute *at* a boundary schedules the following
    /// one rather than a wake at the instant it already woke for.
    pub fn release_at(&self, now_ms: u64) -> Option<u64> {
        let minutes = self.next_in_minutes?.min(MAX_PARK_MINUTES);
        Some(now_ms - now_ms % MS_PER_MINUTE + u64::from(minutes) * MS_PER_MINUTE)
    }
}

/// Raw config body as serde sees it. Unknown fields are ignored (no
/// `deny_unknown_fields`): this is a de-facto external contract that evolves
/// additively. `mode` is required; `schedule` is optional (absent → default).
///
/// `Serialize` is for the help generator, which serializes a placeholder instance
/// as the documented body shape so the field names in the doc are the ones serde
/// reads here.
#[derive(Deserialize, serde::Serialize)]
pub(crate) struct RawConfig {
    pub(crate) mode: String,
    #[serde(default)]
    pub(crate) schedule: Option<RawSchedule>,
}

/// Raw schedule times as `HH:MM` strings, validated explicitly so an unparseable
/// time produces a precise malformed reason rather than a generic serde error.
#[derive(Deserialize, serde::Serialize)]
pub(crate) struct RawSchedule {
    pub(crate) light_start: String,
    pub(crate) dark_start: String,
}

/// Parse an `HH:MM` wall-clock time to minutes since midnight, or `None` if it
/// is not a valid 24-hour time.
fn parse_hhmm(s: &str) -> Option<u16> {
    let (h, m) = s.split_once(':')?;
    let hours: u16 = h.parse().ok()?;
    let minutes: u16 = m.parse().ok()?;
    if hours > 23 || minutes > 59 {
        return None;
    }
    Some(hours * 60 + minutes)
}

/// Render minutes since midnight as the `HH:MM` wall-clock string
/// [`parse_hhmm`] accepts — the inverse, so a schedule default stated in the
/// help sidecar is computed from the constant rather than retyped.
pub(crate) fn fmt_hhmm(minutes: u16) -> String {
    format!("{:02}:{:02}", minutes / 60, minutes % 60)
}

/// Mode-clock state: the effective config, the last theme dispatched (for
/// change-only dispatch), and a page-lifetime malformed-config counter.
#[derive(Debug, Default)]
pub struct ModeClock {
    config: Config,
    last_dispatched: Option<Theme>,
    faults: u64,
}

impl ModeClock {
    pub fn new() -> Self {
        Self::default()
    }

    /// The number of malformed config messages seen this page lifetime.
    pub fn faults(&self) -> u64 {
        self.faults
    }

    /// Handle a `config`-port delivery. Rejects a wrong port and an unparseable
    /// envelope (both `ContractViolation`, panic-worthy skew). A well-formed
    /// envelope whose body violates the config convention returns
    /// `ConfigOutcome::Malformed` — config untouched, counter bumped — so one
    /// buggy publisher cannot brick the theme axis.
    pub fn on_config(
        &mut self,
        port: &str,
        envelope_json: &str,
    ) -> Result<ConfigOutcome, ContractViolation> {
        let envelope = parse_delivery(port, &[CONFIG], envelope_json)?;
        match parse_config(&envelope.body) {
            Ok(config) => {
                self.config = config;
                Ok(ConfigOutcome::Accepted)
            }
            Err(reason) => {
                self.faults += 1;
                Ok(ConfigOutcome::Malformed(FaultReport::new(
                    &envelope, reason,
                )))
            }
        }
    }

    /// Fold one activation window on the `config` port.
    ///
    /// The port is latest-wins: a config message is a full snapshot that
    /// replaces the effective config wholesale, so only the newest new one is
    /// applied. A window presenting more than one new document is a binding
    /// whose `push_depth` exceeds 1 — reported, then the latest is applied
    /// anyway, so a misconfigured depth costs an operator error and nothing
    /// else.
    ///
    /// `dropped` is deliberately not reported here: on a retained latest-wins
    /// channel a superseded config passing the position unserved is coalescing
    /// doing its job, and the very next window carries the current value.
    ///
    /// The port is checked first, so a window from a port this component does not
    /// bind is the contract violation it is whether or not it happens to carry a
    /// new message — an idle turn must not be the turn that lets the skew through.
    pub fn on_config_window(
        &mut self,
        window: &PortWindow,
    ) -> Result<Vec<ConfigNote>, ContractViolation> {
        if window.port != CONFIG {
            return Err(ContractViolation::WrongPort {
                port: window.port.clone(),
            });
        }
        let mut notes = Vec::new();
        if let Some(message) = window.latest_wins_misconfiguration() {
            notes.push(ConfigNote::Misconfigured(message));
        }
        let Some(envelope) = window.latest_new() else {
            return Ok(notes);
        };
        let envelope_json =
            serde_json::to_string(envelope).expect("a MessageEnvelope serializes to JSON");
        if let ConfigOutcome::Malformed(report) = self.on_config(&window.port, &envelope_json)? {
            notes.push(ConfigNote::Malformed(report));
        }
        Ok(notes)
    }

    /// Recompute the effective theme at `now` (minutes since local midnight),
    /// deciding what to dispatch and when to wake next.
    pub fn tick(&mut self, now: u16) -> TickPlan {
        let theme = self.compute(now);
        let dispatch = if self.last_dispatched == Some(theme) {
            None
        } else {
            self.last_dispatched = Some(theme);
            Some(theme)
        };
        let next_in_minutes = match self.config.mode {
            Mode::Auto => Some(self.config.schedule.minutes_until_next_boundary(now)),
            Mode::Dark | Mode::Light => None,
        };
        TickPlan {
            dispatch,
            next_in_minutes,
        }
    }

    /// The effective theme at `now` under the current config.
    fn compute(&self, now: u16) -> Theme {
        match self.config.mode {
            Mode::Dark => Theme::Dark,
            Mode::Light => Theme::Light,
            Mode::Auto => {
                if self.config.schedule.is_light(now) {
                    Theme::Light
                } else {
                    Theme::Dark
                }
            }
        }
    }
}

/// Validate a config body into a [`Config`], or produce a precise malformed
/// reason. An omitted `schedule` resets to the default (snapshot semantics: each
/// message fully replaces the config).
fn parse_config(body: &str) -> Result<Config, String> {
    let raw: RawConfig =
        serde_json::from_str(body).map_err(|e| format!("unparseable config: {e}"))?;
    let mode = match Mode::parse(&raw.mode) {
        Some(mode) => mode,
        None => return Err(format!("unknown mode {:?}", raw.mode)),
    };
    let schedule = match raw.schedule {
        None => Schedule::default(),
        Some(raw) => {
            let light_start = parse_hhmm(&raw.light_start)
                .ok_or_else(|| format!("unparseable light_start {:?}", raw.light_start))?;
            let dark_start = parse_hhmm(&raw.dark_start)
                .ok_or_else(|| format!("unparseable dark_start {:?}", raw.dark_start))?;
            if light_start == dark_start {
                return Err(format!(
                    "light_start == dark_start ({:?}); the schedule has no boundary",
                    raw.light_start
                ));
            }
            Schedule {
                light_start,
                dark_start,
            }
        }
    };
    Ok(Config { mode, schedule })
}

#[cfg(test)]
mod tests {
    use super::*;
    use brenn_surface_test_fixtures::sample_envelope_json;

    /// A config body as wire JSON text, wrapped in the sample envelope.
    fn config_msg(fields: serde_json::Value) -> String {
        sample_envelope_json(&fields.to_string())
    }

    fn m(h: u16, min: u16) -> u16 {
        h * 60 + min
    }

    /// The mode declared after `mode`, or `None` for the last one.
    ///
    /// The one enumeration of the variants that does not read [`Mode::ALL`], so it
    /// can be used to check it. What the exhaustive match buys is a compile error
    /// *at this site* when a variant is added — no more: the arm you write must
    /// link the new variant into the chain (the previous last mode now returns it,
    /// and it returns `None`). An arm that merely terminates the chain compiles,
    /// and the walk below then never reaches the new variant, so the check passes
    /// with the mode missing from both. Stable Rust cannot enumerate variants, so
    /// that step is on the author; this chain is where the compiler asks for it.
    fn next_mode(mode: Mode) -> Option<Mode> {
        match mode {
            Mode::Auto => Some(Mode::Dark),
            Mode::Dark => Some(Mode::Light),
            Mode::Light => None,
        }
    }

    /// `ALL` must list every variant exactly once: it is the accepted vocabulary
    /// (`parse` searches it), the published one, and the iteration order for
    /// every other test — so a gap here is a silent gap everywhere.
    #[test]
    fn mode_all_lists_every_declared_mode_exactly_once() {
        let declared: Vec<Mode> =
            std::iter::successors(Some(Mode::Auto), |mode| next_mode(*mode)).collect();
        for mode in &declared {
            let count = Mode::ALL.iter().filter(|m| *m == mode).count();
            assert_eq!(
                count,
                1,
                "Mode::ALL must list {} exactly once, found it {count} times",
                mode.as_str()
            );
        }
        assert_eq!(
            Mode::ALL.len(),
            declared.len(),
            "Mode::ALL holds a mode the declaration chain does not"
        );
    }

    /// `fmt_hhmm` is `parse_hhmm`'s inverse across the whole day, so a schedule
    /// time the help sidecar prints is one the parser reads back as the same
    /// minute.
    #[test]
    fn hhmm_round_trips_through_the_parser() {
        for minutes in 0..MINUTES_PER_DAY {
            let text = fmt_hhmm(minutes);
            assert_eq!(
                parse_hhmm(&text),
                Some(minutes),
                "{minutes} formatted as {text}"
            );
        }
        assert_eq!(fmt_hhmm(DEFAULT_LIGHT_START), "07:00");
        assert_eq!(fmt_hhmm(DEFAULT_DARK_START), "19:00");
    }

    /// The wire values a dispatched theme carries into the `ThemeBody` the wasm
    /// glue publishes on `local:brenn/theme` — the exact strings chrome parses.
    #[test]
    fn theme_wire_values_match_the_control_plane_body() {
        use brenn_surface_schema::{CONTROL_PLANE_VERSION, ThemeBody};
        for (theme, wire) in [(Theme::Dark, "dark"), (Theme::Light, "light")] {
            assert_eq!(theme.as_wire_str(), wire);
            let body = ThemeBody {
                v: CONTROL_PLANE_VERSION,
                theme: theme.as_wire_str().to_string(),
            };
            let json = serde_json::to_string(&body).unwrap();
            let back: ThemeBody = serde_json::from_str(&json).unwrap();
            assert_eq!(back, body);
            assert_eq!(back.theme, wire);
        }
    }

    #[test]
    fn default_is_auto_with_default_schedule() {
        let mut clock = ModeClock::new();
        // 08:00 is inside the default 07:00–19:00 light span.
        let plan = clock.tick(m(8, 0));
        assert_eq!(plan.dispatch, Some(Theme::Light));
        // Next boundary is 19:00, 11 h away.
        assert_eq!(plan.next_in_minutes, Some(11 * 60));
    }

    #[test]
    fn default_schedule_is_dark_overnight() {
        let mut clock = ModeClock::new();
        let plan = clock.tick(m(23, 0));
        assert_eq!(plan.dispatch, Some(Theme::Dark));
        // Next boundary is 07:00, 8 h away.
        assert_eq!(plan.next_in_minutes, Some(8 * 60));
    }

    #[test]
    fn first_tick_always_dispatches_then_dedups() {
        let mut clock = ModeClock::new();
        assert_eq!(clock.tick(m(8, 0)).dispatch, Some(Theme::Light));
        // Same span → no re-dispatch.
        assert_eq!(clock.tick(m(9, 0)).dispatch, None);
        // Cross into dark → dispatch again.
        assert_eq!(clock.tick(m(20, 0)).dispatch, Some(Theme::Dark));
        assert_eq!(clock.tick(m(21, 0)).dispatch, None);
    }

    #[test]
    fn schedule_boundary_instants_are_half_open() {
        let mut clock = ModeClock::new();
        // light_start (07:00) is light; dark_start (19:00) is dark.
        assert_eq!(clock.tick(m(7, 0)).dispatch, Some(Theme::Light));
        assert_eq!(clock.tick(m(19, 0)).dispatch, Some(Theme::Dark));
    }

    #[test]
    fn fixed_dark_ignores_schedule_and_schedules_nothing() {
        let mut clock = ModeClock::new();
        assert_eq!(
            clock.on_config("config", &config_msg(serde_json::json!({ "mode": "dark" }))),
            Ok(ConfigOutcome::Accepted)
        );
        // Noon would be light under auto; fixed dark overrides, no boundary.
        let plan = clock.tick(m(12, 0));
        assert_eq!(plan.dispatch, Some(Theme::Dark));
        assert_eq!(plan.next_in_minutes, None);
    }

    #[test]
    fn fixed_light_ignores_schedule_and_schedules_nothing() {
        let mut clock = ModeClock::new();
        assert_eq!(
            clock.on_config(
                "config",
                &config_msg(serde_json::json!({ "mode": "light" }))
            ),
            Ok(ConfigOutcome::Accepted)
        );
        let plan = clock.tick(m(2, 0));
        assert_eq!(plan.dispatch, Some(Theme::Light));
        assert_eq!(plan.next_in_minutes, None);
    }

    #[test]
    fn custom_schedule_applies() {
        let mut clock = ModeClock::new();
        assert_eq!(
            clock.on_config(
                "config",
                &config_msg(serde_json::json!({
                    "mode": "auto",
                    "schedule": { "light_start": "06:30", "dark_start": "20:15" }
                }))
            ),
            Ok(ConfigOutcome::Accepted)
        );
        assert_eq!(clock.tick(m(6, 0)).dispatch, Some(Theme::Dark));
        assert_eq!(clock.tick(m(6, 30)).dispatch, Some(Theme::Light));
        assert_eq!(clock.tick(m(20, 0)).dispatch, None);
        assert_eq!(clock.tick(m(20, 15)).dispatch, Some(Theme::Dark));
    }

    #[test]
    fn midnight_wrapping_light_span() {
        let mut clock = ModeClock::new();
        // Light span 22:00 → 06:00 wraps midnight.
        assert_eq!(
            clock.on_config(
                "config",
                &config_msg(serde_json::json!({
                    "mode": "auto",
                    "schedule": { "light_start": "22:00", "dark_start": "06:00" }
                }))
            ),
            Ok(ConfigOutcome::Accepted)
        );
        assert_eq!(clock.tick(m(23, 0)).dispatch, Some(Theme::Light)); // after light_start
        assert_eq!(clock.tick(m(3, 0)).dispatch, None); // still in wrapped span
        assert_eq!(clock.tick(m(6, 0)).dispatch, Some(Theme::Dark)); // dark_start
        assert_eq!(clock.tick(m(12, 0)).dispatch, None); // daytime dark
        // Boundary from 12:00: next is 22:00, 10 h away.
        assert_eq!(clock.tick(m(12, 0)).next_in_minutes, Some(10 * 60));
    }

    #[test]
    fn boundary_landing_on_now_is_a_full_day_away() {
        let mut clock = ModeClock::new();
        // At exactly light_start 07:00 the next boundary is dark_start 19:00
        // (12 h), not 07:00 again.
        assert_eq!(clock.tick(m(7, 0)).next_in_minutes, Some(12 * 60));
    }

    #[test]
    fn omitted_schedule_resets_to_default() {
        let mut clock = ModeClock::new();
        // First set a custom schedule…
        clock
            .on_config(
                "config",
                &config_msg(serde_json::json!({
                    "mode": "auto",
                    "schedule": { "light_start": "06:00", "dark_start": "22:00" }
                })),
            )
            .unwrap();
        // …then a snapshot without schedule resets to the 07:00–19:00 default.
        clock
            .on_config("config", &config_msg(serde_json::json!({ "mode": "auto" })))
            .unwrap();
        // 06:30 is dark under the default (light_start 07:00), proving the reset.
        assert_eq!(clock.tick(m(6, 30)).dispatch, Some(Theme::Dark));
    }

    #[test]
    fn unknown_fields_are_ignored() {
        let mut clock = ModeClock::new();
        assert_eq!(
            clock.on_config(
                "config",
                &config_msg(serde_json::json!({ "mode": "light", "future_knob": 42 }))
            ),
            Ok(ConfigOutcome::Accepted)
        );
    }

    #[test]
    fn malformed_configs_keep_last_good_and_count() {
        let cases: &[serde_json::Value] = &[
            serde_json::json!({ "schedule": { "light_start": "07:00", "dark_start": "19:00" } }), // missing mode
            serde_json::json!({ "mode": "sepia" }), // unknown mode
            serde_json::json!({ "mode": "auto", "schedule": { "light_start": "7am", "dark_start": "19:00" } }), // bad time
            serde_json::json!({ "mode": "auto", "schedule": { "light_start": "25:00", "dark_start": "19:00" } }), // hour out of range
            serde_json::json!({ "mode": "auto", "schedule": { "light_start": "12:00", "dark_start": "12:00" } }), // equal boundaries
        ];
        for (i, case) in cases.iter().enumerate() {
            let mut clock = ModeClock::new();
            // Seed a known-good fixed-dark config first.
            clock
                .on_config("config", &config_msg(serde_json::json!({ "mode": "dark" })))
                .unwrap();
            let outcome = clock
                .on_config("config", &config_msg(case.clone()))
                .unwrap();
            assert!(
                matches!(outcome, ConfigOutcome::Malformed(_)),
                "case {i} should be malformed: {case}"
            );
            assert_eq!(clock.faults(), 1, "case {i} bumps the fault counter");
            // Last-good config (fixed dark) survives.
            assert_eq!(clock.tick(m(12, 0)).dispatch, Some(Theme::Dark));
        }
    }

    #[test]
    fn unparseable_envelope_is_a_contract_violation() {
        let mut clock = ModeClock::new();
        assert!(matches!(
            clock.on_config("config", "not json"),
            Err(ContractViolation::BadEnvelope(_))
        ));
    }

    #[test]
    fn wrong_port_is_a_contract_violation() {
        let mut clock = ModeClock::new();
        assert_eq!(
            clock.on_config(
                "messages",
                &config_msg(serde_json::json!({ "mode": "dark" }))
            ),
            Err(ContractViolation::WrongPort {
                port: "messages".to_string()
            })
        );
    }

    // ── The activation-window fold ──────────────────────────────────────────

    /// A `config` window whose first `context.len()` envelopes are retained
    /// context and the rest new.
    fn config_window(context: &[serde_json::Value], new: &[serde_json::Value]) -> PortWindow {
        let envelopes = context
            .iter()
            .chain(new.iter())
            .map(|body| brenn_surface_test_fixtures::sample_envelope(&body.to_string()))
            .collect();
        PortWindow {
            port: CONFIG.to_string(),
            envelopes,
            new_from: context.len() as u32,
            dropped: 0,
        }
    }

    #[test]
    fn a_config_window_applies_only_the_newest_and_reports_the_rest() {
        // Latest wins is the fold: a config message is a whole snapshot, so the
        // older two are already superseded when the window arrives. Applying
        // them first is two discarded state replacements, and the window itself
        // is evidence the binding's push_depth is wrong.
        let mut clock = ModeClock::new();
        let notes = clock
            .on_config_window(&config_window(
                &[],
                &[
                    serde_json::json!({ "mode": "light" }),
                    serde_json::json!({ "mode": "auto" }),
                    serde_json::json!({ "mode": "dark" }),
                ],
            ))
            .expect("the config port satisfies the contract");
        match notes.as_slice() {
            [ConfigNote::Misconfigured(message)] => {
                assert!(message.contains("\"config\""), "{message}");
                assert!(message.contains('3'), "{message}");
                assert!(message.contains("push_depth"), "{message}");
            }
            other => panic!("expected one misconfiguration note, got {other:?}"),
        }
        // Noon is light under `auto` and under the older `light`; only the
        // newest `dark` explains this.
        assert_eq!(clock.tick(m(12, 0)).dispatch, Some(Theme::Dark));
    }

    #[test]
    fn a_single_new_config_is_applied_silently() {
        // The healthy shape under `push_depth = 1`: retained context plus one
        // new snapshot. The context is not re-applied and nothing is reported.
        let mut clock = ModeClock::new();
        let notes = clock
            .on_config_window(&config_window(
                &[serde_json::json!({ "mode": "dark" })],
                &[serde_json::json!({ "mode": "light" })],
            ))
            .expect("the config port satisfies the contract");
        assert_eq!(notes, Vec::new());
        assert_eq!(clock.tick(m(23, 0)).dispatch, Some(Theme::Light));
    }

    #[test]
    fn a_pure_context_config_window_changes_nothing() {
        let mut clock = ModeClock::new();
        let notes = clock
            .on_config_window(&config_window(
                &[serde_json::json!({ "mode": "dark" })],
                &[],
            ))
            .expect("the config port satisfies the contract");
        assert_eq!(notes, Vec::new());
        // Default `auto` still holds: the context config was never applied.
        assert_eq!(clock.tick(m(12, 0)).dispatch, Some(Theme::Light));
    }

    #[test]
    fn a_malformed_newest_config_keeps_last_good_over_a_valid_older_one() {
        // Take-latest applies the newest and only the newest. An older valid
        // snapshot in the same window is not a fallback: presenting a
        // superseded config as current is worse than keeping last-good.
        let mut clock = ModeClock::new();
        clock
            .on_config("config", &config_msg(serde_json::json!({ "mode": "dark" })))
            .unwrap();
        let notes = clock
            .on_config_window(&config_window(
                &[],
                &[
                    serde_json::json!({ "mode": "light" }),
                    serde_json::json!({ "mode": "sepia" }),
                ],
            ))
            .expect("the config port satisfies the contract");
        assert!(
            matches!(
                notes.as_slice(),
                [ConfigNote::Misconfigured(_), ConfigNote::Malformed(_)]
            ),
            "{notes:?}"
        );
        assert_eq!(clock.faults(), 1);
        // Fixed dark from before the window, not the older `light` row.
        assert_eq!(clock.tick(m(12, 0)).dispatch, Some(Theme::Dark));
    }

    #[test]
    fn a_dropped_config_is_not_reported() {
        // Coalescing on a retained latest-wins channel is the subscription
        // doing its job, not a degradation: the superseded config that passed
        // the position unserved is exactly the one this window supersedes too.
        let mut clock = ModeClock::new();
        let mut window = config_window(&[], &[serde_json::json!({ "mode": "dark" })]);
        window.dropped = 7;
        let notes = clock
            .on_config_window(&window)
            .expect("the config port satisfies the contract");
        assert_eq!(notes, Vec::new());
    }

    #[test]
    fn a_window_on_a_wrong_port_is_a_contract_violation() {
        let mut clock = ModeClock::new();
        let mut window = config_window(&[], &[serde_json::json!({ "mode": "dark" })]);
        window.port = "messages".to_string();
        assert_eq!(
            clock.on_config_window(&window),
            Err(ContractViolation::WrongPort {
                port: "messages".to_string()
            })
        );
    }

    #[test]
    fn a_wrong_port_window_carrying_nothing_new_is_still_a_contract_violation() {
        // The port is the contract, not the traffic on it. A window routed here
        // from a port this component does not bind is skew whichever turn it
        // lands on, and most turns are idle ones: a check that only fires when
        // the window happens to carry a new message is a check that lets the
        // skew through until it does.
        let mut clock = ModeClock::new();
        let mut window = config_window(&[serde_json::json!({ "mode": "dark" })], &[]);
        window.port = "messages".to_string();
        assert_eq!(
            clock.on_config_window(&window),
            Err(ContractViolation::WrongPort {
                port: "messages".to_string()
            })
        );
    }

    #[test]
    fn retained_replay_converges_with_a_single_dispatch() {
        // Reconnect: the retained config replays, then a tick computes. One
        // dispatch, no re-dispatch on a following unchanged tick.
        let mut clock = ModeClock::new();
        clock
            .on_config(
                "config",
                &config_msg(serde_json::json!({ "mode": "light" })),
            )
            .unwrap();
        assert_eq!(clock.tick(m(3, 0)).dispatch, Some(Theme::Light));
        assert_eq!(clock.tick(m(4, 0)).dispatch, None);
    }

    /// The release instant a plan resolves to is the boundary itself: the current
    /// minute floored, plus the plan's whole minutes. A recompute part-way through
    /// a minute must not land the wake that many minutes *later* than the boundary.
    #[test]
    fn a_plan_releases_at_the_boundary_not_the_offset() {
        // 00:00:37.500 UTC, 3 minutes to the boundary → 00:03:00.000 exactly.
        let plan = TickPlan {
            dispatch: None,
            next_in_minutes: Some(3),
        };
        assert_eq!(plan.release_at(37_500), Some(180_000));
        // On the minute already: no rounding to undo.
        assert_eq!(plan.release_at(60_000), Some(240_000));
    }

    /// Two properties the ticker chain depends on: a plan with a boundary always
    /// schedules strictly in the future — so a recompute at a boundary schedules
    /// the *following* one instead of busy-firing at the instant it woke — and a
    /// fixed mode schedules nothing at all.
    #[test]
    fn a_release_is_always_future_and_a_fixed_mode_has_none() {
        let now_ms = 1_754_000_123_456;
        for minutes in [1, 2, 30, MINUTES_PER_DAY] {
            let plan = TickPlan {
                dispatch: None,
                next_in_minutes: Some(minutes),
            };
            let release = plan.release_at(now_ms).expect("a boundary schedules");
            assert!(release > now_ms, "{minutes} minutes ahead of {now_ms}");
        }
        assert_eq!(
            TickPlan {
                dispatch: None,
                next_in_minutes: None,
            }
            .release_at(now_ms),
            None
        );
    }

    /// A boundary beyond the horizon is parked at the horizon, so the chain
    /// re-derives its aim from a fresh local reading rather than trusting a count
    /// of local minutes to also be a count of elapsed milliseconds. Across a DST
    /// transition those differ by the whole shift, and a ten-hour park would land
    /// an hour off the boundary it was aimed at.
    #[test]
    fn a_distant_boundary_parks_at_the_horizon() {
        let now_ms = 1_754_000_100_000;
        let far = TickPlan {
            dispatch: None,
            next_in_minutes: Some(10 * 60),
        };
        assert_eq!(
            far.release_at(now_ms),
            Some(now_ms + u64::from(MAX_PARK_MINUTES) * MS_PER_MINUTE)
        );
        // At the horizon exactly, and one minute inside it, the boundary is still
        // the target: the clamp only ever pulls a wake nearer.
        for minutes in [MAX_PARK_MINUTES - 1, MAX_PARK_MINUTES] {
            let plan = TickPlan {
                dispatch: None,
                next_in_minutes: Some(minutes),
            };
            assert_eq!(
                plan.release_at(now_ms),
                Some(now_ms + u64::from(minutes) * MS_PER_MINUTE)
            );
        }
    }
}
