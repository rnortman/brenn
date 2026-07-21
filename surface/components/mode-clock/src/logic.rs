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

use brenn_surface_proto::{THEME_DARK, THEME_LIGHT};
use serde::Deserialize;

use brenn_surface_component_support::parse_delivery;
pub use brenn_surface_component_support::{ContractViolation, FaultReport};

/// The config-bound input port name. A `[[surface.subscription]] port` must
/// match this string, or [`ModeClock::on_config`] rejects the delivery.
const CONFIG_PORT: &str = "config";

/// Minutes in a wall-clock day. Membership and boundary math are done in
/// minutes-since-local-midnight, so no timezone arithmetic is ever needed.
const MINUTES_PER_DAY: u16 = 24 * 60;

/// The default auto schedule: light 07:00, dark 19:00 — the product default for
/// a fresh install with no retained config.
const DEFAULT_LIGHT_START: u16 = 7 * 60;
const DEFAULT_DARK_START: u16 = 19 * 60;

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
enum Mode {
    /// Day/night switching by [`Schedule`].
    Auto,
    /// Always dark, timer cancelled.
    Dark,
    /// Always light, timer cancelled.
    Light,
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

/// The result of a recompute: what to dispatch now and when to wake next.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TickPlan {
    /// The theme to dispatch, or `None` when it is unchanged since the last
    /// dispatch. The first recompute always dispatches (so the shell converges
    /// from the page default).
    pub dispatch: Option<Theme>,
    /// Minutes until the next scheduled recompute, or `None` to cancel the timer
    /// (fixed dark/light modes have no boundary).
    pub next_in_minutes: Option<u16>,
}

/// Raw config body as serde sees it. Unknown fields are ignored (no
/// `deny_unknown_fields`): this is a de-facto external contract that evolves
/// additively. `mode` is required; `schedule` is optional (absent → default).
#[derive(Deserialize)]
struct RawConfig {
    mode: String,
    #[serde(default)]
    schedule: Option<RawSchedule>,
}

/// Raw schedule times as `HH:MM` strings, validated explicitly so an unparseable
/// time produces a precise malformed reason rather than a generic serde error.
#[derive(Deserialize)]
struct RawSchedule {
    light_start: String,
    dark_start: String,
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
        let envelope = parse_delivery(port, &[CONFIG_PORT], envelope_json)?;
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
    let mode = match raw.mode.as_str() {
        "auto" => Mode::Auto,
        "dark" => Mode::Dark,
        "light" => Mode::Light,
        other => return Err(format!("unknown mode {other:?}")),
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

    /// The wire values a dispatched theme carries into the `ThemeBody` the wasm
    /// glue publishes on `local:brenn/theme` — the exact strings chrome parses.
    #[test]
    fn theme_wire_values_match_the_control_plane_body() {
        use brenn_surface_proto::{CONTROL_PLANE_VERSION, ThemeBody};
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
    fn fixed_dark_ignores_schedule_and_cancels_timer() {
        let mut clock = ModeClock::new();
        assert_eq!(
            clock.on_config("config", &config_msg(serde_json::json!({ "mode": "dark" }))),
            Ok(ConfigOutcome::Accepted)
        );
        // Noon would be light under auto; fixed dark overrides, no timer.
        let plan = clock.tick(m(12, 0));
        assert_eq!(plan.dispatch, Some(Theme::Dark));
        assert_eq!(plan.next_in_minutes, None);
    }

    #[test]
    fn fixed_light_ignores_schedule_and_cancels_timer() {
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
}
