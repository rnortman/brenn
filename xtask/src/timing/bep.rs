//! Bazel's build event file (`--build_event_json_file`), read for the
//! invocation's own numbers.
//!
//! One JSON event per line. Only `started`, `buildFinished`, `buildMetrics`,
//! `testResult` and `progress` are read, and in them only the fields listed
//! on [`Bep`]; every other event and field is ignored, because Bazel adds them
//! freely. The three events `started`, `buildFinished` and `buildMetrics` must
//! be present, with the fields that are never a default: `started.command`,
//! `started.buildToolVersion`, `started.startTime`,
//! `finished.exitCode.name` and `timingMetrics.wallTimeInMs`. Every other field
//! read is a count, a duration or a flag, and the file is proto3 JSON, which
//! omits a field holding its default: absent means zero. A field Bazel renames
//! therefore reads as zero rather than failing here; the fixture tests, which
//! hold a non-zero value for each, are what catch that.
//!
//! The file also carries the invocation's whole command line and environment.
//! Nothing here keeps those events.

use std::collections::BTreeMap;

use serde_json::{Map, Value};

/// The phrase in Bazel's stderr that says the analysis cache was discarded. No
/// structured field says it.
const DISCARD_PHRASE: &str = "discarding analysis cache";

/// One test result event: one attempt of one shard of one test.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TestResult {
    pub label: String,
    pub status: String,
    /// Served from Bazel's local action cache.
    pub cached_locally: bool,
    /// Served from a cache service (the disk cache counts as one).
    pub cached_remotely: bool,
    /// The attempt's duration. For a cached result this is the original run's.
    pub duration_ms: u64,
}

/// What one build event file says about its invocation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Bep {
    /// `build`, `test`, `run`, `cquery`, ...
    pub command: String,
    pub tool_version: String,
    pub start_time: String,
    /// `SUCCESS`, `BUILD_FAILURE`, ...
    pub exit_name: String,
    pub wall_ms: u64,
    pub analysis_ms: u64,
    pub execution_ms: u64,
    pub targets_configured: u64,
    pub actions_executed: u64,
    /// `actionSummary.runnerCount`: count per runner name, including Bazel's
    /// own `total` and `internal` rows.
    pub runner_counts: BTreeMap<String, u64>,
    pub action_cache_hits: u64,
    pub action_cache_misses: u64,
    pub tests: Vec<TestResult>,
    /// Each stderr line reporting an analysis-cache discard, as Bazel wrote it
    /// less terminal colour codes.
    pub discards: Vec<String>,
}

/// Why a build event file could not be read.
#[derive(Debug, PartialEq, Eq)]
pub enum BepError {
    /// The file stops early: its last line is cut off, or one of the three
    /// required events never came. What a failed or interrupted command can
    /// leave.
    Incomplete(String),
    /// The file is complete but not what Bazel writes.
    Malformed(String),
}

impl std::fmt::Display for BepError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BepError::Incomplete(m) => write!(f, "incomplete build event file: {m}"),
            BepError::Malformed(m) => write!(f, "malformed build event file: {m}"),
        }
    }
}

/// Parse a build event file's text.
pub fn parse(text: &str) -> Result<Bep, BepError> {
    let body = match text.strip_suffix('\n') {
        Some(body) => body,
        None if text.is_empty() => return Err(BepError::Incomplete("the file is empty".into())),
        None => return Err(BepError::Incomplete("the last line is cut off".into())),
    };

    let mut started: Option<Map<String, Value>> = None;
    let mut finished: Option<Map<String, Value>> = None;
    let mut metrics: Option<Map<String, Value>> = None;
    let mut tests = Vec::new();
    let mut discards = Vec::new();

    for (n, line) in body.split('\n').enumerate() {
        let at = |m: String| BepError::Malformed(format!("line {}: {m}", n + 1));
        let event: Value = serde_json::from_str(line).map_err(|e| at(format!("not JSON: {e}")))?;
        let id = event
            .get("id")
            .and_then(Value::as_object)
            .ok_or_else(|| at("no event id".into()))?;
        let payload = |key: &str| -> Result<Map<String, Value>, BepError> {
            match event.get(key) {
                Some(Value::Object(m)) => Ok(m.clone()),
                // An event whose every field is a default has no payload.
                None => Ok(Map::new()),
                Some(_) => Err(at(format!("{key} is not an object"))),
            }
        };
        if id.contains_key("started") {
            once(&mut started, payload("started")?, "started").map_err(at)?;
        } else if id.contains_key("buildFinished") {
            once(&mut finished, payload("finished")?, "buildFinished").map_err(at)?;
        } else if id.contains_key("buildMetrics") {
            once(&mut metrics, payload("buildMetrics")?, "buildMetrics").map_err(at)?;
        } else if let Some(test_id) = id.get("testResult") {
            tests.push(test_result(test_id, &payload("testResult")?).map_err(at)?);
        } else if id.contains_key("progress") {
            let progress = payload("progress")?;
            if let Some(stderr) = progress.get("stderr") {
                let stderr = stderr
                    .as_str()
                    .ok_or_else(|| at("progress.stderr is not a string".into()))?;
                discards.extend(
                    stderr
                        .lines()
                        .map(strip_ansi)
                        .filter(|l| l.contains(DISCARD_PHRASE))
                        .map(|l| l.trim().to_string()),
                );
            }
        }
    }

    let missing = |event: &str| BepError::Incomplete(format!("no {event} event"));
    let started = started.ok_or_else(|| missing("started"))?;
    let finished = finished.ok_or_else(|| missing("buildFinished"))?;
    let metrics = metrics.ok_or_else(|| missing("buildMetrics"))?;
    let bad = BepError::Malformed;

    let summary = object(&metrics, "actionSummary").map_err(bad)?;
    let timing = object(&metrics, "timingMetrics").map_err(bad)?;
    let mut runner_counts = BTreeMap::new();
    for row in array(&summary, "runnerCount").map_err(bad)? {
        let row = row
            .as_object()
            .ok_or_else(|| bad("a runnerCount row is not an object".into()))?;
        let name = required_str(row, "name", "runnerCount.name").map_err(bad)?;
        let count = count(row, "count").map_err(bad)?;
        if runner_counts.insert(name.clone(), count).is_some() {
            return Err(bad(format!("runner {name:?} is counted twice")));
        }
    }
    let action_cache = object(&summary, "actionCacheStatistics").map_err(bad)?;

    Ok(Bep {
        command: required_str(&started, "command", "started.command").map_err(bad)?,
        tool_version: required_str(&started, "buildToolVersion", "started.buildToolVersion")
            .map_err(bad)?,
        start_time: required_str(&started, "startTime", "started.startTime").map_err(bad)?,
        exit_name: required_str(
            &object(&finished, "exitCode").map_err(bad)?,
            "name",
            "finished.exitCode.name",
        )
        .map_err(bad)?,
        wall_ms: match timing.get("wallTimeInMs") {
            Some(v) => number(v, "timingMetrics.wallTimeInMs").map_err(bad)?,
            None => return Err(bad("no timingMetrics.wallTimeInMs".into())),
        },
        analysis_ms: count(&timing, "analysisPhaseTimeInMs").map_err(bad)?,
        execution_ms: count(&timing, "executionPhaseTimeInMs").map_err(bad)?,
        targets_configured: count(
            &object(&metrics, "targetMetrics").map_err(bad)?,
            "targetsConfigured",
        )
        .map_err(bad)?,
        actions_executed: count(&summary, "actionsExecuted").map_err(bad)?,
        runner_counts,
        action_cache_hits: count(&action_cache, "hits").map_err(bad)?,
        action_cache_misses: count(&action_cache, "misses").map_err(bad)?,
        tests,
        discards,
    })
}

fn once(
    slot: &mut Option<Map<String, Value>>,
    value: Map<String, Value>,
    name: &str,
) -> Result<(), String> {
    if slot.replace(value).is_some() {
        return Err(format!("a second {name} event"));
    }
    Ok(())
}

fn test_result(id: &Value, payload: &Map<String, Value>) -> Result<TestResult, String> {
    let id = id
        .as_object()
        .ok_or_else(|| "testResult id is not an object".to_string())?;
    let execution = object(payload, "executionInfo")?;
    Ok(TestResult {
        label: required_str(id, "label", "testResult id label")?,
        status: required_str(payload, "status", "testResult.status")?,
        cached_locally: flag(payload, "cachedLocally")?,
        cached_remotely: flag(&execution, "cachedRemotely")?,
        duration_ms: count(payload, "testAttemptDurationMillis")?,
    })
}

/// A sub-object, empty when absent.
fn object(map: &Map<String, Value>, key: &str) -> Result<Map<String, Value>, String> {
    match map.get(key) {
        None => Ok(Map::new()),
        Some(Value::Object(m)) => Ok(m.clone()),
        Some(_) => Err(format!("{key} is not an object")),
    }
}

fn array<'m>(map: &'m Map<String, Value>, key: &str) -> Result<&'m [Value], String> {
    match map.get(key) {
        None => Ok(&[]),
        Some(Value::Array(a)) => Ok(a),
        Some(_) => Err(format!("{key} is not an array")),
    }
}

fn required_str(map: &Map<String, Value>, key: &str, name: &str) -> Result<String, String> {
    match map.get(key) {
        Some(Value::String(s)) if !s.is_empty() => Ok(s.clone()),
        Some(_) => Err(format!("{name} is not a non-empty string")),
        None => Err(format!("no {name}")),
    }
}

/// A count or duration, zero when absent.
fn count(map: &Map<String, Value>, key: &str) -> Result<u64, String> {
    map.get(key).map_or(Ok(0), |v| number(v, key))
}

fn flag(map: &Map<String, Value>, key: &str) -> Result<bool, String> {
    match map.get(key) {
        None => Ok(false),
        Some(Value::Bool(b)) => Ok(*b),
        Some(_) => Err(format!("{key} is not a boolean")),
    }
}

/// A non-negative integer. Proto3 JSON writes 64-bit integers as strings and
/// 32-bit ones as numbers; either is accepted wherever the reader reads one.
fn number(value: &Value, name: &str) -> Result<u64, String> {
    match value {
        Value::Number(n) => n.as_u64(),
        Value::String(s) if !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit()) => {
            s.parse().ok()
        }
        _ => None,
    }
    .ok_or_else(|| format!("{name} {value} is not a non-negative integer"))
}

/// `line` without ANSI escape sequences (`ESC [ ... final byte`), which Bazel
/// writes into stderr when colour is on.
fn strip_ansi(line: &str) -> String {
    let mut out = String::with_capacity(line.len());
    let mut chars = line.chars();
    while let Some(c) = chars.next() {
        if c == '\u{1b}' {
            if chars.next() == Some('[') {
                for c in chars.by_ref() {
                    if ('@'..='~').contains(&c) {
                        break;
                    }
                }
            }
        } else {
            out.push(c);
        }
    }
    out
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    /// The three required events and nothing else, every count left absent.
    pub(crate) fn minimal(command: &str) -> String {
        format!(
            concat!(
                r#"{{"id":{{"started":{{}}}},"started":{{"command":"{}","buildToolVersion":"9.2.0","startTime":"2026-11-22T10:00:00Z"}}}}"#,
                "\n",
                r#"{{"id":{{"buildFinished":{{}}}},"finished":{{"exitCode":{{"name":"SUCCESS"}}}}}}"#,
                "\n",
                r#"{{"id":{{"buildMetrics":{{}}}},"buildMetrics":{{"actionSummary":{{"runnerCount":[{{"name":"total"}}]}},"timingMetrics":{{"wallTimeInMs":"100"}}}}}}"#,
                "\n",
            ),
            command
        )
    }

    #[test]
    fn absent_counts_read_as_zero() {
        let bep = parse(&minimal("cquery")).unwrap();
        assert_eq!(bep.command, "cquery");
        assert_eq!(bep.wall_ms, 100);
        assert_eq!(bep.execution_ms, 0);
        assert_eq!(bep.actions_executed, 0);
        assert_eq!(
            bep.runner_counts,
            BTreeMap::from([("total".to_string(), 0)])
        );
        assert!(bep.tests.is_empty() && bep.discards.is_empty());
    }

    #[test]
    fn unknown_events_and_fields_are_ignored() {
        let text = minimal("build").replace(
            r#""command":"build""#,
            r#""command":"build","somethingNew":[1,2]"#,
        ) + r#"{"id":{"unstructuredCommandLine":{}},"unstructuredCommandLine":{"args":["x"]}}"#
            + "\n";
        assert_eq!(parse(&text).unwrap().command, "build");
    }

    #[test]
    fn a_discard_line_is_found_in_coloured_stderr() {
        let text = minimal("test")
            + r#"{"id":{"progress":{"opaqueCount":1}},"progress":{"stderr":"\u001b[35mWARNING: \u001b[0mBuild option --platforms has changed, discarding analysis cache (this can be expensive).\nINFO: Analyzed 3 targets\n"}}"#
            + "\n";
        assert_eq!(
            parse(&text).unwrap().discards,
            [
                "WARNING: Build option --platforms has changed, discarding analysis cache (this can be expensive)."
            ]
        );
    }

    #[test]
    fn a_missing_required_event_is_incomplete() {
        let text: String = minimal("build")
            .lines()
            .filter(|l| !l.contains("buildMetrics"))
            .map(|l| format!("{l}\n"))
            .collect();
        assert_eq!(
            parse(&text),
            Err(BepError::Incomplete("no buildMetrics event".into()))
        );
    }

    #[test]
    fn a_cut_off_last_line_or_empty_file_is_incomplete() {
        let text = minimal("build");
        assert!(matches!(
            parse(&text[..text.len() - 10]),
            Err(BepError::Incomplete(_))
        ));
        assert!(matches!(parse(""), Err(BepError::Incomplete(_))));
    }

    #[test]
    fn a_missing_always_present_field_is_malformed() {
        for (from, to) in [
            (r#""command":"build","#, ""),
            (r#""buildToolVersion":"9.2.0","#, ""),
            (r#","startTime":"2026-11-22T10:00:00Z""#, ""),
            (r#""name":"SUCCESS""#, ""),
            (r#","timingMetrics":{"wallTimeInMs":"100"}"#, ""),
        ] {
            let text = minimal("build").replace(from, to);
            assert_ne!(text, minimal("build"), "{from}");
            assert!(
                matches!(parse(&text), Err(BepError::Malformed(_))),
                "{from}: {:?}",
                parse(&text)
            );
        }
    }

    #[test]
    fn a_line_that_is_not_json_or_a_duplicate_event_is_malformed() {
        let not_json = minimal("build") + "{oops\n";
        assert!(matches!(parse(&not_json), Err(BepError::Malformed(m)) if m.starts_with("line 4")));
        let first = minimal("build").lines().next().unwrap().to_string();
        let twice = minimal("build") + &first + "\n";
        assert!(
            matches!(parse(&twice), Err(BepError::Malformed(m)) if m.contains("second started"))
        );
    }

    #[test]
    fn a_count_that_is_not_a_non_negative_integer_is_malformed() {
        for bad in [r#""-1""#, r#""1.5""#, "true", r#""""#] {
            let text = minimal("build").replace(r#""100""#, bad);
            assert!(matches!(parse(&text), Err(BepError::Malformed(_))), "{bad}");
        }
        let numeric = minimal("build").replace(r#""100""#, "100");
        assert_eq!(parse(&numeric).unwrap().wall_ms, 100);
    }

    #[test]
    fn test_results_carry_label_status_cache_and_duration() {
        let text = minimal("test")
            + r#"{"id":{"testResult":{"label":"//:t","run":1}},"testResult":{"status":"PASSED","cachedLocally":true,"testAttemptDurationMillis":"420","executionInfo":{"cachedRemotely":true}}}"#
            + "\n";
        assert_eq!(
            parse(&text).unwrap().tests,
            [TestResult {
                label: "//:t".into(),
                status: "PASSED".into(),
                cached_locally: true,
                cached_remotely: true,
                duration_ms: 420,
            }]
        );
    }
}
