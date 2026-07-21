// Test fixture for the brenn:processor log/alert capability.
//
// Each new envelope is a standard MessageEnvelope JSON. The `body` field is
// itself a JSON object (serialised as a string) with a `cmd` key:
//
//   {"cmd":"log", "level":"trace"|"debug"|"info"|"warn"|"error", "message":"..."}
//     Emit one log line at the given level (defaults: level="info", message="").
//
//   {"cmd":"alert", "severity":"info"|"warning"|"critical", "title":"...", "body":"..."}
//     Emit one alert (defaults: severity="warning", title="", body="").
//
//   {"cmd":"log_n", "n":300, "level":"info", "message":"..."}
//     Emit N log calls in a loop.
//
//   {"cmd":"alert_n", "n":10, "severity":"warning", "title":"...", "body":"..."}
//     Emit N alert calls in a loop.
//
//   {"cmd":"trap"}
//     Trap unconditionally via unreachable!.
//
//   {"cmd":"err", "message":"..."}
//     Return Err(ProcessingFailed) with the given message.
//
//   {"cmd":"ok"} or unrecognised body
//     No-op; contributes to overall Ok.
//
// All new envelopes are processed in order; log/alert calls are immediate
// (not buffered, survive a later trap/err — by host design, not this crate).

use brenn_guest::log::Level;
use brenn_guest::alert::Severity;
use brenn_guest::{Activation, Error, MessageEnvelopeExt, Processor};
use brenn_guest::{log, alert};

struct ProcessorLog;

fn parse_level(s: &str) -> Level {
    match s {
        "trace" => Level::Trace,
        "debug" => Level::Debug,
        "warn" => Level::Warn,
        "error" => Level::Error,
        _ => Level::Info,
    }
}

fn parse_severity(s: &str) -> Severity {
    match s {
        "info" => Severity::Info,
        "critical" => Severity::Critical,
        _ => Severity::Warning,
    }
}

fn execute_directive(dir: &serde_json::Value) -> Result<(), Error> {
    let cmd = dir.get("cmd").and_then(|v| v.as_str()).unwrap_or("ok");
    match cmd {
        "log" => {
            let level =
                parse_level(dir.get("level").and_then(|v| v.as_str()).unwrap_or("info"));
            let message = dir.get("message").and_then(|v| v.as_str()).unwrap_or("");
            log::log(level, message);
        }
        "alert" => {
            let severity =
                parse_severity(dir.get("severity").and_then(|v| v.as_str()).unwrap_or("warning"));
            let title = dir.get("title").and_then(|v| v.as_str()).unwrap_or("");
            let body_str = dir.get("body").and_then(|v| v.as_str()).unwrap_or("");
            alert::alert(severity, title, body_str);
        }
        "log_n" => {
            let n = dir.get("n").and_then(|v| v.as_u64()).unwrap_or(1) as usize;
            let level =
                parse_level(dir.get("level").and_then(|v| v.as_str()).unwrap_or("info"));
            let message = dir.get("message").and_then(|v| v.as_str()).unwrap_or("");
            for _ in 0..n {
                log::log(level, message);
            }
        }
        "alert_n" => {
            let n = dir.get("n").and_then(|v| v.as_u64()).unwrap_or(1) as usize;
            let severity =
                parse_severity(dir.get("severity").and_then(|v| v.as_str()).unwrap_or("warning"));
            let title = dir.get("title").and_then(|v| v.as_str()).unwrap_or("");
            let body_str = dir.get("body").and_then(|v| v.as_str()).unwrap_or("");
            for _ in 0..n {
                alert::alert(severity, title, body_str);
            }
        }
        "trap" => {
            unreachable!("processor-log: deliberate trap on 'trap' directive");
        }
        "err" => {
            let message = dir.get("message").and_then(|v| v.as_str()).unwrap_or("error");
            return Err(Error::failed(message));
        }
        _ => {}
    }
    Ok(())
}

impl Processor for ProcessorLog {
    fn receive(activation: Activation) -> Result<(), Error> {
        for window in activation.port_windows() {
            for env in window.new_envelopes() {
                let env = env?;
                // Parse the body as a directive JSON object.
                let dir: serde_json::Value = env.json_body()?;
                execute_directive(&dir)?;
            }
        }
        Ok(())
    }
}

brenn_guest::export_processor!(ProcessorLog);
