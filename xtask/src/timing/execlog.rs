//! Bazel's compact execution log (`--execution_log_compact_file`), read for its
//! spawns.
//!
//! The file is one zstd frame holding a sequence of `ExecLogEntry` protobuf
//! messages, each preceded by its length as a varint
//! (`src/main/protobuf/spawn.proto` in the Bazel source). Only the `spawn`
//! entries are read, and only the handful of fields the reader reports; every
//! other entry and field is skipped by wire type. There is no protobuf crate
//! and no generated code: a field walker over a few numbered fields is all the
//! format needs.
//!
//! The field numbers are those of Bazel 9.2.0's `spawn.proto`. A Bazel upgrade
//! fails the fixture version test until the fixtures are recorded again, which
//! is when a renumbering would show.

use std::io::Read;

/// `ExecLogEntry.spawn`.
const ENTRY_SPAWN: u32 = 7;

/// Fields of `ExecLogEntry.Spawn`.
const SPAWN_TARGET_LABEL: u32 = 7;
const SPAWN_MNEMONIC: u32 = 8;
const SPAWN_EXIT_CODE: u32 = 9;
const SPAWN_RUNNER: u32 = 11;
const SPAWN_CACHE_HIT: u32 = 12;
const SPAWN_METRICS: u32 = 18;

/// Fields of `SpawnMetrics`.
const METRICS_TOTAL_TIME: u32 = 1;

/// Fields of `google.protobuf.Duration`.
const SECONDS: u32 = 1;
const NANOS: u32 = 2;

/// One spawn, executed or served from a cache. Absent fields hold protobuf's
/// default, which is what an absent field means.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Spawn {
    /// The target the spawn's action belongs to; for an aspect's action, the
    /// target the aspect is applied to.
    pub target_label: String,
    pub mnemonic: String,
    /// The strategy that ran it, or the cache that served it, in Bazel's words.
    pub runner: String,
    pub cache_hit: bool,
    pub exit_code: i32,
    /// `SpawnMetrics.total_time`. For a cache hit this is the time the hit
    /// took, not the original execution's.
    pub total_ns: u64,
}

impl Spawn {
    /// Run rather than served from a cache.
    pub fn executed(&self) -> bool {
        !self.cache_hit
    }
}

/// Why an execution log could not be read.
#[derive(Debug, PartialEq, Eq)]
pub enum ExecLogError {
    /// The file ends before its last entry does, or the zstd frame does not
    /// decode: what a command interrupted while writing it leaves.
    Incomplete(String),
    /// The file decodes but its content is not what Bazel writes.
    Malformed(String),
}

impl std::fmt::Display for ExecLogError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExecLogError::Incomplete(m) => write!(f, "incomplete execution log: {m}"),
            ExecLogError::Malformed(m) => write!(f, "malformed execution log: {m}"),
        }
    }
}

/// Every spawn in a compact execution log, in the order Bazel wrote them.
pub fn parse(compressed: &[u8]) -> Result<Vec<Spawn>, ExecLogError> {
    let mut input = compressed;
    let mut decoder = ruzstd::decoding::StreamingDecoder::new(&mut input)
        .map_err(|e| ExecLogError::Incomplete(format!("zstd frame header: {e}")))?;
    let mut bytes = Vec::new();
    decoder
        .read_to_end(&mut bytes)
        .map_err(|e| ExecLogError::Incomplete(format!("zstd frame: {e}")))?;
    drop(decoder);
    if !input.is_empty() {
        return Err(ExecLogError::Malformed(format!(
            "{} bytes follow the zstd frame",
            input.len()
        )));
    }
    parse_entries(&bytes)
}

/// The spawns among length-delimited `ExecLogEntry` messages.
fn parse_entries(mut bytes: &[u8]) -> Result<Vec<Spawn>, ExecLogError> {
    let mut spawns = Vec::new();
    while !bytes.is_empty() {
        let len = read_varint(&mut bytes).map_err(ExecLogError::Incomplete)?;
        let len = usize::try_from(len)
            .map_err(|_| ExecLogError::Malformed(format!("entry length {len}")))?;
        if len > bytes.len() {
            return Err(ExecLogError::Incomplete(format!(
                "an entry of {len} bytes with {} left",
                bytes.len()
            )));
        }
        let (entry, rest) = bytes.split_at(len);
        bytes = rest;
        let mut spawn = None;
        walk(entry, |field, value| {
            if field == ENTRY_SPAWN {
                let body = value.bytes("ExecLogEntry.spawn")?;
                if spawn.replace(parse_spawn(body)?).is_some() {
                    return Err("ExecLogEntry.spawn appears twice".to_string());
                }
            }
            Ok(())
        })
        .map_err(ExecLogError::Malformed)?;
        spawns.extend(spawn);
    }
    Ok(spawns)
}

fn parse_spawn(body: &[u8]) -> Result<Spawn, String> {
    let mut seen = Vec::new();
    let mut spawn = Spawn {
        target_label: String::new(),
        mnemonic: String::new(),
        runner: String::new(),
        cache_hit: false,
        exit_code: 0,
        total_ns: 0,
    };
    walk(body, |field, value| {
        let read = [
            SPAWN_TARGET_LABEL,
            SPAWN_MNEMONIC,
            SPAWN_EXIT_CODE,
            SPAWN_RUNNER,
            SPAWN_CACHE_HIT,
            SPAWN_METRICS,
        ];
        if !read.contains(&field) {
            return Ok(());
        }
        if seen.contains(&field) {
            return Err(format!("Spawn field {field} appears twice"));
        }
        seen.push(field);
        match field {
            SPAWN_TARGET_LABEL => spawn.target_label = value.string("Spawn.target_label")?,
            SPAWN_MNEMONIC => spawn.mnemonic = value.string("Spawn.mnemonic")?,
            SPAWN_RUNNER => spawn.runner = value.string("Spawn.runner")?,
            SPAWN_CACHE_HIT => spawn.cache_hit = value.varint("Spawn.cache_hit")? != 0,
            SPAWN_EXIT_CODE => {
                // An int32 is written as the varint of its sign extension.
                let raw = value.varint("Spawn.exit_code")? as i64;
                spawn.exit_code = i32::try_from(raw)
                    .map_err(|_| format!("Spawn.exit_code {raw} is not an int32"))?;
            }
            SPAWN_METRICS => {
                spawn.total_ns = parse_total_time(value.bytes("Spawn.metrics")?)?;
            }
            _ => unreachable!("only the listed fields get here"),
        }
        Ok(())
    })?;
    Ok(spawn)
}

/// The `total_time` of a `SpawnMetrics`, in nanoseconds; zero when absent.
fn parse_total_time(body: &[u8]) -> Result<u64, String> {
    const NAME: &str = "SpawnMetrics.total_time";
    let mut total = None;
    walk(body, |field, value| {
        if field != METRICS_TOTAL_TIME {
            return Ok(());
        }
        let ns = parse_seconds_nanos(value.bytes(NAME)?, NAME)?;
        if total.replace(ns).is_some() {
            return Err(format!("{NAME} appears twice"));
        }
        Ok(())
    })?;
    Ok(total.unwrap_or(0))
}

/// A `google.protobuf.Duration` as nanoseconds: `{int64 seconds = 1; int32
/// nanos = 2;}`, never negative in a spawn's metrics.
fn parse_seconds_nanos(body: &[u8], name: &str) -> Result<u64, String> {
    let mut seconds = 0u64;
    let mut nanos = 0u64;
    walk(body, |field, value| {
        match field {
            SECONDS => seconds = value.varint(name)?,
            NANOS => nanos = value.varint(name)?,
            _ => {}
        }
        Ok(())
    })?;
    if seconds > i64::MAX as u64 || nanos >= 1_000_000_000 {
        return Err(format!("{name} holds {seconds} s and {nanos} ns"));
    }
    seconds
        .checked_mul(1_000_000_000)
        .and_then(|s| s.checked_add(nanos))
        .ok_or_else(|| format!("{name} of {seconds} s does not fit in nanoseconds"))
}

/// A field's payload by wire type.
#[derive(Debug, Clone, Copy)]
enum Value<'b> {
    Varint(u64),
    Fixed64,
    Bytes(&'b [u8]),
    Fixed32,
}

impl<'b> Value<'b> {
    fn varint(self, name: &str) -> Result<u64, String> {
        match self {
            Value::Varint(v) => Ok(v),
            other => Err(format!("{name} is {other:?}, not a varint")),
        }
    }

    fn bytes(self, name: &str) -> Result<&'b [u8], String> {
        match self {
            Value::Bytes(b) => Ok(b),
            other => Err(format!("{name} is {other:?}, not length-delimited")),
        }
    }

    fn string(self, name: &str) -> Result<String, String> {
        String::from_utf8(self.bytes(name)?.to_vec()).map_err(|_| format!("{name} is not UTF-8"))
    }
}

/// Call `visit` with each field of one protobuf message, in order.
fn walk<'b>(
    mut body: &'b [u8],
    mut visit: impl FnMut(u32, Value<'b>) -> Result<(), String>,
) -> Result<(), String> {
    while !body.is_empty() {
        let key = read_varint(&mut body)?;
        let field = u32::try_from(key >> 3)
            .ok()
            .filter(|f| *f != 0)
            .ok_or_else(|| format!("field number {}", key >> 3))?;
        let value = match key & 7 {
            0 => Value::Varint(read_varint(&mut body)?),
            1 => {
                take(&mut body, 8)?;
                Value::Fixed64
            }
            2 => {
                let len = read_varint(&mut body)?;
                let len = usize::try_from(len).map_err(|_| format!("length {len}"))?;
                Value::Bytes(take(&mut body, len)?)
            }
            5 => {
                take(&mut body, 4)?;
                Value::Fixed32
            }
            wire => return Err(format!("field {field} has wire type {wire}")),
        };
        visit(field, value)?;
    }
    Ok(())
}

fn take<'b>(body: &mut &'b [u8], len: usize) -> Result<&'b [u8], String> {
    if len > body.len() {
        return Err(format!("{len} bytes wanted, {} left", body.len()));
    }
    let (head, rest) = body.split_at(len);
    *body = rest;
    Ok(head)
}

fn read_varint(body: &mut &[u8]) -> Result<u64, String> {
    let mut value = 0u64;
    for i in 0..10 {
        let Some((&byte, rest)) = body.split_first() else {
            return Err("the input ends inside a varint".to_string());
        };
        *body = rest;
        if i == 9 && byte > 1 {
            return Err("a varint longer than 64 bits".to_string());
        }
        value |= u64::from(byte & 0x7f) << (7 * i);
        if byte & 0x80 == 0 {
            return Ok(value);
        }
    }
    Err("a varint longer than 10 bytes".to_string())
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    fn varint(mut v: u64, out: &mut Vec<u8>) {
        loop {
            let byte = (v & 0x7f) as u8;
            v >>= 7;
            if v == 0 {
                out.push(byte);
                return;
            }
            out.push(byte | 0x80);
        }
    }

    fn field_varint(field: u32, v: u64, out: &mut Vec<u8>) {
        varint(u64::from(field) << 3, out);
        varint(v, out);
    }

    fn field_bytes(field: u32, body: &[u8], out: &mut Vec<u8>) {
        varint((u64::from(field) << 3) | 2, out);
        varint(body.len() as u64, out);
        out.extend_from_slice(body);
    }

    fn duration(seconds: u64, nanos: u64) -> Vec<u8> {
        let mut out = Vec::new();
        field_varint(SECONDS, seconds, &mut out);
        field_varint(NANOS, nanos, &mut out);
        out
    }

    /// One encoded `ExecLogEntry` holding a spawn with the given fields, plus an
    /// unread string field, an unread fixed64 field and two unread
    /// `SpawnMetrics` fields (`execution_wall_time`, `start_time`) for the
    /// walker to skip.
    pub(crate) fn spawn_entry(label: &str, runner: &str, hit: bool, exit: i32, ms: u64) -> Vec<u8> {
        let mut metrics = Vec::new();
        field_bytes(
            METRICS_TOTAL_TIME,
            &duration(ms / 1000, (ms % 1000) * 1_000_000),
            &mut metrics,
        );
        field_bytes(8, &duration(0, 5), &mut metrics);
        field_bytes(20, &duration(1_795_406_635, 7), &mut metrics);
        let mut spawn = Vec::new();
        field_bytes(1, b"/bin/bash", &mut spawn);
        field_bytes(SPAWN_TARGET_LABEL, label.as_bytes(), &mut spawn);
        field_bytes(SPAWN_MNEMONIC, b"Genrule", &mut spawn);
        field_varint(SPAWN_EXIT_CODE, exit as i64 as u64, &mut spawn);
        field_bytes(SPAWN_RUNNER, runner.as_bytes(), &mut spawn);
        field_varint(SPAWN_CACHE_HIT, u64::from(hit), &mut spawn);
        varint((17 << 3) | 1, &mut spawn);
        spawn.extend_from_slice(&[0; 8]);
        field_bytes(SPAWN_METRICS, &metrics, &mut spawn);
        let mut entry = Vec::new();
        field_varint(1, 42, &mut entry);
        field_bytes(ENTRY_SPAWN, &spawn, &mut entry);
        entry
    }

    /// Length-delimited entries in one zstd frame, as Bazel writes the file.
    pub(crate) fn exec_log(entries: &[Vec<u8>]) -> Vec<u8> {
        let mut raw = Vec::new();
        for entry in entries {
            varint(entry.len() as u64, &mut raw);
            raw.extend_from_slice(entry);
        }
        ruzstd::encoding::compress_to_vec(
            raw.as_slice(),
            ruzstd::encoding::CompressionLevel::Fastest,
        )
    }

    #[test]
    fn spawns_are_read_and_other_entries_skipped() {
        let mut file_entry = Vec::new();
        field_varint(1, 3, &mut file_entry);
        field_bytes(3, b"\x0a\x05a.txt", &mut file_entry);
        let log = exec_log(&[
            file_entry,
            spawn_entry("//:slow", "linux-sandbox", false, 0, 1_250),
            spawn_entry("//:fails", "disk cache hit", true, -1, 3),
        ]);
        let spawns = parse(&log).unwrap();
        assert_eq!(
            spawns[0],
            Spawn {
                target_label: "//:slow".into(),
                mnemonic: "Genrule".into(),
                runner: "linux-sandbox".into(),
                cache_hit: false,
                exit_code: 0,
                total_ns: 1_250_000_000,
            }
        );
        assert_eq!(spawns[1].exit_code, -1);
        assert!(spawns[1].cache_hit);
        assert_eq!(spawns.len(), 2);
    }

    #[test]
    fn a_spawn_without_metrics_reads_as_zero_times() {
        let mut spawn = Vec::new();
        field_bytes(SPAWN_MNEMONIC, b"Rustc", &mut spawn);
        let mut entry = Vec::new();
        field_bytes(ENTRY_SPAWN, &spawn, &mut entry);
        let spawns = parse(&exec_log(&[entry])).unwrap();
        assert_eq!(spawns[0].total_ns, 0);
        assert_eq!(spawns[0].runner, "");
    }

    #[test]
    fn an_empty_log_has_no_spawns() {
        assert_eq!(parse(&exec_log(&[])).unwrap(), Vec::new());
    }

    #[test]
    fn a_truncated_frame_or_entry_is_incomplete() {
        let log = exec_log(&[spawn_entry("//:a", "x", false, 0, 1)]);
        assert!(matches!(
            parse(&log[..log.len() - 3]),
            Err(ExecLogError::Incomplete(_))
        ));
        let entry = spawn_entry("//:a", "x", false, 0, 1);
        let mut raw = Vec::new();
        varint(entry.len() as u64, &mut raw);
        raw.extend_from_slice(&entry[..entry.len() - 1]);
        assert!(matches!(
            parse_entries(&raw),
            Err(ExecLogError::Incomplete(_))
        ));
    }

    #[test]
    fn garbage_and_trailing_bytes_are_refused() {
        assert!(parse(b"not zstd at all").is_err());
        let mut log = exec_log(&[]);
        log.extend_from_slice(b"xx");
        assert!(matches!(parse(&log), Err(ExecLogError::Malformed(_))));
    }

    #[test]
    fn a_read_field_of_the_wrong_wire_type_is_malformed() {
        let mut spawn = Vec::new();
        field_varint(SPAWN_TARGET_LABEL, 1, &mut spawn);
        let mut entry = Vec::new();
        field_bytes(ENTRY_SPAWN, &spawn, &mut entry);
        let err = parse(&exec_log(&[entry])).unwrap_err();
        assert!(
            matches!(&err, ExecLogError::Malformed(m) if m.contains("target_label")),
            "{err}"
        );
    }

    #[test]
    fn a_group_wire_type_is_malformed() {
        let mut entry = Vec::new();
        varint((5 << 3) | 3, &mut entry);
        assert!(matches!(
            parse(&exec_log(&[entry])),
            Err(ExecLogError::Malformed(_))
        ));
    }

    #[test]
    fn varints_decode_to_64_bits_and_no_further() {
        let mut buf = Vec::new();
        varint(u64::MAX, &mut buf);
        assert_eq!(read_varint(&mut buf.as_slice()), Ok(u64::MAX));
        let too_long = [0xff; 11];
        assert!(read_varint(&mut too_long.as_slice()).is_err());
        assert!(read_varint(&mut [0x80u8].as_slice()).is_err());
    }
}
