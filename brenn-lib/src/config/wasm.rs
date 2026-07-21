//! Global WASM-host policy configuration.
//!
//! Houses `WasmConfig` (the `[wasm]` top-level TOML block) and helpers shared
//! between config resolution and the store layer.

use std::collections::HashMap;

use brenn_common::PAGE_SIZE;
use serde::Deserialize;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Minimum allowed store size in bytes (floor). Below this: fatal load-time
/// panic. 64 KiB = 16 pages — enough for the SQLite header, schema, and
/// minimal headroom.
const MIN_STORE_SIZE_BYTES: u64 = 64 * 1024;

/// Maximum number of entries in a component config map. Generous enough for
/// real config; bounds the map so it cannot become a covert bulk-data channel.
pub const MAX_CONFIG_ENTRIES: usize = 256;

/// Maximum byte length of a component config key.
pub const MAX_CONFIG_KEY_BYTES: usize = 256;

/// Maximum byte length of a component config value (after canonicalization).
pub const MAX_CONFIG_VALUE_BYTES: usize = 4096;

/// Reserved key prefix injected by the host; operator TOML cannot use it.
pub const RESERVED_CONFIG_PREFIX: &str = "brenn.";

// ---------------------------------------------------------------------------
// Config types
// ---------------------------------------------------------------------------

/// Global WASM-host policy (`[wasm]` block). Omitting the block entirely
/// produces the same defaults as an empty `[wasm]` block.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WasmConfig {
    /// Default store size cap applied to every `[[webhook_endpoint]]` with
    /// replay protection unless overridden per-store. Human-readable binary
    /// byte-size string (e.g. `"64MiB"`). Parsed and validated at load time.
    #[serde(default = "default_store_size_limit")]
    pub store_size_limit: String,
}

fn default_store_size_limit() -> String {
    "64MiB".to_string()
}

impl Default for WasmConfig {
    fn default() -> Self {
        Self {
            store_size_limit: default_store_size_limit(),
        }
    }
}

// ---------------------------------------------------------------------------
// Byte-size parsing
// ---------------------------------------------------------------------------

/// Parse a human-readable binary byte-size string into bytes.
///
/// Grammar: integer mantissa followed by a binary unit suffix `KiB`, `MiB`,
/// or `GiB` (case-insensitive, powers of 1024). No fractional mantissa, no
/// decimal-SI units (`KB`/`MB`/`GB`), no bare integer (a unit is required).
///
/// Returns `Err(human message)` on any parse failure.
pub fn parse_byte_size(s: &str) -> Result<u64, String> {
    let s = s.trim();
    if s.is_empty() {
        return Err("byte-size string is empty".to_string());
    }

    // Split at the first non-digit character.
    let split_pos = s
        .find(|c: char| !c.is_ascii_digit())
        .ok_or_else(|| format!("{s:?}: missing unit suffix (use KiB, MiB, or GiB)"))?;

    let (mantissa_str, suffix) = s.split_at(split_pos);
    if mantissa_str.is_empty() {
        return Err(format!(
            "{s:?}: missing integer mantissa before unit suffix"
        ));
    }

    let mantissa: u64 = mantissa_str
        .parse()
        .map_err(|e| format!("{s:?}: invalid integer mantissa: {e}"))?;

    let multiplier: u64 = match suffix.to_ascii_lowercase().as_str() {
        "kib" => 1024,
        "mib" => 1024 * 1024,
        "gib" => 1024 * 1024 * 1024,
        other => {
            return Err(format!(
                "{s:?}: unrecognised unit {:?}; must be one of KiB, MiB, GiB (binary, powers of 1024)",
                other,
            ));
        }
    };

    mantissa
        .checked_mul(multiplier)
        .ok_or_else(|| format!("{s:?}: byte-size overflows u64"))
}

// ---------------------------------------------------------------------------
// Page-count conversion and validation
// ---------------------------------------------------------------------------

/// Convert a byte-size string to a `max_page_count` (u32) for SQLite's
/// `PRAGMA max_page_count`, validating floor and overflow constraints.
///
/// `ceil(bytes / PAGE_SIZE)` is used so the enforced cap is always `>=` the
/// configured bytes, never less (AC-1).
///
/// # Panics
///
/// Panics with a descriptive message naming the field and value if:
/// - `size_str` is unparseable.
/// - The resolved byte count is below `MIN_STORE_SIZE_BYTES` (64 KiB).
/// - The page count overflows `u32` (pathological multi-TiB cap).
pub fn byte_size_to_max_page_count(size_str: &str, field_name: &str) -> u32 {
    let bytes = parse_byte_size(size_str).unwrap_or_else(|e| {
        panic!("config field {field_name:?}: invalid byte-size string {size_str:?}: {e}")
    });

    assert!(
        bytes >= MIN_STORE_SIZE_BYTES,
        "config field {field_name:?}: store_size_limit {size_str:?} resolves to {bytes} bytes, \
         which is below the minimum {MIN_STORE_SIZE_BYTES} bytes (64 KiB); \
         use at least \"64KiB\"",
    );

    // ceil(bytes / PAGE_SIZE)
    let pages = bytes.div_ceil(PAGE_SIZE);

    // SQLite's own max_page_count upper bound is SQLITE_MAX_PAGE_COUNT which
    // is u32::MAX by default in most builds. A checked cast is sufficient.
    u32::try_from(pages).unwrap_or_else(|_| {
        panic!(
            "config field {field_name:?}: store_size_limit {size_str:?} resolves to {pages} pages, \
             which overflows u32; cap is too large",
        )
    })
}

// ---------------------------------------------------------------------------
// Component config resolver
// ---------------------------------------------------------------------------

/// Resolve an operator-supplied `[…config]` TOML table into a flat
/// `HashMap<String, String>`.
///
/// Accepted value types: string (as-is), integer (decimal string), boolean
/// (`"true"` / `"false"`). Floats, datetimes, arrays, and nested tables are
/// rejected — floats because canonical string form is ambiguous; structures
/// because the interface is deliberately flat (a guest wanting structure puts
/// JSON in a string value).
///
/// Validation rules (all violations are `panic!`s, fail-fast per project
/// policy):
/// - Key: non-empty, ≤ `MAX_CONFIG_KEY_BYTES` bytes, no ASCII control chars.
/// - Key must not start with `RESERVED_CONFIG_PREFIX` (`"brenn."`).
/// - Value: ≤ `MAX_CONFIG_VALUE_BYTES` bytes after canonicalization.
/// - Entry count: ≤ `MAX_CONFIG_ENTRIES`.
///
/// Returns an empty map when `raw` is `None`.
///
/// `field_name` is used in panic messages (e.g. `"[[wasm_consumer]] \"x\" config"`).
pub fn resolve_component_config(
    raw: Option<&toml::Table>,
    field_name: &str,
) -> HashMap<String, String> {
    let Some(table) = raw else {
        return HashMap::new();
    };

    let mut result = HashMap::with_capacity(table.len().min(MAX_CONFIG_ENTRIES));

    for (key, value) in table {
        // Key: non-empty.
        assert!(
            !key.is_empty(),
            "{field_name}: config key must be non-empty",
        );
        // Key: no ASCII control characters.
        assert!(
            !key.bytes().any(|b| b < 0x20 || b == 0x7f),
            "{field_name}: config key {key:?} contains ASCII control characters",
        );
        // Key: length bound.
        assert!(
            key.len() <= MAX_CONFIG_KEY_BYTES,
            "{field_name}: config key {key:?} is {} bytes; maximum is {MAX_CONFIG_KEY_BYTES}",
            key.len(),
        );
        // Key: reserved prefix.
        assert!(
            !key.starts_with(RESERVED_CONFIG_PREFIX),
            "{field_name}: config key {key:?} uses the reserved prefix \
             {RESERVED_CONFIG_PREFIX:?}; operator TOML cannot set keys under this prefix",
        );

        // Canonicalize value to String.
        let value_str = match value {
            toml::Value::String(s) => s.clone(),
            toml::Value::Integer(i) => i.to_string(),
            toml::Value::Boolean(b) => b.to_string(),
            toml::Value::Float(_) => panic!(
                "{field_name}: config key {key:?} has a float value; \
                 use a string (canonical float form is ambiguous)",
            ),
            toml::Value::Datetime(_) => panic!(
                "{field_name}: config key {key:?} has a datetime value; \
                 use a string",
            ),
            toml::Value::Array(_) => panic!(
                "{field_name}: config key {key:?} has an array value; \
                 the config surface is flat — encode structure as a JSON string",
            ),
            toml::Value::Table(_) => panic!(
                "{field_name}: config key {key:?} has a table value; \
                 the config surface is flat — encode structure as a JSON string",
            ),
        };

        // Value: length bound.
        assert!(
            value_str.len() <= MAX_CONFIG_VALUE_BYTES,
            "{field_name}: config key {key:?} value is {} bytes; maximum is {MAX_CONFIG_VALUE_BYTES}",
            value_str.len(),
        );

        result.insert(key.clone(), value_str);
    }

    // Count check after key/value validation so any key/value errors surface
    // first (an operator fixing a count violation may not notice a hidden key
    // error on the same restart cycle).
    assert!(
        result.len() <= MAX_CONFIG_ENTRIES,
        "{field_name}: config table has {} entries; maximum is {MAX_CONFIG_ENTRIES}",
        result.len(),
    );

    result
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // --- parse_byte_size ---

    #[test]
    fn parse_64_mib() {
        assert_eq!(parse_byte_size("64MiB").unwrap(), 64 * 1024 * 1024);
    }

    #[test]
    fn parse_512_mib() {
        assert_eq!(parse_byte_size("512MiB").unwrap(), 512 * 1024 * 1024);
    }

    #[test]
    fn parse_1_gib() {
        assert_eq!(parse_byte_size("1GiB").unwrap(), 1024 * 1024 * 1024);
    }

    #[test]
    fn parse_64_kib() {
        assert_eq!(parse_byte_size("64KiB").unwrap(), 64 * 1024);
    }

    #[test]
    fn parse_case_insensitive() {
        assert_eq!(
            parse_byte_size("64mib").unwrap(),
            parse_byte_size("64MiB").unwrap()
        );
        assert_eq!(
            parse_byte_size("64MIB").unwrap(),
            parse_byte_size("64MiB").unwrap()
        );
        assert_eq!(
            parse_byte_size("64KIB").unwrap(),
            parse_byte_size("64KiB").unwrap()
        );
        assert_eq!(
            parse_byte_size("1GIB").unwrap(),
            parse_byte_size("1GiB").unwrap()
        );
    }

    #[test]
    fn reject_decimal_si_mb() {
        assert!(parse_byte_size("64MB").is_err());
    }

    #[test]
    fn reject_decimal_si_kb() {
        assert!(parse_byte_size("64KB").is_err());
    }

    #[test]
    fn reject_bare_integer() {
        assert!(parse_byte_size("67108864").is_err());
    }

    #[test]
    fn reject_fractional() {
        assert!(parse_byte_size("1.5MiB").is_err());
    }

    #[test]
    fn reject_unit_only() {
        assert!(parse_byte_size("MiB").is_err());
    }

    #[test]
    fn reject_empty() {
        assert!(parse_byte_size("").is_err());
    }

    // --- byte_size_to_max_page_count ---

    #[test]
    fn pages_ceil_rounds_up() {
        // 65KiB = 66560 bytes; 66560 / 4096 = 16.25 → ceil = 17 pages.
        // Tests that non-page-multiple values round up (not down).
        let pages = byte_size_to_max_page_count("65KiB", "test");
        assert_eq!(pages, 17);
        // Enforced cap (pages * PAGE_SIZE) >= configured bytes
        assert!(u64::from(pages) * PAGE_SIZE >= 65 * 1024);
    }

    #[test]
    fn pages_exact_multiple() {
        // 64 KiB = 65536 bytes = exactly 16 pages (no rounding)
        let pages = byte_size_to_max_page_count("64KiB", "test");
        assert_eq!(pages, 16);
    }

    #[test]
    fn enforced_cap_gte_configured_bytes() {
        // Verify the invariant for a non-multiple value: 65KiB
        let pages = byte_size_to_max_page_count("65KiB", "test");
        assert!(u64::from(pages) * PAGE_SIZE >= 65 * 1024);
    }

    #[test]
    fn default_64_mib_gives_16384_pages() {
        let pages = byte_size_to_max_page_count("64MiB", "test");
        assert_eq!(pages, 16384); // 64*1024*1024 / 4096 = 16384
    }

    #[test]
    #[should_panic(expected = "below the minimum")]
    fn below_floor_panics() {
        byte_size_to_max_page_count("32KiB", "test.store_size_limit");
    }

    #[test]
    fn exactly_floor_accepted() {
        // 64 KiB is the floor; must not panic
        let pages = byte_size_to_max_page_count("64KiB", "test");
        assert_eq!(pages, 16);
    }

    #[test]
    #[should_panic(expected = "invalid byte-size string")]
    fn unparseable_panics() {
        byte_size_to_max_page_count("notasize", "test.store_size_limit");
    }

    // --- WasmConfig defaults ---

    #[test]
    fn default_store_size_limit_is_64_mib() {
        let cfg = WasmConfig::default();
        assert_eq!(cfg.store_size_limit, "64MiB");
    }

    // --- resolve_component_config ---

    fn make_table(pairs: &[(&str, toml::Value)]) -> toml::Table {
        let mut t = toml::Table::new();
        for (k, v) in pairs {
            t.insert(k.to_string(), v.clone());
        }
        t
    }

    #[test]
    fn absent_table_gives_empty_map() {
        let result = resolve_component_config(None, "test");
        assert!(result.is_empty());
    }

    #[test]
    fn string_value_passthrough() {
        let table = make_table(&[("key", toml::Value::String("hello".to_string()))]);
        let result = resolve_component_config(Some(&table), "test");
        assert_eq!(result.get("key").map(String::as_str), Some("hello"));
    }

    #[test]
    fn integer_canonicalized_to_decimal_string() {
        let table = make_table(&[("max_entries", toml::Value::Integer(1024))]);
        let result = resolve_component_config(Some(&table), "test");
        assert_eq!(result.get("max_entries").map(String::as_str), Some("1024"));
    }

    #[test]
    fn boolean_true_canonicalized() {
        let table = make_table(&[("enabled", toml::Value::Boolean(true))]);
        let result = resolve_component_config(Some(&table), "test");
        assert_eq!(result.get("enabled").map(String::as_str), Some("true"));
    }

    #[test]
    fn boolean_false_canonicalized() {
        let table = make_table(&[("enabled", toml::Value::Boolean(false))]);
        let result = resolve_component_config(Some(&table), "test");
        assert_eq!(result.get("enabled").map(String::as_str), Some("false"));
    }

    #[test]
    #[should_panic(expected = "float value")]
    fn float_rejected() {
        let table = make_table(&[("ratio", toml::Value::Float(1.5))]);
        resolve_component_config(Some(&table), "test");
    }

    #[test]
    #[should_panic(expected = "array value")]
    fn array_rejected() {
        let table = make_table(&[("items", toml::Value::Array(vec![toml::Value::Integer(1)]))]);
        resolve_component_config(Some(&table), "test");
    }

    #[test]
    #[should_panic(expected = "table value")]
    fn nested_table_rejected() {
        let inner = toml::Value::Table(make_table(&[("x", toml::Value::Integer(1))]));
        let table = make_table(&[("nested", inner)]);
        resolve_component_config(Some(&table), "test");
    }

    #[test]
    #[should_panic(expected = "datetime value")]
    fn datetime_rejected() {
        let dt = toml::Value::Datetime(toml::value::Datetime {
            date: Some(toml::value::Date {
                year: 2024,
                month: 1,
                day: 1,
            }),
            time: None,
            offset: None,
        });
        let table = make_table(&[("ts", dt)]);
        resolve_component_config(Some(&table), "test");
    }

    #[test]
    #[should_panic(expected = "reserved prefix")]
    fn reserved_prefix_rejected() {
        let table = make_table(&[("brenn.foo", toml::Value::String("x".to_string()))]);
        resolve_component_config(Some(&table), "test");
    }

    #[test]
    #[should_panic(expected = "non-empty")]
    fn empty_key_rejected() {
        let table = make_table(&[("", toml::Value::String("x".to_string()))]);
        resolve_component_config(Some(&table), "test");
    }

    #[test]
    #[should_panic(expected = "ASCII control characters")]
    fn control_char_in_key_rejected() {
        let table = make_table(&[("key\x01val", toml::Value::String("x".to_string()))]);
        resolve_component_config(Some(&table), "test");
    }

    #[test]
    #[should_panic(expected = "bytes; maximum is")]
    fn oversize_key_rejected() {
        let long_key = "k".repeat(MAX_CONFIG_KEY_BYTES + 1);
        let table = make_table(&[(&long_key, toml::Value::String("x".to_string()))]);
        resolve_component_config(Some(&table), "test");
    }

    #[test]
    #[should_panic(expected = "bytes; maximum is")]
    fn oversize_value_rejected() {
        let long_val = "v".repeat(MAX_CONFIG_VALUE_BYTES + 1);
        let table = make_table(&[("key", toml::Value::String(long_val))]);
        resolve_component_config(Some(&table), "test");
    }

    #[test]
    #[should_panic(expected = "entries; maximum is")]
    fn entry_count_overflow_rejected() {
        let mut table = toml::Table::new();
        for i in 0..=MAX_CONFIG_ENTRIES {
            table.insert(format!("key{i}"), toml::Value::Integer(i as i64));
        }
        resolve_component_config(Some(&table), "test");
    }

    #[test]
    fn multiple_entries_resolved() {
        let table = make_table(&[
            ("str_key", toml::Value::String("hello".to_string())),
            ("int_key", toml::Value::Integer(42)),
            ("bool_key", toml::Value::Boolean(true)),
        ]);
        let result = resolve_component_config(Some(&table), "test");
        assert_eq!(result.len(), 3);
        assert_eq!(result["str_key"], "hello");
        assert_eq!(result["int_key"], "42");
        assert_eq!(result["bool_key"], "true");
    }
}
