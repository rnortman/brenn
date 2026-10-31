//! Pure-arithmetic Gregorian calendar helpers.
//!
//! No external dependencies. `#![no_std]`-compatible; `ms_to_sent_at` and
//! `ms_to_rfc3339_seconds` require `alloc` for their `String` return type.
//!
//! # Domains
//!
//! - `days_from_epoch`: natural Hinnant domain over `u32` year inputs.
//! - `days_to_ymd`: `0 ..= days_from_epoch(u32::MAX, 12, 31)`, the days from
//!   1970-01-01 to the last day whose year fits `u32`; it panics outside it.
//! - `ms_to_sent_at`: `year < 10000` for fixed-width 24-char output. Years ≥
//!   10000 render correctly but exceed the 24-char protocol invariant. No caller
//!   produces such values.
//! - `ms_to_rfc3339_seconds`: every `u64`; years ≥ 10000 render with more than
//!   four digits.
#![no_std]
extern crate alloc;

use alloc::string::String;

/// Returns `true` if `year` is a Gregorian leap year.
pub fn is_leap_year(year: u32) -> bool {
    (year.is_multiple_of(4) && !year.is_multiple_of(100)) || year.is_multiple_of(400)
}

/// Returns the number of days in `month` of `year`, or `None` for an invalid month.
///
/// `month` must be in `1..=12`; `0` or `>12` returns `None`.
pub fn days_in_month(year: u32, month: u32) -> Option<u32> {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => Some(31),
        4 | 6 | 9 | 11 => Some(30),
        2 => Some(if is_leap_year(year) { 29 } else { 28 }),
        _ => None,
    }
}

/// Count of days from 1970-01-01 to the given date (may be negative for pre-epoch dates).
///
/// Uses Howard Hinnant's O(1) `days_from_civil` closed-form formula.
/// Reference: <https://howardhinnant.github.io/date_algorithms.html#days_from_civil>
///
/// No external crate; fully WASI-compatible.
pub fn days_from_epoch(year: u32, month: u32, day: u32) -> i64 {
    // Shift year so March is the first month: avoids special-casing Feb/leap.
    let y = if month <= 2 {
        year as i64 - 1
    } else {
        year as i64
    };
    let m = month as i64;
    let d = day as i64;

    // Days from civil epoch (0000-03-01) to 1970-01-01 = 719468.
    let era = y.div_euclid(400);
    let yoe = y.rem_euclid(400); // year of era [0, 399]
    let doy = (153 * (m + if m > 2 { -3 } else { 9 }) + 2) / 5 + d - 1; // day of year [0, 365]
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy; // day of era [0, 146096]
    era * 146_097 + doe - 719_468
}

/// Gregorian calendar: days since 1970-01-01 → `(year, month, day)`.
///
/// Inverse of `days_from_epoch` for `0 ..= days_from_epoch(u32::MAX, 12, 31)`;
/// panics on a day count outside that range. Uses Howard Hinnant's O(1)
/// `civil_from_days` closed-form formula: 400-year eras of 146 097 days counted
/// from 0000-03-01, so that the leap day falls at the end of each counted year.
/// Reference: <https://howardhinnant.github.io/date_algorithms.html#civil_from_days>
pub fn days_to_ymd(days: i64) -> (u32, u32, u32) {
    assert!(days >= 0, "days_to_ymd: {days} is before 1970-01-01");
    let last = days_from_epoch(u32::MAX, 12, 31);
    assert!(
        days <= last,
        "days_to_ymd: {days} is past {last}, the last day of year u32::MAX"
    );
    let z = days + 719_468;
    let era = z / 146_097;
    let day_of_era = z % 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_from_march = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_from_march + 2) / 5 + 1;
    let month = if month_from_march < 10 {
        month_from_march + 3
    } else {
        month_from_march - 9
    };
    let year = year_of_era + era * 400 + i64::from(month <= 2);
    (
        u32::try_from(year).expect("a day count up to the asserted last day has a u32 year"),
        month as u32,
        day as u32,
    )
}

/// UTC milliseconds-since-epoch as `(year, month, day, hour, minute, second, millis)`.
fn split_ms(ms: u64) -> (u32, u32, u32, u64, u64, u64, u64) {
    let secs = ms / 1000;
    let sec_of_day = secs % 86_400;
    let (year, month, day) = days_to_ymd((secs / 86_400) as i64);
    (
        year,
        month,
        day,
        sec_of_day / 3600,
        sec_of_day % 3600 / 60,
        sec_of_day % 60,
        ms % 1000,
    )
}

/// Renders UTC milliseconds-since-epoch as a fixed-width RFC3339 string.
///
/// Output format: `YYYY-MM-DDTHH:MM:SS.mmmZ` (24 chars for years < 10000).
pub fn ms_to_sent_at(ms: u64) -> String {
    let (year, month, day, hour, min, sec, millis) = split_ms(ms);
    alloc::format!("{year:04}-{month:02}-{day:02}T{hour:02}:{min:02}:{sec:02}.{millis:03}Z")
}

/// Renders UTC milliseconds-since-epoch as an RFC3339 string to the second,
/// the milliseconds dropped.
///
/// Output format: `YYYY-MM-DDTHH:MM:SSZ` (20 chars for years < 10000).
pub fn ms_to_rfc3339_seconds(ms: u64) -> String {
    let (year, month, day, hour, min, sec, _) = split_ms(ms);
    alloc::format!("{year:04}-{month:02}-{day:02}T{hour:02}:{min:02}:{sec:02}Z")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_leap_year_century_rule() {
        assert!(!is_leap_year(1900));
        assert!(is_leap_year(2000));
        assert!(is_leap_year(2024));
        assert!(!is_leap_year(2023));
        assert!(!is_leap_year(2100));
    }

    #[test]
    fn days_in_month_leap() {
        let year = 2024;
        assert_eq!(days_in_month(year, 1), Some(31));
        assert_eq!(days_in_month(year, 2), Some(29));
        assert_eq!(days_in_month(year, 3), Some(31));
        assert_eq!(days_in_month(year, 4), Some(30));
        assert_eq!(days_in_month(year, 5), Some(31));
        assert_eq!(days_in_month(year, 6), Some(30));
        assert_eq!(days_in_month(year, 7), Some(31));
        assert_eq!(days_in_month(year, 8), Some(31));
        assert_eq!(days_in_month(year, 9), Some(30));
        assert_eq!(days_in_month(year, 10), Some(31));
        assert_eq!(days_in_month(year, 11), Some(30));
        assert_eq!(days_in_month(year, 12), Some(31));
    }

    #[test]
    fn days_in_month_non_leap() {
        let year = 2023;
        assert_eq!(days_in_month(year, 1), Some(31));
        assert_eq!(days_in_month(year, 2), Some(28));
        assert_eq!(days_in_month(year, 3), Some(31));
        assert_eq!(days_in_month(year, 4), Some(30));
        assert_eq!(days_in_month(year, 5), Some(31));
        assert_eq!(days_in_month(year, 6), Some(30));
        assert_eq!(days_in_month(year, 7), Some(31));
        assert_eq!(days_in_month(year, 8), Some(31));
        assert_eq!(days_in_month(year, 9), Some(30));
        assert_eq!(days_in_month(year, 10), Some(31));
        assert_eq!(days_in_month(year, 11), Some(30));
        assert_eq!(days_in_month(year, 12), Some(31));
    }

    #[test]
    fn days_in_month_invalid() {
        assert_eq!(days_in_month(2024, 0), None);
        assert_eq!(days_in_month(2024, 13), None);
    }

    #[test]
    fn days_from_epoch_zero() {
        assert_eq!(days_from_epoch(1970, 1, 1), 0);
        assert_eq!(days_from_epoch(1970, 1, 2), 1);
        assert_eq!(days_from_epoch(1970, 12, 31), 364);
    }

    #[test]
    fn days_from_epoch_2000_march() {
        assert_eq!(days_from_epoch(2000, 2, 29), 11016);
        assert_eq!(days_from_epoch(2000, 3, 1), 11017);
    }

    #[test]
    fn days_from_epoch_2024_leap_day() {
        assert_eq!(days_from_epoch(2024, 2, 29), 19782);
        assert_eq!(days_from_epoch(2025, 3, 1), 20148);
        assert_eq!(days_from_epoch(2100, 1, 1), 47482);
    }

    #[test]
    fn days_to_ymd_roundtrip() {
        for (y, m, d) in [
            (1970u32, 1u32, 1u32),
            (1970, 12, 31),
            (2000, 2, 29),
            (2024, 2, 29),
            (2025, 3, 1),
            (2100, 1, 1),
        ] {
            let days = days_from_epoch(y, m, d);
            assert_eq!(
                days_to_ymd(days),
                (y, m, d),
                "roundtrip failed for {y}-{m:02}-{d:02}"
            );
        }
    }

    #[test]
    fn days_to_ymd_is_the_inverse_of_days_from_epoch_over_every_day_to_2500() {
        let last = days_from_epoch(2500, 12, 31);
        let (mut y, mut m, mut d) = (1970u32, 1u32, 1u32);
        for days in 0..=last {
            assert_eq!(days_to_ymd(days), (y, m, d), "day {days}");
            d += 1;
            if d > days_in_month(y, m).unwrap() {
                d = 1;
                m += 1;
                if m > 12 {
                    m = 1;
                    y += 1;
                }
            }
        }
    }

    #[test]
    fn days_to_ymd_far_dates() {
        assert_eq!(days_to_ymd(days_from_epoch(9999, 12, 31)), (9999, 12, 31));
        // The last day `u64` epoch milliseconds reach.
        assert_eq!(
            days_to_ymd((u64::MAX / 86_400_000) as i64),
            (584_556_019, 4, 3)
        );
    }

    #[test]
    #[should_panic(expected = "before 1970-01-01")]
    fn days_to_ymd_refuses_a_negative_day_count() {
        days_to_ymd(-1);
    }

    #[test]
    fn days_to_ymd_reaches_the_last_day_of_year_u32_max() {
        let last = days_from_epoch(u32::MAX, 12, 31);
        assert_eq!(days_to_ymd(last), (u32::MAX, 12, 31));
    }

    #[test]
    #[should_panic(expected = "the last day of year u32::MAX")]
    fn days_to_ymd_refuses_a_day_count_past_year_u32_max() {
        days_to_ymd(days_from_epoch(u32::MAX, 12, 31) + 1);
    }

    #[test]
    #[should_panic(expected = "the last day of year u32::MAX")]
    fn days_to_ymd_refuses_the_largest_day_count() {
        days_to_ymd(i64::MAX);
    }

    #[test]
    fn ms_to_rfc3339_seconds_drops_the_milliseconds() {
        assert_eq!(ms_to_rfc3339_seconds(0), "1970-01-01T00:00:00Z");
        assert_eq!(ms_to_rfc3339_seconds(999), "1970-01-01T00:00:00Z");
        // 2024-02-29T23:59:59.999Z, then the next instant.
        let ms = (19_782 * 86_400 + 86_399) * 1000 + 999;
        assert_eq!(ms_to_rfc3339_seconds(ms), "2024-02-29T23:59:59Z");
        assert_eq!(ms_to_rfc3339_seconds(ms + 1), "2024-03-01T00:00:00Z");
        assert_eq!(
            ms_to_rfc3339_seconds(253_402_300_799_000),
            "9999-12-31T23:59:59Z"
        );
    }

    #[test]
    fn ms_to_sent_at_epoch() {
        assert_eq!(ms_to_sent_at(0), "1970-01-01T00:00:00.000Z");
    }

    #[test]
    fn ms_to_sent_at_with_ms() {
        assert_eq!(ms_to_sent_at(123), "1970-01-01T00:00:00.123Z");
        // Verify each field is independently distinct: 05:37:42.456Z
        let ms = (1_000 * (3600 * 5 + 60 * 37 + 42)) + 456;
        assert_eq!(ms_to_sent_at(ms), "1970-01-01T05:37:42.456Z");
    }

    #[test]
    fn ms_to_sent_at_leap_boundary() {
        // 2024-02-29T23:59:59.999Z
        // days_from_epoch(2024,2,29) = 19782 => secs_from_epoch = 19782 * 86400 + 86399
        let base_days: u64 = 19782;
        let ms = (base_days * 86_400 + 86_399) * 1000 + 999;
        assert_eq!(ms_to_sent_at(ms), "2024-02-29T23:59:59.999Z");
    }
}
