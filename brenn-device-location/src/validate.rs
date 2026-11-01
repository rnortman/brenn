//! Intake bounds for a position fix reported by a browser.
//!
//! Every bound is one no browser's Geolocation API can exceed, so a refused
//! value comes from a modified client. [`FixRejection`] names the field and
//! the bound and carries no value: a log line or security event built from it
//! cannot contain a coordinate.

use std::fmt;

/// Largest accepted accuracy radius, in metres: half the Earth's
/// circumference. No browser reports more.
pub const MAX_ACCURACY_M: f64 = 20_000_000.0;

/// A fix whose three numbers passed [`validate_fix`].
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ValidFix {
    latitude: f64,
    longitude: f64,
    accuracy_m: f64,
}

impl ValidFix {
    /// Degrees north, in `-90.0..=90.0`.
    pub fn latitude(&self) -> f64 {
        self.latitude
    }

    /// Degrees east, in `-180.0..=180.0`.
    pub fn longitude(&self) -> f64 {
        self.longitude
    }

    /// 68 % confidence radius in metres, in `0.0..=MAX_ACCURACY_M`.
    pub fn accuracy_m(&self) -> f64 {
        self.accuracy_m
    }
}

/// Which field of a fix was out of bounds. Carries no value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FixRejection {
    Latitude,
    Longitude,
    Accuracy,
}

impl fmt::Display for FixRejection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FixRejection::Latitude => f.write_str("latitude not a finite number in -90..=90"),
            FixRejection::Longitude => f.write_str("longitude not a finite number in -180..=180"),
            FixRejection::Accuracy => f.write_str("accuracy_m not a finite number in 0..=20000000"),
        }
    }
}

/// Check a fix's three numbers against the bounds a browser can produce.
///
/// The finiteness checks guard this function's own boundary: JSON has no NaN
/// or infinity, so serde never hands one over the wire.
pub fn validate_fix(
    latitude: f64,
    longitude: f64,
    accuracy_m: f64,
) -> Result<ValidFix, FixRejection> {
    // `RangeInclusive::contains` is false for NaN, and the bounds are finite,
    // so one check covers both finiteness and range.
    if !(-90.0..=90.0).contains(&latitude) {
        return Err(FixRejection::Latitude);
    }
    if !(-180.0..=180.0).contains(&longitude) {
        return Err(FixRejection::Longitude);
    }
    if !(0.0..=MAX_ACCURACY_M).contains(&accuracy_m) {
        return Err(FixRejection::Accuracy);
    }
    Ok(ValidFix {
        latitude,
        longitude,
        accuracy_m,
    })
}

/// A [`ValidFix`] for this crate's tests; panics if out of bounds.
#[cfg(test)]
pub(crate) fn fix(latitude: f64, longitude: f64, accuracy_m: f64) -> ValidFix {
    validate_fix(latitude, longitude, accuracy_m).expect("test fix in bounds")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounds_are_inclusive_at_every_edge() {
        for (lat, lng, acc) in [
            (90.0, 180.0, 0.0),
            (-90.0, -180.0, MAX_ACCURACY_M),
            (0.0, 0.0, -0.0),
            (51.5, -0.12, 12.5),
        ] {
            let f = validate_fix(lat, lng, acc).unwrap();
            assert_eq!(
                (f.latitude(), f.longitude(), f.accuracy_m()),
                (lat, lng, acc)
            );
        }
    }

    #[test]
    fn out_of_range_values_are_refused_naming_the_field() {
        let cases = [
            ((90.000_001, 0.0, 1.0), FixRejection::Latitude),
            ((-90.000_001, 0.0, 1.0), FixRejection::Latitude),
            ((0.0, 180.000_001, 1.0), FixRejection::Longitude),
            ((0.0, -180.000_001, 1.0), FixRejection::Longitude),
            ((0.0, 0.0, MAX_ACCURACY_M + 1.0), FixRejection::Accuracy),
            ((0.0, 0.0, -0.001), FixRejection::Accuracy),
        ];
        for ((lat, lng, acc), want) in cases {
            assert_eq!(validate_fix(lat, lng, acc), Err(want), "{lat} {lng} {acc}");
        }
    }

    #[test]
    fn non_finite_values_are_refused_on_every_field() {
        for bad in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            assert_eq!(validate_fix(bad, 0.0, 1.0), Err(FixRejection::Latitude));
            assert_eq!(validate_fix(0.0, bad, 1.0), Err(FixRejection::Longitude));
            assert_eq!(validate_fix(0.0, 0.0, bad), Err(FixRejection::Accuracy));
        }
    }

    /// The rendered rejection carries only the digits of its own bound, so a
    /// message built from it cannot leak the refused coordinate.
    #[test]
    fn rejection_display_contains_no_digit_but_the_bounds() {
        for (r, bound_digits) in [
            (FixRejection::Latitude, "9090"),
            (FixRejection::Longitude, "180180"),
            (FixRejection::Accuracy, "020000000"),
        ] {
            let digits: String = r.to_string().chars().filter(char::is_ascii_digit).collect();
            assert_eq!(digits, bound_digits, "{r}");
        }
    }
}
