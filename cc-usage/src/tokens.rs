use crate::schema::RawUsage;

/// The five token classes tracked per usage record.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct TokenCounts {
    pub input: u64,
    pub cache_write_5m: u64,
    pub cache_write_1h: u64,
    pub cache_read: u64,
    pub output: u64,
}

impl TokenCounts {
    pub fn total(&self) -> u64 {
        self.input + self.cache_write_5m + self.cache_write_1h + self.cache_read + self.output
    }
}

impl std::ops::Add for TokenCounts {
    type Output = Self;
    fn add(self, rhs: Self) -> Self {
        Self {
            input: self.input + rhs.input,
            cache_write_5m: self.cache_write_5m + rhs.cache_write_5m,
            cache_write_1h: self.cache_write_1h + rhs.cache_write_1h,
            cache_read: self.cache_read + rhs.cache_read,
            output: self.output + rhs.output,
        }
    }
}

impl std::ops::AddAssign for TokenCounts {
    fn add_assign(&mut self, rhs: Self) {
        *self = *self + rhs;
    }
}

/// Compute the five token classes from a raw usage record.
///
/// Rules (per design + requirements):
/// - `cache_read` = `cache_read_input_tokens` (default 0).
/// - If nested `cache_creation` is present: w5 = `ephemeral_5m`, w1 = `ephemeral_1h`.
///   Otherwise: w5 = scalar `cache_creation_input_tokens` (default 0), w1 = 0.
/// - `input` = `input_tokens` − cache_read − (w5 + w1), clamped at 0.
/// - `output` = `output_tokens` (default 0).
pub fn compute(usage: &RawUsage) -> TokenCounts {
    let cache_read = usage.cache_read_input_tokens.unwrap_or(0);

    let (w5, w1) = match &usage.cache_creation {
        Some(c) => (
            c.ephemeral_5m_input_tokens.unwrap_or(0),
            c.ephemeral_1h_input_tokens.unwrap_or(0),
        ),
        None => (usage.cache_creation_input_tokens.unwrap_or(0), 0),
    };

    let output = usage.output_tokens.unwrap_or(0);
    let raw_input = usage.input_tokens.unwrap_or(0);
    let input = raw_input.saturating_sub(cache_read).saturating_sub(w5 + w1);

    TokenCounts {
        input,
        cache_write_5m: w5,
        cache_write_1h: w1,
        cache_read,
        output,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{RawCacheCreation, RawUsage};

    fn usage(
        input: Option<u64>,
        output: Option<u64>,
        cache_create_scalar: Option<u64>,
        cache_read: Option<u64>,
        cache_creation_nested: Option<RawCacheCreation>,
    ) -> RawUsage {
        RawUsage {
            input_tokens: input,
            output_tokens: output,
            cache_creation_input_tokens: cache_create_scalar,
            cache_read_input_tokens: cache_read,
            cache_creation: cache_creation_nested,
        }
    }

    fn nested(m5: Option<u64>, h1: Option<u64>) -> RawCacheCreation {
        RawCacheCreation {
            ephemeral_5m_input_tokens: m5,
            ephemeral_1h_input_tokens: h1,
        }
    }

    #[test]
    fn scalar_only() {
        // input=1000, output=200, cache_create_scalar=100, cache_read=50
        let u = usage(Some(1000), Some(200), Some(100), Some(50), None);
        let t = compute(&u);
        assert_eq!(t.cache_read, 50);
        assert_eq!(t.cache_write_5m, 100);
        assert_eq!(t.cache_write_1h, 0);
        assert_eq!(t.output, 200);
        // input = 1000 - 50 - 100 = 850
        assert_eq!(t.input, 850);
    }

    #[test]
    fn nested_only() {
        let u = usage(
            Some(1000),
            Some(200),
            None,
            Some(50),
            Some(nested(Some(80), Some(20))),
        );
        let t = compute(&u);
        assert_eq!(t.cache_read, 50);
        assert_eq!(t.cache_write_5m, 80);
        assert_eq!(t.cache_write_1h, 20);
        assert_eq!(t.output, 200);
        // input = 1000 - 50 - (80+20) = 850
        assert_eq!(t.input, 850);
    }

    #[test]
    fn both_present_nested_wins() {
        // scalar=999, nested=(80, 20) — nested should win
        let u = usage(
            Some(1000),
            Some(200),
            Some(999),
            Some(50),
            Some(nested(Some(80), Some(20))),
        );
        let t = compute(&u);
        assert_eq!(t.cache_write_5m, 80);
        assert_eq!(t.cache_write_1h, 20);
    }

    #[test]
    fn missing_fields_default_to_zero() {
        let u = usage(None, None, None, None, None);
        let t = compute(&u);
        assert_eq!(t, TokenCounts::default());
    }

    #[test]
    fn clamp_at_zero() {
        // input_tokens=10, cache_read=5, cache_write_5m=20
        // raw: 10 - 5 = 5, then 5 - 20 = should clamp to 0
        let u = usage(Some(10), Some(50), Some(20), Some(5), None);
        let t = compute(&u);
        assert_eq!(t.input, 0);
        assert_eq!(t.output, 50);
    }

    #[test]
    fn partial_missing_treated_as_zero() {
        // Only input present, output missing
        let u = usage(Some(500), None, Some(100), Some(50), None);
        let t = compute(&u);
        assert_eq!(t.input, 350); // 500 - 50 - 100
        assert_eq!(t.output, 0);
    }

    #[test]
    fn total() {
        let t = TokenCounts {
            input: 10,
            cache_write_5m: 20,
            cache_write_1h: 30,
            cache_read: 40,
            output: 50,
        };
        assert_eq!(t.total(), 150);
    }
}
