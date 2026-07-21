use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::tokens::TokenCounts;

/// Prices per million tokens for each token class.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriceVector {
    pub input: f64,
    pub cache_write_5m: f64,
    pub cache_write_1h: f64,
    pub cache_read: f64,
    pub output: f64,
}

impl PriceVector {
    pub fn cost(&self, tokens: &TokenCounts) -> f64 {
        (tokens.input as f64 * self.input
            + tokens.cache_write_5m as f64 * self.cache_write_5m
            + tokens.cache_write_1h as f64 * self.cache_write_1h
            + tokens.cache_read as f64 * self.cache_read
            + tokens.output as f64 * self.output)
            / 1_000_000.0
    }
}

/// Model family used as a fallback when exact model name has no match.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Family {
    Haiku,
    Opus,
    Sonnet,
}

/// Price table with exact-model entries and family-level defaults.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriceTable {
    /// Exact model name → price vector.
    pub exact: BTreeMap<String, PriceVector>,
    /// Family → default price vector.
    pub family_defaults: BTreeMap<Family, PriceVector>,
}

impl PriceTable {
    /// Built-in default price table (USD per million tokens).
    pub fn defaults() -> Self {
        let mut exact = BTreeMap::new();

        // Opus 4 models
        for model in &[
            "claude-opus-4-7",
            "claude-opus-4-20250514",
            "claude-opus-4-5",
        ] {
            exact.insert(model.to_string(), opus_prices());
        }

        // Sonnet 4 models
        for model in &[
            "claude-sonnet-4-6",
            "claude-sonnet-4-20250514",
            "claude-sonnet-4-5",
            "claude-sonnet-4-5-20250927",
        ] {
            exact.insert(model.to_string(), sonnet_prices());
        }

        // Haiku 4 models
        for model in &["claude-haiku-4-5", "claude-haiku-4-5-20251001"] {
            exact.insert(model.to_string(), haiku_prices());
        }

        let mut family_defaults = BTreeMap::new();
        family_defaults.insert(Family::Opus, opus_prices());
        family_defaults.insert(Family::Sonnet, sonnet_prices());
        family_defaults.insert(Family::Haiku, haiku_prices());

        Self {
            exact,
            family_defaults,
        }
    }

    /// Look up the price vector for a model name.
    /// Tries exact match first, then family heuristic.
    pub fn lookup(&self, model: &str) -> Option<&PriceVector> {
        if let Some(v) = self.exact.get(model) {
            return Some(v);
        }
        let lower = model.to_lowercase();
        let family = if lower.contains("opus") {
            Some(Family::Opus)
        } else if lower.contains("sonnet") {
            Some(Family::Sonnet)
        } else if lower.contains("haiku") {
            Some(Family::Haiku)
        } else {
            None
        };
        family.and_then(|f| self.family_defaults.get(&f))
    }

    /// SHA-256 fingerprint of the canonical JSON serialization of the table.
    pub fn fingerprint(&self) -> String {
        let json = serde_json::to_string(self).expect("PriceTable serialization must not fail");
        let hash = Sha256::digest(json.as_bytes());
        format!("sha256:{}", hex::encode(hash))
    }
}

fn opus_prices() -> PriceVector {
    PriceVector {
        input: 5.00,
        cache_write_5m: 6.25,
        cache_write_1h: 10.00,
        cache_read: 0.50,
        output: 25.00,
    }
}

fn sonnet_prices() -> PriceVector {
    PriceVector {
        input: 3.00,
        cache_write_5m: 3.75,
        cache_write_1h: 6.00,
        cache_read: 0.30,
        output: 15.00,
    }
}

fn haiku_prices() -> PriceVector {
    PriceVector {
        input: 1.00,
        cache_write_5m: 1.25,
        cache_write_1h: 2.00,
        cache_read: 0.10,
        output: 5.00,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_match() {
        let t = PriceTable::defaults();
        let v = t.lookup("claude-sonnet-4-6").unwrap();
        assert!((v.input - 3.00).abs() < 1e-9);
        assert!((v.output - 15.00).abs() < 1e-9);
    }

    #[test]
    fn family_fallback_opus() {
        let t = PriceTable::defaults();
        let v = t.lookup("claude-opus-99-ultra").unwrap();
        assert!((v.input - 5.00).abs() < 1e-9);
    }

    #[test]
    fn family_fallback_sonnet() {
        let t = PriceTable::defaults();
        let v = t.lookup("claude-sonnet-5-0-future").unwrap();
        assert!((v.input - 3.00).abs() < 1e-9);
    }

    #[test]
    fn family_fallback_haiku() {
        let t = PriceTable::defaults();
        let v = t.lookup("claude-haiku-tiny").unwrap();
        assert!((v.input - 1.00).abs() < 1e-9);
    }

    #[test]
    fn unknown_model_returns_none() {
        let t = PriceTable::defaults();
        assert!(t.lookup("gpt-9-turbo").is_none());
        assert!(t.lookup("").is_none());
    }

    #[test]
    fn cost_arithmetic() {
        let p = PriceVector {
            input: 3.00,
            cache_write_5m: 3.75,
            cache_write_1h: 6.00,
            cache_read: 0.30,
            output: 15.00,
        };
        let tokens = TokenCounts {
            input: 1_000_000,
            cache_write_5m: 0,
            cache_write_1h: 0,
            cache_read: 0,
            output: 0,
        };
        // 1M input tokens * $3/M = $3.00
        let cost = p.cost(&tokens);
        assert!((cost - 3.00).abs() < 1e-9, "cost={cost}");
    }

    #[test]
    fn fingerprint_is_stable() {
        let t = PriceTable::defaults();
        let f1 = t.fingerprint();
        let f2 = t.fingerprint();
        assert_eq!(f1, f2);
        assert!(f1.starts_with("sha256:"));
    }
}
