//! The account ledger: per configured profile, the spend and refusal flags the
//! host has observed.
//!
//! Pure state. Its storage is the retained message on the accounts channel: a
//! ledger is [`seed`](AccountLedger::seed)ed from that message at boot and its
//! [`snapshot`](AccountLedger::snapshot) is what gets published back.
//!
//! Every operation first [`roll`](AccountLedger::roll)s, so a record is never
//! read or written in a period that has already ended.

use std::collections::BTreeMap;

use brenn_envelope::agent::{
    AccountFlag, AccountKind, AgentAccount, AgentAccounts, AgentDecodeError, decode,
};
use brenn_lib::config::ClaudeProfile;
use chrono::{DateTime, Utc};

/// `period` of a subscription account, which has no spend.
pub const NO_PERIOD: &str = "-";

/// What one `result` frame under an API-key profile contributed.
#[derive(Debug, Clone, PartialEq)]
pub struct TurnOutcome {
    /// Dollars the turn cost. Finite and not negative.
    pub cost: f64,
    /// The frame reported an error.
    pub is_error: bool,
    /// The turn was refused for lack of credit.
    pub refusal: Option<AccountFlag>,
}

/// One profile's record, with the settings its accounting reads.
#[derive(Debug, Clone)]
struct Entry {
    /// The zone whose calendar month is the period; `Some` exactly for a key.
    reset_tz: Option<chrono_tz::Tz>,
    account: AgentAccount,
}

/// Per configured profile, the account record.
#[derive(Debug, Clone)]
pub struct AccountLedger {
    entries: BTreeMap<String, Entry>,
}

/// `now_ms` as an instant.
///
/// # Panics
///
/// If no `DateTime<Utc>` can carry it. The caller reads the wall clock, which
/// always can.
fn instant(now_ms: u64) -> DateTime<Utc> {
    brenn_envelope::utc_from_epoch_ms(now_ms)
        .unwrap_or_else(|| panic!("{now_ms} ms is not a representable instant"))
}

/// The `YYYY-MM` accounting period containing `now_ms` in `tz`.
fn period(tz: chrono_tz::Tz, now_ms: u64) -> String {
    instant(now_ms)
        .with_timezone(&tz)
        .format("%Y-%m")
        .to_string()
}

impl AccountLedger {
    /// Every configured profile at zero: no spend, no flags.
    pub fn new(profiles: &BTreeMap<String, ClaudeProfile>, now_ms: u64) -> Self {
        let entries = profiles
            .iter()
            .map(|(name, profile)| {
                let key = profile.credential.key_settings();
                let reset_tz = key.map(|key| key.reset_tz);
                let account = AgentAccount {
                    account: name.clone(),
                    account_kind: profile.credential.kind(),
                    period: reset_tz.map_or_else(|| NO_PERIOD.to_string(), |tz| period(tz, now_ms)),
                    usd: 0.0,
                    turns: reset_tz.map(|_| 0),
                    budget_usd: key.and_then(|key| key.budget_usd),
                    exhausted: None,
                    exhausted_buckets: BTreeMap::new(),
                    updated_at_ms: now_ms,
                };
                (name.clone(), Entry { reset_tz, account })
            })
            .collect();
        Self { entries }
    }

    /// Take the records of the retained accounts body for every profile still
    /// configured with the same kind, then roll.
    ///
    /// A record for a profile no longer configured, or configured as the other
    /// kind, is dropped and the profile keeps its zero record. `budget_usd`
    /// always comes from config.
    ///
    /// # Panics
    ///
    /// If `body` does not decode as this version's accounts body, or carries an
    /// API-key record this ledger could not have written: no turn count, or a
    /// spend that is negative or not finite. There is no migration between
    /// versions; the operator clears the channel.
    pub fn seed(&mut self, body: &str, now_ms: u64) {
        let retained: AgentAccounts = decode(body).unwrap_or_else(|e: AgentDecodeError| {
            panic!(
                "the retained accounts body does not decode ({e}); clear the accounts channel \
                 to start every profile at zero"
            )
        });
        for record in retained.accounts {
            if record.account_kind == AccountKind::ApiKey {
                assert!(
                    record.turns.is_some() && record.usd.is_finite() && record.usd >= 0.0,
                    "the retained accounts body's record for `{}` has usd {} and turns {:?}; an \
                     API-key record counts its turns and spends a finite, non-negative amount. \
                     Clear the accounts channel to start every profile at zero",
                    record.account,
                    record.usd,
                    record.turns
                );
            }
            let Some(entry) = self.entries.get_mut(&record.account) else {
                continue;
            };
            if entry.account.account_kind != record.account_kind {
                continue;
            }
            let budget_usd = entry.account.budget_usd;
            entry.account = AgentAccount {
                budget_usd,
                ..record
            };
        }
        self.roll(now_ms);
    }

    /// Start a new period on every key whose period has ended, and drop every
    /// bucket flag whose `until_ms` has passed.
    pub fn roll(&mut self, now_ms: u64) {
        for entry in self.entries.values_mut() {
            let account = &mut entry.account;
            if let Some(tz) = entry.reset_tz {
                let current = period(tz, now_ms);
                if account.period != current {
                    account.period = current;
                    account.usd = 0.0;
                    account.turns = Some(0);
                    account.exhausted = None;
                    account.exhausted_buckets.clear();
                    account.updated_at_ms = now_ms;
                }
            }
            let before = account.exhausted_buckets.len();
            account
                .exhausted_buckets
                .retain(|_, flag| flag.until_ms.is_none_or(|until| until > now_ms));
            if account.exhausted_buckets.len() != before {
                account.updated_at_ms = now_ms;
            }
        }
    }

    /// Charge one turn to an API-key profile.
    ///
    /// The cost is added and the turn counted. A refusal becomes the account's
    /// `exhausted` flag, replacing any in force. A turn that succeeded removes
    /// the flag from this record: it is the evidence the key works again. An
    /// error that is not a refusal leaves flags as they are.
    ///
    /// # Panics
    ///
    /// If `profile` is not a configured API-key profile, or `cost` is negative
    /// or not finite. The caller routes by the profile's kind and computes the
    /// cost; either being wrong is a host bug.
    pub fn turn(&mut self, profile: &str, outcome: TurnOutcome, now_ms: u64) {
        self.roll(now_ms);
        assert!(
            outcome.cost.is_finite() && outcome.cost >= 0.0,
            "turn under claude_profile `{profile}` cost {} — a turn's cost is finite and not \
             negative",
            outcome.cost
        );
        let account = self.account_mut(profile, AccountKind::ApiKey);
        account.usd += outcome.cost;
        let turns = account
            .turns
            .as_mut()
            .unwrap_or_else(|| panic!("API-key account `{profile}` has no turn count"));
        *turns += 1;
        match outcome.refusal {
            Some(flag) => account.exhausted = Some(flag),
            // TODO(claude-key-flag-operator-clear): a successful turn is the
            // only thing besides the period rollover that removes the flag.
            None if !outcome.is_error => account.exhausted = None,
            None => {}
        }
        account.updated_at_ms = now_ms;
    }

    /// Record a refusal in one rate-limit bucket of a subscription profile,
    /// replacing any flag already in force on that bucket.
    ///
    /// # Panics
    ///
    /// If `profile` is not a configured subscription profile, or `flag` has no
    /// `until_ms`: a bucket flag always says when it lifts.
    pub fn flag_bucket(&mut self, profile: &str, bucket: &str, flag: AccountFlag, now_ms: u64) {
        self.roll(now_ms);
        assert!(
            flag.until_ms.is_some(),
            "bucket flag `{bucket}` on claude_profile `{profile}` has no until_ms"
        );
        let account = self.account_mut(profile, AccountKind::Subscription);
        account.exhausted_buckets.insert(bucket.to_string(), flag);
        account.updated_at_ms = now_ms;
    }

    /// Every record, ascending by profile name, as of `now_ms`.
    pub fn snapshot(&mut self, now_ms: u64) -> AgentAccounts {
        self.roll(now_ms);
        AgentAccounts {
            at_ms: now_ms,
            accounts: self
                .entries
                .values()
                .map(|entry| entry.account.clone())
                .collect(),
        }
    }

    /// The record of `profile`, which must be of `kind`.
    fn account_mut(&mut self, profile: &str, kind: AccountKind) -> &mut AgentAccount {
        let entry = self
            .entries
            .get_mut(profile)
            .unwrap_or_else(|| panic!("claude_profile `{profile}` is not configured"));
        assert_eq!(
            entry.account.account_kind,
            kind,
            "claude_profile `{profile}` is {}, not {}",
            entry.account.account_kind.as_str(),
            kind.as_str()
        );
        &mut entry.account
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use brenn_envelope::agent::{bucket, encode};
    use brenn_lib::config::{ClaudeCredential, KeyProfileSettings, SecretString};

    /// 2026-11-12T12:00:00Z.
    const NOV_12: u64 = 1_794_484_800_000;
    const HOUR: u64 = 3_600_000;

    fn key_profile(tz: chrono_tz::Tz, budget_usd: Option<f64>) -> ClaudeProfile {
        ClaudeProfile {
            credential: ClaudeCredential::ApiKey {
                secret: SecretString::new("sk-test".to_string()),
                settings: KeyProfileSettings {
                    budget_usd,
                    reset_tz: tz,
                    exhausted_markers: vec!["credit balance is too low".to_string()],
                },
            },
            expires: None,
        }
    }

    fn token_profile() -> ClaudeProfile {
        ClaudeProfile {
            credential: ClaudeCredential::OauthToken(SecretString::new("tok".to_string())),
            expires: None,
        }
    }

    fn profiles() -> BTreeMap<String, ClaudeProfile> {
        BTreeMap::from([
            ("personal".to_string(), token_profile()),
            (
                "work-key".to_string(),
                key_profile(chrono_tz::UTC, Some(50.0)),
            ),
        ])
    }

    fn account(ledger: &mut AccountLedger, name: &str, now_ms: u64) -> AgentAccount {
        ledger
            .snapshot(now_ms)
            .accounts
            .into_iter()
            .find(|a| a.account == name)
            .unwrap_or_else(|| panic!("no record for {name}"))
    }

    fn refusal(at_ms: u64) -> AccountFlag {
        AccountFlag {
            at_ms,
            session: Some("7".to_string()),
            message: Some("Credit balance is too low".to_string()),
            until_ms: None,
            kind: None,
        }
    }

    fn bucket_flag(at_ms: u64, until_ms: u64) -> AccountFlag {
        AccountFlag {
            at_ms,
            session: Some("7".to_string()),
            message: None,
            until_ms: Some(until_ms),
            kind: Some("five_hour".to_string()),
        }
    }

    fn ok_turn(cost: f64) -> TurnOutcome {
        TurnOutcome {
            cost,
            is_error: false,
            refusal: None,
        }
    }

    #[test]
    fn a_new_ledger_holds_every_profile_at_zero() {
        let mut ledger = AccountLedger::new(&profiles(), NOV_12);
        let snapshot = ledger.snapshot(NOV_12);
        assert_eq!(snapshot.at_ms, NOV_12);
        let names: Vec<_> = snapshot
            .accounts
            .iter()
            .map(|a| a.account.as_str())
            .collect();
        assert_eq!(names, ["personal", "work-key"]);

        let personal = &snapshot.accounts[0];
        assert_eq!(personal.account_kind, AccountKind::Subscription);
        assert_eq!(personal.period, NO_PERIOD);
        assert_eq!(personal.usd, 0.0);
        assert_eq!(personal.turns, None);
        assert_eq!(personal.budget_usd, None);

        let key = &snapshot.accounts[1];
        assert_eq!(key.account_kind, AccountKind::ApiKey);
        assert_eq!(key.period, "2026-11");
        assert_eq!(key.turns, Some(0));
        assert_eq!(key.budget_usd, Some(50.0));
        assert_eq!(key.exhausted, None);
    }

    #[test]
    fn a_turn_adds_its_cost_and_counts_and_a_zero_cost_turn_counts() {
        let mut ledger = AccountLedger::new(&profiles(), NOV_12);
        ledger.turn("work-key", ok_turn(0.25), NOV_12 + 1);
        ledger.turn("work-key", ok_turn(0.0), NOV_12 + 2);
        let key = account(&mut ledger, "work-key", NOV_12 + 3);
        assert_eq!(key.usd, 0.25);
        assert_eq!(key.turns, Some(2));
        assert_eq!(key.updated_at_ms, NOV_12 + 2);
    }

    #[test]
    fn a_refused_turn_sets_the_flag_and_replaces_one_in_force() {
        let mut ledger = AccountLedger::new(&profiles(), NOV_12);
        let refused = |at| TurnOutcome {
            cost: 0.0,
            is_error: true,
            refusal: Some(refusal(at)),
        };
        ledger.turn("work-key", refused(NOV_12 + 1), NOV_12 + 1);
        assert_eq!(
            account(&mut ledger, "work-key", NOV_12 + 1).exhausted,
            Some(refusal(NOV_12 + 1))
        );
        ledger.turn("work-key", refused(NOV_12 + 2), NOV_12 + 2);
        let key = account(&mut ledger, "work-key", NOV_12 + 2);
        assert_eq!(key.exhausted, Some(refusal(NOV_12 + 2)));
        assert_eq!(key.turns, Some(2));
    }

    #[test]
    fn a_later_successful_turn_removes_the_flag() {
        let mut ledger = AccountLedger::new(&profiles(), NOV_12);
        ledger.turn(
            "work-key",
            TurnOutcome {
                cost: 0.0,
                is_error: true,
                refusal: Some(refusal(NOV_12)),
            },
            NOV_12,
        );
        ledger.turn("work-key", ok_turn(0.1), NOV_12 + 1);
        assert_eq!(account(&mut ledger, "work-key", NOV_12 + 1).exhausted, None);
    }

    #[test]
    fn an_error_that_is_not_a_refusal_neither_sets_nor_removes() {
        let mut ledger = AccountLedger::new(&profiles(), NOV_12);
        let other_error = TurnOutcome {
            cost: 0.05,
            is_error: true,
            refusal: None,
        };
        ledger.turn("work-key", other_error.clone(), NOV_12);
        assert_eq!(account(&mut ledger, "work-key", NOV_12).exhausted, None);

        ledger.turn(
            "work-key",
            TurnOutcome {
                cost: 0.0,
                is_error: true,
                refusal: Some(refusal(NOV_12 + 1)),
            },
            NOV_12 + 1,
        );
        ledger.turn("work-key", other_error, NOV_12 + 2);
        let key = account(&mut ledger, "work-key", NOV_12 + 2);
        assert_eq!(key.exhausted, Some(refusal(NOV_12 + 1)));
        assert_eq!(key.usd, 0.1);
    }

    /// The period is the calendar month in the profile's zone, not UTC's: at
    /// 2026-12-01T03:00Z it is still November in Chicago, and December arrives
    /// there at 06:00Z.
    #[test]
    fn the_period_rolls_at_the_month_boundary_of_the_profiles_zone() {
        let chicago = BTreeMap::from([(
            "work-key".to_string(),
            key_profile(chrono_tz::America::Chicago, None),
        )]);
        // 2026-12-01T03:00:00Z
        let dec_1_3z = 1_796_094_000_000;
        let mut ledger = AccountLedger::new(&chicago, NOV_12);
        ledger.turn(
            "work-key",
            TurnOutcome {
                cost: 2.0,
                is_error: true,
                refusal: Some(refusal(NOV_12)),
            },
            NOV_12,
        );

        let before = account(&mut ledger, "work-key", dec_1_3z);
        assert_eq!(before.period, "2026-11");
        assert_eq!(before.usd, 2.0);

        let after = account(&mut ledger, "work-key", dec_1_3z + 4 * HOUR);
        assert_eq!(after.period, "2026-12");
        assert_eq!(after.usd, 0.0);
        assert_eq!(after.turns, Some(0));
        assert_eq!(after.exhausted, None);
        assert_eq!(after.updated_at_ms, dec_1_3z + 4 * HOUR);
    }

    #[test]
    fn a_bucket_flag_is_dropped_once_its_until_passes() {
        let mut ledger = AccountLedger::new(&profiles(), NOV_12);
        ledger.flag_bucket(
            "personal",
            bucket::FIVE_HOUR,
            bucket_flag(NOV_12, NOV_12 + HOUR),
            NOV_12,
        );
        let flagged = account(&mut ledger, "personal", NOV_12 + HOUR - 1);
        assert_eq!(
            flagged.exhausted_buckets.get(bucket::FIVE_HOUR),
            Some(&bucket_flag(NOV_12, NOV_12 + HOUR))
        );
        assert_eq!(flagged.period, NO_PERIOD);

        let lifted = account(&mut ledger, "personal", NOV_12 + HOUR);
        assert!(lifted.exhausted_buckets.is_empty());
        assert_eq!(lifted.updated_at_ms, NOV_12 + HOUR);
    }

    /// The wire type has no `cleared_at`, and no operation reaches past the
    /// record it names.
    #[test]
    fn no_operation_produces_a_cleared_at() {
        let mut ledger = AccountLedger::new(&profiles(), NOV_12);
        ledger.flag_bucket(
            "personal",
            bucket::GENERAL,
            bucket_flag(NOV_12, NOV_12 + HOUR),
            NOV_12,
        );
        ledger.turn(
            "work-key",
            TurnOutcome {
                cost: 0.0,
                is_error: true,
                refusal: Some(refusal(NOV_12)),
            },
            NOV_12,
        );
        ledger.turn("work-key", ok_turn(0.1), NOV_12 + 1);
        let body = encode(&ledger.snapshot(NOV_12 + 2));
        assert!(!body.contains("cleared"), "{body}");
    }

    #[test]
    fn seed_restores_records_and_drops_unknown_profiles_and_kind_changes() {
        let mut first = AccountLedger::new(&profiles(), NOV_12);
        first.turn("work-key", ok_turn(1.5), NOV_12);
        first.flag_bucket(
            "personal",
            bucket::FIVE_HOUR,
            bucket_flag(NOV_12, NOV_12 + HOUR),
            NOV_12,
        );
        let mut retained = first.snapshot(NOV_12);
        retained.accounts.push(AgentAccount {
            account: "gone".to_string(),
            ..retained.accounts[1].clone()
        });
        let body = encode(&retained);

        // `personal` is now a key and `work-key` carries a new budget.
        let reconfigured = BTreeMap::from([
            ("personal".to_string(), key_profile(chrono_tz::UTC, None)),
            (
                "work-key".to_string(),
                key_profile(chrono_tz::UTC, Some(80.0)),
            ),
        ]);
        let mut ledger = AccountLedger::new(&reconfigured, NOV_12 + 1);
        ledger.seed(&body, NOV_12 + 1);
        let snapshot = ledger.snapshot(NOV_12 + 1);
        let names: Vec<_> = snapshot
            .accounts
            .iter()
            .map(|a| a.account.as_str())
            .collect();
        assert_eq!(names, ["personal", "work-key"]);

        let personal = &snapshot.accounts[0];
        assert_eq!(personal.account_kind, AccountKind::ApiKey);
        assert_eq!(personal.usd, 0.0);
        assert!(personal.exhausted_buckets.is_empty());

        let key = &snapshot.accounts[1];
        assert_eq!(key.usd, 1.5);
        assert_eq!(key.turns, Some(1));
        assert_eq!(key.budget_usd, Some(80.0));
    }

    #[test]
    fn seed_rolls_a_record_from_an_earlier_period() {
        let mut first = AccountLedger::new(&profiles(), NOV_12);
        first.turn("work-key", ok_turn(1.5), NOV_12);
        let body = encode(&first.snapshot(NOV_12));

        // 2026-12-02T12:00:00Z
        let dec_2 = NOV_12 + 20 * 24 * HOUR;
        let mut ledger = AccountLedger::new(&profiles(), dec_2);
        ledger.seed(&body, dec_2);
        let key = account(&mut ledger, "work-key", dec_2);
        assert_eq!(key.period, "2026-12");
        assert_eq!(key.usd, 0.0);
    }

    #[test]
    #[should_panic(expected = "does not decode")]
    fn seed_panics_on_a_body_that_does_not_parse() {
        AccountLedger::new(&profiles(), NOV_12).seed("{", NOV_12);
    }

    #[test]
    #[should_panic(expected = "version 2")]
    fn seed_panics_on_another_version() {
        AccountLedger::new(&profiles(), NOV_12).seed(r#"{"v":2,"at_ms":1,"accounts":[]}"#, NOV_12);
    }

    /// The key's record as this ledger writes it, with `edit` applied, as a
    /// retained body.
    fn retained_with(edit: impl FnOnce(&mut AgentAccount)) -> String {
        let mut first = AccountLedger::new(&profiles(), NOV_12);
        first.turn("work-key", ok_turn(1.5), NOV_12);
        let mut retained = first.snapshot(NOV_12);
        edit(
            retained
                .accounts
                .iter_mut()
                .find(|a| a.account == "work-key")
                .expect("the key's record"),
        );
        encode(&retained)
    }

    #[test]
    #[should_panic(expected = "counts its turns")]
    fn seed_panics_on_a_key_record_without_a_turn_count() {
        let body = retained_with(|key| key.turns = None);
        AccountLedger::new(&profiles(), NOV_12).seed(&body, NOV_12);
    }

    #[test]
    #[should_panic(expected = "finite, non-negative")]
    fn seed_panics_on_a_key_record_with_negative_spend() {
        let body = retained_with(|key| key.usd = -1.0);
        AccountLedger::new(&profiles(), NOV_12).seed(&body, NOV_12);
    }

    #[test]
    #[should_panic(expected = "is subscription, not api-key")]
    fn a_turn_on_a_subscription_profile_panics() {
        AccountLedger::new(&profiles(), NOV_12).turn("personal", ok_turn(0.1), NOV_12);
    }

    #[test]
    #[should_panic(expected = "is api-key, not subscription")]
    fn a_bucket_flag_on_a_key_profile_panics() {
        AccountLedger::new(&profiles(), NOV_12).flag_bucket(
            "work-key",
            bucket::FIVE_HOUR,
            bucket_flag(NOV_12, NOV_12 + HOUR),
            NOV_12,
        );
    }

    #[test]
    #[should_panic(expected = "has no until_ms")]
    fn a_bucket_flag_without_until_panics() {
        AccountLedger::new(&profiles(), NOV_12).flag_bucket(
            "personal",
            bucket::FIVE_HOUR,
            refusal(NOV_12),
            NOV_12,
        );
    }

    #[test]
    #[should_panic(expected = "is not configured")]
    fn a_turn_on_an_unknown_profile_panics() {
        AccountLedger::new(&profiles(), NOV_12).turn("nobody", ok_turn(0.1), NOV_12);
    }

    #[test]
    #[should_panic(expected = "finite and not negative")]
    fn a_negative_cost_panics() {
        AccountLedger::new(&profiles(), NOV_12).turn("work-key", ok_turn(-0.1), NOV_12);
    }
}
