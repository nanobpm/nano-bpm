//! Canonical ISO-8601 duration parsing (L0 leaf: depends on nothing internal).
//!
//! Historically two hand-rolled parsers existed and had already drifted:
//!
//! - `bpmn::parse_iso8601_duration` — milliseconds, unsigned, weeks supported,
//!   **no** fractional seconds.
//! - `feel::temporal::DtDuration::parse` — nanoseconds, signed, fractional
//!   seconds supported, **no** weeks.
//!
//! The concrete symptom: `PT1.5S` was a valid FEEL duration but a silently
//! `None` BPMN timer.
//!
//! This module is the single source of truth. The ISO-8601 grammar is parsed
//! once, into nanoseconds, and the per-layer width (ms vs ns) and the policy
//! decisions (sign? weeks? fractions?) become **explicit, documented choices**
//! via [`DurationForm`]. The two callers delegate here; their historical
//! behavioural differences are preserved as distinct `DurationForm` presets
//! ([`DurationForm::BPMN_TIMER`], [`DurationForm::FEEL_DAYTIME`]).
//!
//! Accepted/rejected forms are aligned with the Camunda/Zeebe timer-expression
//! parsing at `camunda/camunda`: the date-portion years (`Y`) and months (`M`)
//! are calendar-ambiguous and rejected; weeks (`W`), days (`D`), hours (`H`),
//! minutes (`M`) and seconds (`S`) are accepted.
//!
//! One historical divergence is resolved here as an *intended unification*
//! rather than preserved. The old `bpmn::parse_iso8601_duration` scanned
//! character-by-character with a single accumulator and did **not** flush its
//! pending digits at the `T` boundary, so a malformed value like `P1T2H` (a
//! date-part `1` with no unit designator) concatenated across `T` into `12H`
//! and was silently accepted as 12 hours; a repeated `T` (e.g. `PT1T2H`)
//! behaved the same way. The old `feel::temporal::DtDuration::parse` already
//! split on `T` and **rejected** `P1T2H`. The two parsers had therefore already
//! drifted, so unification cannot preserve both. This canonical parser splits
//! cleanly on `T` and rejects trailing unit-less digits in either part, so
//! `P1T2H` and repeated-`T` forms are rejected — matching Camunda: its
//! `io.camunda.zeebe.model.bpmn.util.time.Interval.parse` splits on the `T`
//! index and delegates to `java.time.Period.parse`/`Duration.parse`, and
//! `Period.parse("P1")` throws `DateTimeParseException` (a bare digit with no
//! `Y`/`M`/`W`/`D` designator), so Camunda rejects `P1T2H` too. Rejecting
//! malformed ISO-8601 durations is Camunda's documented contract (see its
//! `IntervalTest`), so the legacy BPMN 12h reading was the non-conformant quirk.

const NANOS_PER_SEC: i128 = 1_000_000_000;
const NANOS_PER_MILLI: i128 = 1_000_000;
const SECS_PER_DAY: i128 = 86_400;
const SECS_PER_HOUR: i128 = 3_600;
const SECS_PER_MINUTE: i128 = 60;
const DAYS_PER_WEEK: i128 = 7;

/// Which optional ISO-8601 duration forms a caller accepts.
///
/// The core grammar is shared; these three flags are the documented points at
/// which BPMN timers and FEEL day-time durations historically diverged.
#[derive(Clone, Copy, Debug)]
pub struct DurationForm {
    /// Accept a leading `+`/`-` sign (FEEL: yes; BPMN timers: no).
    pub allow_sign: bool,
    /// Accept the `nW` weeks field in the date part (BPMN timers: yes; FEEL: no).
    pub allow_weeks: bool,
    /// Accept a fractional seconds field, e.g. `PT1.5S` (FEEL: yes; BPMN: no).
    pub allow_fraction: bool,
}

impl DurationForm {
    /// BPMN timer policy: unsigned, weeks allowed, **no** fractional seconds.
    pub const BPMN_TIMER: DurationForm = DurationForm {
        allow_sign: false,
        allow_weeks: true,
        allow_fraction: false,
    };

    /// FEEL day-time-duration policy: signed, **no** weeks, fractional seconds.
    pub const FEEL_DAYTIME: DurationForm = DurationForm {
        allow_sign: true,
        allow_weeks: false,
        allow_fraction: true,
    };
}

/// Parses an ISO-8601 duration (e.g. `PT5S`, `P1DT6H30M`, `P1W`) into a signed
/// total of **nanoseconds**, applying `form`.
///
/// Supports weeks (when allowed), days, hours, minutes and seconds. The
/// date-portion years/months are calendar-ambiguous and always rejected.
/// Returns `None` if the string is not a recognisable duration, or if the
/// total overflows `i128`.
pub fn parse_duration_nanos(raw: &str, form: DurationForm) -> Option<i128> {
    let (neg, body) = if form.allow_sign {
        strip_sign(raw)
    } else {
        (false, raw)
    };
    let body = body.strip_prefix('P')?;
    if body.is_empty() {
        return None;
    }
    let (date_part, time_part) = match body.find('T') {
        Some(idx) => (&body[..idx], &body[idx + 1..]),
        None => (body, ""),
    };

    let mut nanos: i128 = 0;
    let mut saw_unit = false;
    let mut num = String::new();

    // Date part: `D` always, `W` when allowed. `Y`/`M` are calendar-ambiguous.
    for c in date_part.chars() {
        match c {
            '0'..='9' => num.push(c),
            'W' if form.allow_weeks => {
                nanos = add_unit(nanos, &num, DAYS_PER_WEEK * SECS_PER_DAY * NANOS_PER_SEC)?;
                num.clear();
                saw_unit = true;
            }
            'D' => {
                nanos = add_unit(nanos, &num, SECS_PER_DAY * NANOS_PER_SEC)?;
                num.clear();
                saw_unit = true;
            }
            _ => return None,
        }
    }
    if !num.is_empty() {
        return None;
    }

    // Time part: `H`, `M`, `S`; `S` may carry a fraction when allowed.
    for c in time_part.chars() {
        match c {
            '0'..='9' => num.push(c),
            '.' if form.allow_fraction => num.push(c),
            'H' => {
                nanos = add_unit(nanos, &num, SECS_PER_HOUR * NANOS_PER_SEC)?;
                num.clear();
                saw_unit = true;
            }
            'M' => {
                nanos = add_unit(nanos, &num, SECS_PER_MINUTE * NANOS_PER_SEC)?;
                num.clear();
                saw_unit = true;
            }
            'S' => {
                nanos = add_seconds(nanos, &num, form.allow_fraction)?;
                num.clear();
                saw_unit = true;
            }
            _ => return None,
        }
    }

    // Trailing digits without a unit, or no units at all, are invalid.
    if !num.is_empty() || !saw_unit {
        return None;
    }
    Some(if neg { -nanos } else { nanos })
}

/// BPMN timer helper: parses an ISO-8601 duration into whole **milliseconds**
/// under [`DurationForm::BPMN_TIMER`]. Leading/trailing whitespace is trimmed.
/// Returns `None` if the value is not a recognisable duration or does not fit
/// in `u64` milliseconds.
pub fn parse_duration_millis(raw: &str) -> Option<u64> {
    let nanos = parse_duration_nanos(raw.trim(), DurationForm::BPMN_TIMER)?;
    u64::try_from(nanos / NANOS_PER_MILLI).ok()
}

/// Accumulates an integer-valued unit (`num` × `factor` nanoseconds). Returns
/// `None` on an empty/invalid number or on `i128` overflow.
fn add_unit(nanos: i128, num: &str, factor: i128) -> Option<i128> {
    if num.is_empty() {
        return None;
    }
    let value: i128 = num.parse().ok()?;
    nanos.checked_add(value.checked_mul(factor)?)
}

/// Accumulates a seconds field, which may carry a fraction when `allow_fraction`.
/// Returns `None` on an empty/invalid number or on `i128` overflow.
fn add_seconds(nanos: i128, num: &str, allow_fraction: bool) -> Option<i128> {
    if num.is_empty() {
        return None;
    }
    if allow_fraction {
        nanos.checked_add(parse_decimal_seconds_nanos(num)?)
    } else {
        let value: i128 = num.parse().ok()?;
        nanos.checked_add(value.checked_mul(NANOS_PER_SEC)?)
    }
}

/// Parses a (non-negative) decimal seconds field such as `"1.5"` directly into
/// integer nanoseconds, rounding any digits beyond nanosecond precision
/// half-up. Parsing the decimal exactly (rather than via `f64`) avoids both
/// precision loss for large integer second counts and the saturating `as i128`
/// conversion that would silently clamp an overflow to `i128::MAX` instead of
/// reporting it. Returns `None` on a malformed number or on `i128` overflow.
fn parse_decimal_seconds_nanos(num: &str) -> Option<i128> {
    let (int_str, frac_str) = match num.split_once('.') {
        Some((i, f)) => (i, f),
        None => (num, ""),
    };
    // A single decimal point at most, digits only, and at least one digit.
    if frac_str.contains('.')
        || !int_str.bytes().all(|b| b.is_ascii_digit())
        || !frac_str.bytes().all(|b| b.is_ascii_digit())
        || (int_str.is_empty() && frac_str.is_empty())
    {
        return None;
    }

    let int_secs: i128 = if int_str.is_empty() {
        0
    } else {
        int_str.parse().ok()?
    };
    let mut total = int_secs.checked_mul(NANOS_PER_SEC)?;

    if !frac_str.is_empty() {
        // Take up to 9 fractional digits as nanoseconds; round on the 10th.
        let mut nanos_frac: i128 = 0;
        let mut digits = frac_str.bytes();
        for _ in 0..9 {
            let d = digits.next().map(|b| (b - b'0') as i128).unwrap_or(0);
            nanos_frac = nanos_frac * 10 + d;
        }
        if let Some(next) = digits.next() {
            if next - b'0' >= 5 {
                nanos_frac += 1;
            }
        }
        total = total.checked_add(nanos_frac)?;
    }

    Some(total)
}

/// Strips an optional leading `+`/`-` sign, returning `(is_negative, rest)`.
fn strip_sign(s: &str) -> (bool, &str) {
    if let Some(rest) = s.strip_prefix('-') {
        (true, rest)
    } else if let Some(rest) = s.strip_prefix('+') {
        (false, rest)
    } else {
        (false, s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bpmn_millis_basic() {
        assert_eq!(parse_duration_millis("PT5S"), Some(5_000));
        assert_eq!(parse_duration_millis("PT1M"), Some(60_000));
        assert_eq!(parse_duration_millis("PT2H"), Some(7_200_000));
        assert_eq!(parse_duration_millis("P1D"), Some(86_400_000));
        assert_eq!(parse_duration_millis("P1W"), Some(604_800_000));
        assert_eq!(parse_duration_millis("P1DT6H30M"), Some(109_800_000));
        assert_eq!(parse_duration_millis("  PT5S  "), Some(5_000));
    }

    #[test]
    fn bpmn_millis_rejects() {
        // Empty / missing units.
        assert_eq!(parse_duration_millis("P"), None);
        assert_eq!(parse_duration_millis("PT"), None);
        assert_eq!(parse_duration_millis("5S"), None);
        assert_eq!(parse_duration_millis("PT5"), None);
        // Calendar-ambiguous date fields.
        assert_eq!(parse_duration_millis("P1Y"), None);
        assert_eq!(parse_duration_millis("P1M"), None);
        // BPMN policy: no sign, no fractions.
        assert_eq!(parse_duration_millis("-PT5S"), None);
        assert_eq!(parse_duration_millis("PT1.5S"), None);
    }

    #[test]
    fn feel_nanos_fractions_and_sign() {
        let f = DurationForm::FEEL_DAYTIME;
        // The drift symptom: PT1.5S is valid under the FEEL policy.
        assert_eq!(parse_duration_nanos("PT1.5S", f), Some(1_500_000_000));
        assert_eq!(parse_duration_nanos("PT1M", f), Some(60_000_000_000));
        assert_eq!(parse_duration_nanos("P1DT2H", f), Some(93_600_000_000_000));
        assert_eq!(parse_duration_nanos("-PT1S", f), Some(-1_000_000_000));
        assert_eq!(parse_duration_nanos("+PT1S", f), Some(1_000_000_000));
        // FEEL policy: weeks are not allowed.
        assert_eq!(parse_duration_nanos("P1W", f), None);
    }

    #[test]
    fn policy_divergence_is_explicit() {
        // Same input, two policies, two answers — the documented divergences.
        assert_eq!(
            parse_duration_nanos("PT1.5S", DurationForm::BPMN_TIMER),
            None
        );
        assert_eq!(
            parse_duration_nanos("PT1.5S", DurationForm::FEEL_DAYTIME),
            Some(1_500_000_000)
        );
        assert_eq!(
            parse_duration_nanos("P1W", DurationForm::BPMN_TIMER),
            Some(604_800_000_000_000)
        );
        assert_eq!(
            parse_duration_nanos("P1W", DurationForm::FEEL_DAYTIME),
            None
        );
        assert_eq!(
            parse_duration_nanos("-PT1S", DurationForm::BPMN_TIMER),
            None
        );
        assert_eq!(
            parse_duration_nanos("-PT1S", DurationForm::FEEL_DAYTIME),
            Some(-1_000_000_000)
        );
    }

    #[test]
    fn millis_overflow_is_none() {
        // Enormous day count overflows u64 milliseconds.
        assert_eq!(parse_duration_millis("P100000000000000000D"), None);
    }

    #[test]
    fn fractional_seconds_are_exact_not_f64() {
        let f = DurationForm::FEEL_DAYTIME;
        // A large integer second count that is not representable exactly in f64
        // must convert exactly, not round to the nearest double.
        assert_eq!(
            parse_duration_nanos("PT9007199254740993S", f),
            Some(9_007_199_254_740_993 * NANOS_PER_SEC)
        );
        // Full nanosecond precision is preserved.
        assert_eq!(parse_duration_nanos("PT0.123456789S", f), Some(123_456_789));
        // Leading/trailing edge fraction forms.
        assert_eq!(parse_duration_nanos("PT1.S", f), Some(1_000_000_000));
        assert_eq!(parse_duration_nanos("PT0.5S", f), Some(500_000_000));
    }

    #[test]
    fn fractional_seconds_round_half_up_beyond_nanos() {
        let f = DurationForm::FEEL_DAYTIME;
        // 10th fractional digit >= 5 rounds up; < 5 truncates.
        assert_eq!(
            parse_duration_nanos("PT0.1234567895S", f),
            Some(123_456_790)
        );
        assert_eq!(
            parse_duration_nanos("PT0.1234567891S", f),
            Some(123_456_789)
        );
        // Rounding may carry into the whole-second boundary.
        assert_eq!(
            parse_duration_nanos("PT0.9999999995S", f),
            Some(1_000_000_000)
        );
    }

    #[test]
    fn fractional_seconds_overflow_is_none() {
        let f = DurationForm::FEEL_DAYTIME;
        // A seconds count large enough to overflow i128 nanoseconds must report
        // overflow (None), not saturate to Some(i128::MAX).
        assert_eq!(
            parse_duration_nanos("PT99999999999999999999999999999999999999S", f),
            None
        );
    }

    #[test]
    fn fractional_seconds_reject_malformed() {
        let f = DurationForm::FEEL_DAYTIME;
        assert_eq!(parse_duration_nanos("PT1.2.3S", f), None);
        assert_eq!(parse_duration_nanos("PT.S", f), None);
    }

    #[test]
    fn malformed_iso8601_across_t_boundary_is_rejected() {
        // Defect class: a date/time part digit with no unit designator must
        // reject, matching Camunda's `Interval.parse` (`Period.parse("P1")`
        // throws). The legacy `bpmn::parse_iso8601_duration` accepted `P1T2H`
        // as 12h by concatenating digits across the `T` boundary; that quirk is
        // deliberately not preserved. This guards the whole class, not just the
        // single input — every form must reject under both policies.
        for form in [DurationForm::BPMN_TIMER, DurationForm::FEEL_DAYTIME] {
            // Bare date-part digit with no designator, concatenated across `T`.
            assert_eq!(parse_duration_nanos("P1T2H", form), None);
            // Repeated `T` marker.
            assert_eq!(parse_duration_nanos("PT1T2H", form), None);
            // Same class in the time part: a trailing unit-less digit.
            assert_eq!(parse_duration_nanos("PT1H2", form), None);
        }
        // And through the BPMN millis entry point callers actually use.
        assert_eq!(parse_duration_millis("P1T2H"), None);
    }
}
