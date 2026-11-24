//! BPMN `timeCycle` schedules — Zeebe parity (#1878).
//!
//! A `timeCycle` value (a literal, or the string a FEEL expression evaluates
//! to) is one of three forms. Zeebe's `CatchEventTransformer` /
//! `TimerCatchEventExpressionValidator` pick the form by a single rule —
//! *starts with `R` → ISO-8601 repeating interval, otherwise cron* — and this
//! module mirrors it:
//!
//! - **ISO-8601 repeating interval** (`RepeatingInterval.parse`):
//!   `R[n]/<duration>` or `R[n]/<start>/<duration>`. With no start the first
//!   due date is `from + duration`; with a start it is `max(start, from)` — a
//!   start already in the past fires immediately (`Interval.toEpochMilli`).
//!   Zeebe splits the interval on its **last** `/` and parses the prefix as a
//!   `ZonedDateTime`, so the end-date form `R[n]/<duration>/<end>` is
//!   **rejected** by Zeebe and therefore here too. `R0` stays rejected (a
//!   cycle that never fires, #1596).
//! - **Cron** (`CronTimer`, cron-utils `CronType.SPRING53`): six fields
//!   `second minute hour day-of-month month day-of-week`, with `*`, `?`,
//!   lists, ranges, steps, month / weekday names, `L`, `L-n`, `nW`, `LW`,
//!   `nL` and `n#k`, plus the `@yearly`/`@annually`/`@monthly`/`@weekly`/
//!   `@daily`/`@midnight`/`@hourly` macros. A cron cycle is unbounded; its
//!   next due date is the first matching second strictly after `from`.
//!   Day-of-month and day-of-week must both match (Spring semantics).
//! - **Bare duration** (`PT1H`): a nano leniency kept for backward
//!   compatibility — Zeebe would try it as cron and reject it. It behaves as
//!   `R/PT1H`.
//!
//! Re-arming (`TimerTriggerProcessor.refreshTimer`): an interval cycle re-arms
//! at `previous due + duration` (never earlier than now, so fires missed while
//! the engine was not driven collapse into one), a cron cycle at its next
//! match strictly after `max(previous due, now)` — missed cron fires are
//! skipped, as in Zeebe.
//!
//! **Time zone.** Zeebe evaluates cron in the broker's `ZoneId.systemDefault()`.
//! The engine is deterministic and host-independent (ADR 0066), so cron is
//! evaluated in **UTC**: there is no DST shift, and a broker running in UTC
//! produces identical due dates. A start date must carry an explicit offset
//! (`Z` / `±hh:mm`), optionally followed by a bracketed zone id.

use crate::feel::temporal::{civil_from_days, days_from_civil, DateTime};

const MILLIS_PER_SEC: u64 = 1_000;
const SECS_PER_DAY: u64 = 86_400;

/// A parsed `timeCycle`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct TimerCycle {
    /// The `n` of `Rn/…`; `None` for unbounded (`R/…`, a bare duration, cron).
    pub(crate) repetitions: Option<u32>,
    pub(crate) schedule: Schedule,
}

/// How a cycle computes its due dates.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Schedule {
    /// A fixed period, optionally anchored at an explicit start instant.
    Interval {
        start_millis: Option<u64>,
        interval_millis: u64,
    },
    /// A Spring-style six-field cron expression.
    Cron(Box<Cron>),
}

impl TimerCycle {
    /// Parses a `timeCycle` string (see the module docs for the forms).
    pub(crate) fn parse(raw: &str) -> Option<TimerCycle> {
        let s = raw.trim();
        if s.starts_with('R') {
            let (repetitions, interval) = crate::temporal::split_repetitions(s)?;
            let (start_millis, duration) = match interval.rfind('/') {
                Some(idx) => (Some(parse_start(&interval[..idx])?), &interval[idx + 1..]),
                None => (None, interval),
            };
            return Some(TimerCycle {
                repetitions,
                schedule: Schedule::Interval {
                    start_millis,
                    interval_millis: crate::temporal::parse_duration_millis(duration)?,
                },
            });
        }
        if let Some(interval_millis) = crate::temporal::parse_duration_millis(s) {
            return Some(TimerCycle {
                repetitions: None,
                schedule: Schedule::Interval {
                    start_millis: None,
                    interval_millis,
                },
            });
        }
        Some(TimerCycle {
            repetitions: None,
            schedule: Schedule::Cron(Box::new(Cron::parse(s)?)),
        })
    }

    /// The fixed period between fires, or 0 for a cron cycle.
    pub(crate) fn interval_millis(&self) -> u64 {
        match &self.schedule {
            Schedule::Interval {
                interval_millis, ..
            } => *interval_millis,
            Schedule::Cron(_) => 0,
        }
    }

    /// The first due date of a cycle armed at `from` (Zeebe
    /// `Timer.getDueDate`), and its remaining-fire count. A cron expression
    /// that never matches again is due at `from` and fires once (Zeebe
    /// `CronTimer` sets `repetitions = 0` and returns `from`).
    pub(crate) fn first_due(&self, from: u64) -> (u64, Option<u32>) {
        match &self.schedule {
            Schedule::Interval {
                start_millis: Some(start),
                ..
            } => ((*start).max(from), self.repetitions),
            Schedule::Interval {
                start_millis: None,
                interval_millis,
            } => (from.saturating_add(*interval_millis), self.repetitions),
            Schedule::Cron(cron) => match cron.next_after(from) {
                Some(due) => (due, None),
                None => (from, Some(1)),
            },
        }
    }

    /// The due date a fired cycle re-arms for, given the fired timer's due
    /// date and the current time (Zeebe `TimerTriggerProcessor.refreshTimer`).
    pub(crate) fn next_due(&self, previous_due: u64, now: u64) -> u64 {
        match &self.schedule {
            Schedule::Interval {
                interval_millis, ..
            } => previous_due.saturating_add(*interval_millis).max(now),
            // A cron with no further match is parked at `u64::MAX` (never
            // due) rather than re-fired at `now`, which would loop forever.
            Schedule::Cron(cron) => cron.next_after(previous_due.max(now)).unwrap_or(u64::MAX),
        }
    }
}

/// Parses a cycle's start instant (`ZonedDateTime.parse`): an ISO-8601 date-time
/// with an explicit offset, optionally followed by a `[Zone/Id]`.
fn parse_start(raw: &str) -> Option<u64> {
    let body = match raw.find('[') {
        Some(idx) if raw.ends_with(']') => &raw[..idx],
        Some(_) => return None,
        None => raw,
    };
    if body.contains('@') {
        return None;
    }
    let (local, offset_secs) = split_start_offset(body)?;
    // FEEL's offset parser keeps only hours and minutes, so the local
    // date-time is read as UTC and the full (second-resolution) offset is
    // applied here.
    let dt = DateTime::parse(&format!("{local}Z"))?;
    let millis = dt.epoch_nanos() / 1_000_000 - i128::from(offset_secs) * 1000;
    // A start before the epoch is simply overdue: fire immediately.
    u64::try_from(millis.max(0)).ok()
}

/// Strict `ZonedDateTime` clock/offset validation for a cycle start
/// (`…Thh:mm[:ss[.f+]](Z|±hh:mm[:ss])`): FEEL's lenient date-time parser
/// would otherwise normalise `24:30`, `:60` or an offset `+99:99`.
/// Returns the body without its offset and the offset in seconds.
fn split_start_offset(body: &str) -> Option<(&str, i32)> {
    let (date, rest) = body.split_once('T')?;
    let (clock, offset) = rest.split_at(rest.find(['Z', '+', '-'])?);
    if !valid_start_syntax(clock, offset) {
        return None;
    }
    let local = &body[..date.len() + 1 + clock.len()];
    if offset == "Z" {
        return Some((local, 0));
    }
    let mut secs = 0i32;
    for (part, unit) in offset[1..].split(':').zip([3600, 60, 1]) {
        secs += part.parse::<i32>().ok()? * unit;
    }
    Some((local, if offset.starts_with('-') { -secs } else { secs }))
}

fn valid_start_syntax(clock: &str, offset: &str) -> bool {
    let two = |s: &str, max: u32| {
        s.len() == 2
            && s.bytes().all(|b| b.is_ascii_digit())
            && s.parse::<u32>().is_ok_and(|v| v <= max)
    };
    let (hms, frac) = match clock.split_once('.') {
        Some((h, f)) => (h, Some(f)),
        None => (clock, None),
    };
    if frac.is_some_and(|f| f.is_empty() || f.len() > 9 || !f.bytes().all(|b| b.is_ascii_digit())) {
        return false;
    }
    let parts: Vec<&str> = hms.split(':').collect();
    let clock_ok = match parts.as_slice() {
        [h, m] => frac.is_none() && two(h, 23) && two(m, 59),
        [h, m, s] => two(h, 23) && two(m, 59) && two(s, 59),
        _ => false,
    };
    if !clock_ok {
        return false;
    }
    if offset == "Z" {
        return true;
    }
    if !offset.starts_with(['+', '-']) {
        return false;
    }
    let parts: Vec<&str> = offset[1..].split(':').collect();
    match parts.as_slice() {
        [h, m] => two(h, 18) && two(m, 59) && !(h == &"18" && m != &"00"),
        [h, m, s] => {
            two(h, 18) && two(m, 59) && two(s, 59) && (h != &"18" || (m == &"00" && s == &"00"))
        }
        _ => false,
    }
}

/// A Spring-style (cron-utils `SPRING53`) six-field cron expression.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Cron {
    seconds: u64,
    minutes: u64,
    hours: u32,
    months: u16,
    dom: DayOfMonth,
    dow: DayOfWeek,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct DayOfMonth {
    /// Bit `d` set ⇒ day `d` (1..=31) matches.
    days: u32,
    /// `L` / `L-n`: offsets back from the last day of the month.
    last_offsets: Vec<u32>,
    /// `nW`: the weekday nearest to day `n` (within the month).
    nearest_weekday: Vec<u32>,
    /// `LW`: the last weekday (Mon–Fri) of the month.
    last_weekday: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct DayOfWeek {
    /// Bit `d` set ⇒ ISO weekday `d` (1 = Monday ..= 7 = Sunday) matches.
    days: u8,
    /// `nL`: the last given weekday of the month.
    last_of_month: u8,
    /// `n#k`: the k-th given weekday of the month.
    nth: Vec<(u32, u32)>,
}

const MONTH_NAMES: [&str; 12] = [
    "JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC",
];
// Spring numbering: SUN = 0 (also 7), MON = 1 .. SAT = 6.
const DAY_NAMES: [&str; 7] = ["SUN", "MON", "TUE", "WED", "THU", "FRI", "SAT"];

impl Cron {
    /// Parses a cron expression; `None` on any syntax or range error.
    pub(crate) fn parse(raw: &str) -> Option<Cron> {
        let expanded = match raw.trim().to_ascii_lowercase().as_str() {
            "@yearly" | "@annually" => "0 0 0 1 1 *".to_string(),
            "@monthly" => "0 0 0 1 * *".to_string(),
            "@weekly" => "0 0 0 * * 0".to_string(),
            "@daily" | "@midnight" => "0 0 0 * * *".to_string(),
            "@hourly" => "0 0 * * * *".to_string(),
            _ => raw.trim().to_ascii_uppercase(),
        };
        let fields: Vec<&str> = expanded.split_whitespace().collect();
        let [sec, min, hour, dom, month, dow] = fields.as_slice() else {
            return None;
        };
        Some(Cron {
            seconds: parse_set(sec, 0, 59, &[])?,
            minutes: parse_set(min, 0, 59, &[])?,
            hours: u32::try_from(parse_set(hour, 0, 23, &[])?).ok()?,
            months: u16::try_from(parse_calendar_set(month, 1, 12, &MONTH_NAMES)? >> 1).ok()?,
            dom: parse_dom(dom)?,
            dow: parse_dow(dow)?,
        })
    }

    /// The first matching instant (epoch millis, whole seconds) strictly after
    /// `from`, or `None` if the expression never matches within 400 years
    /// (the Gregorian cycle — every day/weekday combination recurs within it).
    pub(crate) fn next_after(&self, from: u64) -> Option<u64> {
        let start = from / MILLIS_PER_SEC + 1;
        let first_day = start / SECS_PER_DAY;
        let first_sod = (start % SECS_PER_DAY) as u32;
        for day in first_day..first_day + 146_097 {
            let (y, m, d) = civil_from_days(i64::try_from(day).ok()?);
            if self.months & (1 << (m - 1)) == 0 || !self.day_matches(y, m, d) {
                continue;
            }
            let floor = if day == first_day { first_sod } else { 0 };
            if let Some(sod) = self.first_time_at_or_after(floor) {
                return (day * SECS_PER_DAY + u64::from(sod)).checked_mul(MILLIS_PER_SEC);
            }
        }
        None
    }

    fn first_time_at_or_after(&self, floor: u32) -> Option<u32> {
        for h in 0..24u32 {
            if self.hours & (1 << h) == 0 || (h + 1) * 3600 <= floor {
                continue;
            }
            for m in 0..60u32 {
                if self.minutes & (1 << m) == 0 || h * 3600 + (m + 1) * 60 <= floor {
                    continue;
                }
                for s in 0..60u32 {
                    let t = h * 3600 + m * 60 + s;
                    if self.seconds & (1 << s) != 0 && t >= floor {
                        return Some(t);
                    }
                }
            }
        }
        None
    }

    fn day_matches(&self, y: i32, m: u32, d: u32) -> bool {
        let len = days_in_month(y, m);
        let weekday = iso_weekday(y, m, d);
        let dom = &self.dom;
        let dom_ok = dom.days & (1 << d) != 0
            || dom
                .last_offsets
                .iter()
                .any(|o| len.checked_sub(*o) == Some(d))
            || dom
                .nearest_weekday
                .iter()
                .any(|n| nearest_weekday(y, m, *n, len) == Some(d))
            || (dom.last_weekday && nearest_weekday(y, m, len, len) == Some(d));
        let dow = &self.dow;
        let dow_ok = dow.days & (1 << weekday) != 0
            || (dow.last_of_month & (1 << weekday) != 0 && d + 7 > len)
            || dow
                .nth
                .iter()
                .any(|(wd, k)| *wd == weekday && (d - 1) / 7 + 1 == *k);
        dom_ok && dow_ok
    }
}

/// ISO weekday (1 = Monday ..= 7 = Sunday).
fn iso_weekday(y: i32, m: u32, d: u32) -> u32 {
    // 1970-01-01 was a Thursday (= 4).
    ((days_from_civil(y, m, d) + 3).rem_euclid(7)) as u32 + 1
}

fn days_in_month(y: i32, m: u32) -> u32 {
    match m {
        2 if (y % 4 == 0 && y % 100 != 0) || y % 400 == 0 => 29,
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    }
}

/// The weekday (Mon–Fri) nearest to day `n` of the month, never crossing into
/// an adjacent month (Quartz/Spring `nW`). `None` if `n` exceeds the month.
fn nearest_weekday(y: i32, m: u32, n: u32, len: u32) -> Option<u32> {
    if n > len {
        return None;
    }
    Some(match iso_weekday(y, m, n) {
        6 if n == 1 => 3,
        6 => n - 1,
        7 if n == len => n - 2,
        7 => n + 1,
        _ => n,
    })
}

/// Parses one numeric value or (when `names` is non-empty) a name, whose
/// value is its index plus `name_base`.
fn parse_value(token: &str, min: u32, max: u32, names: &[&str], name_base: u32) -> Option<u32> {
    let v = if !token.is_empty() && token.bytes().all(|b| b.is_ascii_digit()) {
        token.parse().ok()?
    } else {
        u32::try_from(names.iter().position(|n| *n == token)?).ok()? + name_base
    };
    (min..=max).contains(&v).then_some(v)
}

/// Parses a list of `*` / `n` / `a-b` / `x/step` items into a bitmask.
fn parse_set(field: &str, min: u32, max: u32, names: &[&str]) -> Option<u64> {
    parse_set_with(field, min, max, names, false)
}

/// Like [`parse_set`] for a calendar field (day-of-month, month, weekday),
/// where cron-utils `SPRING53` leaves ranges non-strict: a descending range
/// `a-b` wraps (`FRI-MON`, `NOV-FEB`), stepping across the wrap.
fn parse_calendar_set(field: &str, min: u32, max: u32, names: &[&str]) -> Option<u64> {
    parse_set_with(field, min, max, names, true)
}

fn parse_set_with(field: &str, min: u32, max: u32, names: &[&str], wrap: bool) -> Option<u64> {
    // Month names are 1-based (JAN = 1); weekday names 0-based (SUN = 0).
    let name_base = if names.len() == 12 { 1 } else { 0 };
    let mut mask = 0u64;
    for item in field.split(',') {
        let (range, step) = match item.split_once('/') {
            Some((r, s)) => (r, Some(parse_value(s, 1, u32::MAX, &[], 0)?)),
            None => (item, None),
        };
        let (lo, hi) = if range == "*" {
            (min, max)
        } else if let Some((a, b)) = range.split_once('-') {
            let (a, b) = (
                parse_value(a, min, max, names, name_base)?,
                parse_value(b, min, max, names, name_base)?,
            );
            if a > b && !wrap {
                return None;
            }
            (a, b)
        } else {
            let v = parse_value(range, min, max, names, name_base)?;
            // `n/step` runs from n to the field maximum.
            (v, if step.is_some() { max } else { v })
        };
        let step = u64::from(step.unwrap_or(1));
        if lo > hi {
            // Wrapped range: walk lo..=max then min..=hi as one sequence.
            let span = u64::from(max - min + 1);
            let len = u64::from(max - lo + 1) + u64::from(hi - min + 1);
            let mut i = 0u64;
            while i < len {
                let v = (u64::from(lo - min) + i) % span + u64::from(min);
                mask |= 1 << v;
                i = i.saturating_add(step);
            }
            continue;
        }
        let mut v = u64::from(lo);
        while v <= u64::from(hi) {
            mask |= 1 << v;
            v = v.saturating_add(step);
        }
    }
    Some(mask)
}

fn parse_dom(field: &str) -> Option<DayOfMonth> {
    if field == "?" {
        return Some(DayOfMonth {
            days: parse_set("*", 1, 31, &[])? as u32,
            ..DayOfMonth::default()
        });
    }
    let mut dom = DayOfMonth::default();
    for item in field.split(',') {
        if item == "L" {
            dom.last_offsets.push(0);
        } else if let Some(offset) = item.strip_prefix("L-") {
            dom.last_offsets.push(parse_value(offset, 0, 30, &[], 0)?);
        } else if item == "LW" {
            dom.last_weekday = true;
        } else if let Some(n) = item.strip_suffix('W') {
            dom.nearest_weekday.push(parse_value(n, 1, 31, &[], 0)?);
        } else {
            dom.days |= u32::try_from(parse_calendar_set(item, 1, 31, &[])?).ok()?;
        }
    }
    Some(dom)
}

fn parse_dow(field: &str) -> Option<DayOfWeek> {
    let field = if field == "?" { "*" } else { field };
    let iso = |spring: u32| if spring == 0 { 7 } else { spring };
    let mut dow = DayOfWeek::default();
    for item in field.split(',') {
        if item == "L" {
            // cron-utils treats a bare `L` weekday as plain weekday 7 (Sunday).
            dow.days |= 1 << 7;
        } else if let Some((day, k)) = item.split_once('#') {
            let day = iso(parse_value(day, 0, 7, &DAY_NAMES, 0)?);
            dow.nth.push((day, parse_value(k, 1, 5, &[], 0)?));
        } else if let Some(day) = item.strip_suffix('L').filter(|d| !d.is_empty()) {
            dow.last_of_month |= 1 << iso(parse_value(day, 0, 7, &DAY_NAMES, 0)?);
        } else {
            // Weekdays form a 7-day cycle: alias the `7` spelling of Sunday to
            // `0` and expand over 0..=6 (wrapping), so Sunday is never counted
            // twice in a stepped range (`SAT-SUN/2` == `SAT-7/2`, `7/2` == `0/2`).
            let (range, step) = match item.split_once('/') {
                Some((r, s)) => (r, Some(s)),
                None => (item, None),
            };
            let alias = |v: &str| {
                if v == "7" {
                    "0".to_string()
                } else {
                    v.to_string()
                }
            };
            let range = match range.split_once('-') {
                Some((a, b)) => format!("{}-{}", alias(a), alias(b)),
                None => alias(range),
            };
            let item = match step {
                Some(s) => format!("{range}/{s}"),
                None => range,
            };
            let spring = parse_calendar_set(&item, 0, 6, &DAY_NAMES)?;
            for d in 0..=6u32 {
                if spring & (1 << d) != 0 {
                    dow.days |= 1 << iso(d);
                }
            }
        }
    }
    Some(dow)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Epoch millis of a UTC civil date-time.
    fn at(y: i32, mo: u32, d: u32, h: u64, mi: u64, s: u64) -> u64 {
        let day = u64::try_from(days_from_civil(y, mo, d)).unwrap();
        (day * SECS_PER_DAY + h * 3600 + mi * 60 + s) * 1000
    }

    fn cron(expr: &str) -> Cron {
        Cron::parse(expr).unwrap_or_else(|| panic!("{expr} must parse"))
    }

    #[test]
    fn start_offset_keeps_seconds() {
        assert_eq!(
            parse_start("2026-01-01T06:00:00+00:00:30"),
            Some(at(2026, 1, 1, 5, 59, 30))
        );
        assert_eq!(
            parse_start("2026-01-01T06:00:00-01:30:15"),
            Some(at(2026, 1, 1, 7, 30, 15))
        );
        assert_eq!(
            parse_start("2026-01-01T06:00+02:00"),
            Some(at(2026, 1, 1, 4, 0, 0))
        );
    }

    #[test]
    fn start_offset_requires_sign() {
        assert_eq!(parse_start("2026-01-01T06:00:00Z02:00"), None);
        assert_eq!(parse_start("2026-01-01T06:00:00Z+02:00"), None);
    }

    #[test]
    fn stepped_weekday_ranges_count_sunday_once() {
        assert_eq!(cron("0 0 0 ? * SAT-SUN/2"), cron("0 0 0 ? * SAT"));
        assert_eq!(cron("0 0 0 ? * SAT-7/2"), cron("0 0 0 ? * SAT"));
        assert_eq!(cron("0 0 0 ? * FRI-MON/2"), cron("0 0 0 ? * FRI,SUN"));
        assert_eq!(cron("0 0 0 ? * 5-1/2"), cron("0 0 0 ? * 5-7/2"));
        assert_eq!(cron("0 0 0 ? * MON-7"), cron("0 0 0 ? * MON-SUN"));
    }

    #[test]
    fn bare_l_weekday_is_sunday() {
        assert_eq!(cron("0 0 0 ? * L"), cron("0 0 0 ? * SUN"));
        // 2026-01-01 is a Thursday; next Sunday is 2026-01-04.
        assert_eq!(
            cron("0 0 0 ? * L").next_after(at(2026, 1, 1, 0, 0, 0)),
            Some(at(2026, 1, 4, 0, 0, 0))
        );
    }

    #[test]
    fn weekday_step_anchor_seven_is_zero() {
        assert_eq!(cron("0 0 0 ? * 7/2"), cron("0 0 0 ? * 0/2"));
        assert_eq!(cron("0 0 0 ? * 7/2"), cron("0 0 0 ? * SUN,TUE,THU,SAT"));
    }

    #[test]
    fn rejects_what_zeebe_cron_timer_test_rejects() {
        // CronTimerTest.shouldFailToParseIfWrongExpression.
        for bad in [
            "",
            "*",
            "* * * * *",
            "* * * * * * *",
            "60 * * * * *",
            "20-10 * * * * *",
            "* 60 * * * *",
            "* 20-10 * * * *",
            "* * 24 * * *",
            "* * 20-10 * * *",
            "* * * 0 * *",
            "* * * 32 * *",
            "* * * * 0 *",
            "* * * * 13 *",
            "* * * * * 8",
            "* * * * * *SUN",
            "* * * * * SUN*",
        ] {
            assert_eq!(Cron::parse(bad), None, "{bad:?} must be rejected");
        }
    }

    #[test]
    fn calendar_ranges_wrap_but_clock_ranges_stay_strict() {
        // FRI-MON wraps through the weekend (2026-01-02 is a Friday).
        let c = cron("0 0 9 * * FRI-MON");
        let mut t = at(2026, 1, 1, 10, 0, 0);
        let mut days = vec![];
        for _ in 0..5 {
            t = c.next_after(t).unwrap();
            days.push(t);
        }
        assert_eq!(
            days,
            vec![
                at(2026, 1, 2, 9, 0, 0),
                at(2026, 1, 3, 9, 0, 0),
                at(2026, 1, 4, 9, 0, 0),
                at(2026, 1, 5, 9, 0, 0),
                at(2026, 1, 9, 9, 0, 0),
            ]
        );
        // NOV-FEB wraps the year; stepped wrap NOV-FEB/2 = NOV, JAN.
        let c = cron("0 0 9 1 NOV-FEB ?");
        assert_eq!(
            c.next_after(at(2026, 2, 2, 0, 0, 0)),
            Some(at(2026, 11, 1, 9, 0, 0))
        );
        assert_eq!(
            c.next_after(at(2026, 12, 2, 0, 0, 0)),
            Some(at(2027, 1, 1, 9, 0, 0))
        );
        let c = cron("0 0 9 1 NOV-FEB/2 ?");
        assert_eq!(
            c.next_after(at(2026, 11, 2, 0, 0, 0)),
            Some(at(2027, 1, 1, 9, 0, 0))
        );
        assert_eq!(
            c.next_after(at(2027, 1, 2, 0, 0, 0)),
            Some(at(2027, 11, 1, 9, 0, 0))
        );
        // Day-of-month 30-2 wraps too.
        let c = cron("0 0 0 30-2 * ?");
        assert_eq!(
            c.next_after(at(2026, 1, 3, 0, 0, 0)),
            Some(at(2026, 1, 30, 0, 0, 0))
        );
        assert_eq!(
            c.next_after(at(2026, 1, 31, 0, 0, 0)),
            Some(at(2026, 2, 1, 0, 0, 0))
        );
    }

    #[test]
    fn cycle_start_rejects_malformed_clock_and_offset() {
        for bad in [
            "R/2026-01-01T24:30:00Z/PT1H",
            "R/2026-01-01T23:60:00Z/PT1H",
            "R/2026-01-01T23:30:60Z/PT1H",
            "R/2026-01-01T23:30:00+99:99/PT1H",
            "R/2026-01-01T23:30:00+19:00/PT1H",
            "R/2026-01-01T23:30:00+01:60/PT1H",
            "R/2026-01-01T23:30:00+1:00/PT1H",
            "R/2026-01-01T23:30:00./PT1H",
            "R/2026-01-01T2:30:00Z/PT1H",
        ] {
            assert_eq!(TimerCycle::parse(bad), None, "{bad:?} must be rejected");
        }
        for good in [
            "R/2026-01-01T23:30Z/PT1H",
            "R/2026-01-01T23:30:59.123Z/PT1H",
            "R/2026-01-01T23:30:00+18:00/PT1H",
            "R/2026-01-01T23:30:00-05:30[America/New_York]/PT1H",
        ] {
            assert!(TimerCycle::parse(good).is_some(), "{good:?} must parse");
        }
    }

    #[test]
    fn pre_epoch_cycle_start_is_overdue_not_rejected() {
        let c = TimerCycle::parse("R/1969-12-31T23:00:00Z/PT1H").expect("pre-epoch start parses");
        assert_eq!(c.next_due(0, 7), 3_600_000);
    }

    #[test]
    fn every_twelve_hours() {
        let c = cron("0 0 */12 * * ?");
        let from = at(2026, 1, 1, 5, 30, 0);
        let first = c.next_after(from).unwrap();
        assert_eq!(first, at(2026, 1, 1, 12, 0, 0));
        assert_eq!(c.next_after(first).unwrap(), at(2026, 1, 2, 0, 0, 0));
    }

    #[test]
    fn matches_zeebe_cron_timer_test_cases() {
        // shouldMatchSpecificSecond: strictly-after, truncated to the second.
        let c = cron("10 * * * * *");
        assert_eq!(
            c.next_after(at(2026, 3, 4, 10, 0, 9) + 250),
            Some(at(2026, 3, 4, 10, 0, 10))
        );
        // A match exactly at `from` is not "next".
        assert_eq!(
            c.next_after(at(2026, 3, 4, 10, 0, 10)),
            Some(at(2026, 3, 4, 10, 1, 10))
        );
        // shouldParseSpecificMinuteSecond.
        let c = cron("55 5 * * * *");
        let first = c.next_after(at(2026, 3, 4, 9, 4, 54)).unwrap();
        assert_eq!(first, at(2026, 3, 4, 9, 5, 55));
        assert_eq!(c.next_after(first), Some(at(2026, 3, 4, 10, 5, 55)));
        // shouldParseSecondRange.
        let c = cron("10-15 * * * * *");
        for s in 9..15 {
            assert_eq!(
                c.next_after(at(2026, 3, 4, 9, 0, s)),
                Some(at(2026, 3, 4, 9, 0, s + 1))
            );
        }
        // TimerStartEventTest.shouldCreateCronTimer: "0 0 * * * *".
        assert_eq!(
            cron("0 0 * * * *").next_after(at(2026, 3, 4, 9, 0, 0)),
            Some(at(2026, 3, 4, 10, 0, 0))
        );
    }

    #[test]
    fn month_and_year_boundaries() {
        // Last day of month, across a 30/31 and a leap February.
        let c = cron("0 0 0 L * *");
        assert_eq!(
            c.next_after(at(2026, 4, 15, 0, 0, 0)),
            Some(at(2026, 4, 30, 0, 0, 0))
        );
        assert_eq!(
            c.next_after(at(2028, 2, 1, 0, 0, 0)),
            Some(at(2028, 2, 29, 0, 0, 0))
        );
        assert_eq!(
            c.next_after(at(2026, 12, 31, 0, 0, 0)),
            Some(at(2027, 1, 31, 0, 0, 0))
        );
        // Day 31 skips 30-day months.
        let c = cron("0 30 6 31 * *");
        assert_eq!(
            c.next_after(at(2026, 4, 1, 0, 0, 0)),
            Some(at(2026, 5, 31, 6, 30, 0))
        );
        // Feb 29 only in leap years.
        let c = cron("0 0 0 29 FEB ?");
        assert_eq!(
            c.next_after(at(2026, 1, 1, 0, 0, 0)),
            Some(at(2028, 2, 29, 0, 0, 0))
        );
        // Feb 30 never matches.
        assert_eq!(cron("0 0 0 30 2 ?").next_after(0), None);
    }

    #[test]
    fn weekday_forms() {
        // 2026-01-01 is a Thursday.
        assert_eq!(iso_weekday(2026, 1, 1), 4);
        let c = cron("0 0 9 * * MON-FRI");
        assert_eq!(
            c.next_after(at(2026, 1, 2, 10, 0, 0)),
            Some(at(2026, 1, 5, 9, 0, 0))
        );
        // 0 and 7 are both Sunday.
        for e in ["0 0 0 ? * 0", "0 0 0 ? * 7", "0 0 0 ? * sun"] {
            assert_eq!(
                cron(e).next_after(at(2026, 1, 1, 0, 0, 0)),
                Some(at(2026, 1, 4, 0, 0, 0))
            );
        }
        // Last Friday of January 2026 is the 30th; second Tuesday is the 13th.
        assert_eq!(
            cron("0 0 0 ? * 5L").next_after(at(2026, 1, 1, 0, 0, 0)),
            Some(at(2026, 1, 30, 0, 0, 0))
        );
        assert_eq!(
            cron("0 0 0 ? * TUE#2").next_after(at(2026, 1, 1, 0, 0, 0)),
            Some(at(2026, 1, 13, 0, 0, 0))
        );
        // Nearest weekday to the 3rd (a Saturday in Jan 2026) is Friday the 2nd;
        // nearest to the 1st of Feb 2026 (a Sunday) is Monday the 2nd.
        assert_eq!(
            cron("0 0 0 3W * ?").next_after(at(2026, 1, 1, 0, 0, 0)),
            Some(at(2026, 1, 2, 0, 0, 0))
        );
        assert_eq!(
            cron("0 0 0 1W * ?").next_after(at(2026, 1, 31, 0, 0, 0)),
            Some(at(2026, 2, 2, 0, 0, 0))
        );
        // Last weekday of May 2026 (31st is a Sunday) is Friday the 29th.
        assert_eq!(
            cron("0 0 0 LW * ?").next_after(at(2026, 5, 1, 0, 0, 0)),
            Some(at(2026, 5, 29, 0, 0, 0))
        );
    }

    #[test]
    fn macros_lists_and_steps() {
        assert_eq!(cron("@daily"), cron("0 0 0 * * *"));
        assert_eq!(cron("@hourly"), cron("0 0 * * * *"));
        let c = cron("0 5/20 1,3 * * *");
        assert_eq!(
            c.next_after(at(2026, 1, 1, 1, 6, 0)),
            Some(at(2026, 1, 1, 1, 25, 0))
        );
        assert_eq!(
            c.next_after(at(2026, 1, 1, 1, 45, 0)),
            Some(at(2026, 1, 1, 3, 5, 0))
        );
    }

    #[test]
    fn iso_cycle_forms() {
        let c = TimerCycle::parse("R/2026-01-01T06:00:00Z/PT12H").unwrap();
        let start = at(2026, 1, 1, 6, 0, 0);
        assert_eq!(
            c.schedule,
            Schedule::Interval {
                start_millis: Some(start),
                interval_millis: 12 * 3_600_000
            }
        );
        assert_eq!(c.repetitions, None);
        // A future start is the first due date; a past start fires now.
        assert_eq!(c.first_due(start - 5_000), (start, None));
        assert_eq!(c.first_due(start + 5_000), (start + 5_000, None));
        // Re-arm from the previous due date, never earlier than now.
        assert_eq!(c.next_due(start, start), start + 12 * 3_600_000);
        assert_eq!(
            c.next_due(start, start + 30 * 3_600_000),
            start + 30 * 3_600_000
        );

        let c = TimerCycle::parse("R3/2026-01-01T07:00:00+01:00/PT1H").unwrap();
        assert_eq!(c.repetitions, Some(3));
        assert_eq!(c.first_due(0).0, start);
        assert!(TimerCycle::parse("R/2026-01-01T07:00:00+01:00[Europe/Paris]/PT1H").is_some());

        // Zeebe rejects the end-date form and a zone-less start.
        assert_eq!(TimerCycle::parse("R/PT1H/2026-01-01T06:00:00Z"), None);
        assert_eq!(TimerCycle::parse("R/2026-01-01T06:00:00/PT1H"), None);
        assert_eq!(TimerCycle::parse("R0/PT1H"), None);
        assert_eq!(TimerCycle::parse("Rx/PT1H"), None);

        // Plain and bare forms.
        assert_eq!(
            TimerCycle::parse("R2/PT1M").unwrap().first_due(1_000),
            (61_000, Some(2))
        );
        assert_eq!(
            TimerCycle::parse("PT1M").unwrap().first_due(0),
            (60_000, None)
        );
        // Anything not `R…` and not a duration is cron.
        let c = TimerCycle::parse("0 0 */12 * * ?").unwrap();
        assert!(matches!(c.schedule, Schedule::Cron(_)));
        assert_eq!(c.interval_millis(), 0);
        assert_eq!(
            c.first_due(at(2026, 1, 1, 1, 0, 0)),
            (at(2026, 1, 1, 12, 0, 0), None)
        );
        assert_eq!(
            c.next_due(at(2026, 1, 1, 12, 0, 0), at(2026, 1, 3, 1, 0, 0)),
            at(2026, 1, 3, 12, 0, 0)
        );
        // A cron that never fires again is due immediately, once (Zeebe CronTimer).
        assert_eq!(
            TimerCycle::parse("0 0 0 30 2 ?").unwrap().first_due(7),
            (7, Some(1))
        );
    }
}
