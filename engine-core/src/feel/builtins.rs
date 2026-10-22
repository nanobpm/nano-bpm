//! The FEEL builtin function library (feel-scala standard library plus the
//! Camunda extensions used by Zeebe), implemented dependency-free.

use std::collections::{BTreeMap, HashMap};
use std::time::{SystemTime, UNIX_EPOCH};

use super::error::FeelError;
use super::eval::compare;
use super::regex::Regex;
use super::temporal::{civil_from_days, Date, DateTime, DtDuration, Time, YmDuration};
use super::value::{FeelVal, Range};
use crate::model::Value;

/// The canonical names of every builtin we recognise.
const BUILTINS: &[&str] = &[
    // conversion
    "string",
    "number",
    "context",
    "get value",
    "get entries",
    "date",
    "time",
    "date and time",
    "duration",
    "years and months duration",
    "from json",
    "to json",
    // boolean
    "not",
    // string
    "substring",
    "string length",
    "upper case",
    "lower case",
    "substring before",
    "substring after",
    "contains",
    "starts with",
    "ends with",
    "matches",
    "replace",
    "split",
    "string join",
    "trim",
    "extract",
    "uuid",
    "to base64",
    "from base64",
    // list
    "list contains",
    "count",
    "min",
    "max",
    "sum",
    "product",
    "mean",
    "median",
    "stddev",
    "mode",
    "all",
    "any",
    "sublist",
    "append",
    "concatenate",
    "insert before",
    "remove",
    "reverse",
    "index of",
    "union",
    "distinct values",
    "duplicate values",
    "flatten",
    "sort",
    "is empty",
    "partition",
    "and",
    "or",
    // numeric
    "decimal",
    "floor",
    "ceiling",
    "abs",
    "modulo",
    "sqrt",
    "log",
    "exp",
    "even",
    "odd",
    "round up",
    "round down",
    "round half up",
    "round half down",
    "random number",
    // context
    "context put",
    "context merge",
    "put",
    "put all",
    "get or else",
    "is defined",
    // boolean
    "assert",
    // temporal
    "now",
    "today",
    "day of week",
    "day of year",
    "week of year",
    "month of year",
    "last day of month",
    // range / interval (Allen's algebra)
    "before",
    "after",
    "meets",
    "met by",
    "overlaps",
    "overlaps before",
    "overlaps after",
    "finishes",
    "finished by",
    "includes",
    "during",
    "starts",
    "started by",
    "coincides",
    // misc extensions
    "is blank",
    // Camunda agentic extension — tags a value as AI-generated; the FEEL engine
    // simply returns the value unchanged (the description/type/schema/options
    // metadata is consumed by the connector/job worker, not here).
    "fromAi",
];

pub fn is_builtin(name: &str) -> bool {
    BUILTINS.contains(&name)
}

/// Whether `prefix` is a builtin name or a word-prefix of one (used by the
/// parser to decide whether to keep joining multi-word names across the `and`
/// keyword, e.g. `date and time`).
pub fn name_prefix(prefix: &str) -> bool {
    let with_space = format!("{prefix} ");
    BUILTINS
        .iter()
        .any(|b| **b == *prefix || b.starts_with(&with_space))
}

pub fn canonical(name: &str) -> &'static str {
    BUILTINS.iter().find(|b| **b == name).copied().unwrap_or("")
}

/// Ordered parameter names for the builtins that are commonly invoked with
/// named arguments. Returning `None` means named arguments are unsupported.
pub fn params(name: &str) -> Option<&'static [&'static str]> {
    Some(match name {
        "substring" => &["string", "start position", "length"],
        "contains" => &["string", "match"],
        "starts with" => &["string", "match"],
        "ends with" => &["string", "match"],
        "matches" => &["input", "pattern", "flags"],
        "replace" => &["input", "pattern", "replacement", "flags"],
        "split" => &["string", "delimiter"],
        "list contains" => &["list", "element"],
        "sublist" => &["list", "start position", "length"],
        "string length" => &["string"],
        "upper case" => &["string"],
        "lower case" => &["string"],
        "string join" => &["list", "delimiter"],
        "get value" => &["context", "key"],
        "get or else" => &["value", "default"],
        "round up" | "round down" | "round half up" | "round half down" => &["n", "scale"],
        "fromAi" => &["value", "description", "type", "schema", "options"],
        _ => return None,
    })
}

fn arg(args: &[FeelVal], i: usize) -> FeelVal {
    args.get(i).cloned().unwrap_or(FeelVal::Null)
}

fn want_num(v: &FeelVal, ctx: &str) -> Result<f64, FeelError> {
    v.as_f64()
        .ok_or_else(|| FeelError(format!("{ctx}: expected a number, got {}", v.type_name())))
}

fn want_str(v: &FeelVal, ctx: &str) -> Result<String, FeelError> {
    match v {
        FeelVal::Str(s) => Ok(s.clone()),
        _ => Err(FeelError(format!(
            "{ctx}: expected a string, got {}",
            v.type_name()
        ))),
    }
}

fn want_list(v: &FeelVal, ctx: &str) -> Result<Vec<FeelVal>, FeelError> {
    match v {
        FeelVal::List(items) => Ok(items.clone()),
        _ => Err(FeelError(format!(
            "{ctx}: expected a list, got {}",
            v.type_name()
        ))),
    }
}

/// A list argument that also accepts variadic scalar arguments (FEEL allows
/// `sum(1,2,3)` as well as `sum([1,2,3])`).
fn list_or_varargs(args: &[FeelVal]) -> Vec<FeelVal> {
    if args.len() == 1 {
        if let FeelVal::List(items) = &args[0] {
            return items.clone();
        }
    }
    args.to_vec()
}

pub fn call(
    name: &str,
    args: Vec<FeelVal>,
    base: &HashMap<String, Value>,
) -> Result<FeelVal, FeelError> {
    use FeelVal::*;
    match name {
        // --- conversion ---
        "string" => Ok(arg(&args, 0).to_feel_string().map(Str).unwrap_or(Null)),
        "number" => number_from(&args),
        "context" => context_from(&arg(&args, 0)),
        "get value" => Ok(match (arg(&args, 0), arg(&args, 1)) {
            (Context(m), Str(k)) => m.get(&k).cloned().unwrap_or(Null),
            _ => Null,
        }),
        "get entries" => Ok(match arg(&args, 0) {
            Context(m) => List(
                m.into_iter()
                    .map(|(k, v)| {
                        let mut e = BTreeMap::new();
                        e.insert("key".to_string(), Str(k));
                        e.insert("value".to_string(), v);
                        Context(e)
                    })
                    .collect(),
            ),
            _ => Null,
        }),
        "date" => date_builtin(&args),
        "time" => time_builtin(&args),
        "date and time" => datetime_builtin(&args),
        "duration" => Ok(parse_duration(&want_str(&arg(&args, 0), "duration")?).unwrap_or(Null)),
        "years and months duration" => years_months_between(&arg(&args, 0), &arg(&args, 1)),
        "from json" => Ok(from_json(&want_str(&arg(&args, 0), "from json")?).unwrap_or(Null)),
        "to json" => Ok(Str(to_json(&arg(&args, 0)))),

        // --- boolean ---
        "not" => Ok(match arg(&args, 0) {
            Bool(b) => Bool(!b),
            _ => Null,
        }),

        // --- string ---
        "substring" => substring(&args),
        "string length" => Ok(Int(
            want_str(&arg(&args, 0), "string length")?.chars().count() as i64,
        )),
        "upper case" => Ok(Str(want_str(&arg(&args, 0), "upper case")?.to_uppercase())),
        "lower case" => Ok(Str(want_str(&arg(&args, 0), "lower case")?.to_lowercase())),
        "substring before" => {
            let s = want_str(&arg(&args, 0), "substring before")?;
            let m = want_str(&arg(&args, 1), "substring before")?;
            Ok(Str(match s.find(&m) {
                Some(i) => s[..i].to_string(),
                None => String::new(),
            }))
        }
        "substring after" => {
            let s = want_str(&arg(&args, 0), "substring after")?;
            let m = want_str(&arg(&args, 1), "substring after")?;
            Ok(Str(match s.find(&m) {
                Some(i) => s[i + m.len()..].to_string(),
                None => String::new(),
            }))
        }
        "contains" => Ok(Bool(
            want_str(&arg(&args, 0), "contains")?.contains(&want_str(&arg(&args, 1), "contains")?),
        )),
        "starts with" => Ok(Bool(
            want_str(&arg(&args, 0), "starts with")?
                .starts_with(&want_str(&arg(&args, 1), "starts with")?),
        )),
        "ends with" => Ok(Bool(
            want_str(&arg(&args, 0), "ends with")?
                .ends_with(&want_str(&arg(&args, 1), "ends with")?),
        )),
        "matches" => {
            let input = want_str(&arg(&args, 0), "matches")?;
            let pattern = want_str(&arg(&args, 1), "matches")?;
            let flags = match arg(&args, 2) {
                Str(f) => f,
                _ => String::new(),
            };
            let re = Regex::new(&pattern, &flags)
                .ok_or_else(|| FeelError(format!("invalid regex '{pattern}'")))?;
            Ok(Bool(re.is_match(&input)))
        }
        "replace" => {
            let input = want_str(&arg(&args, 0), "replace")?;
            let pattern = want_str(&arg(&args, 1), "replace")?;
            let replacement = want_str(&arg(&args, 2), "replace")?;
            let flags = match arg(&args, 3) {
                Str(f) => f,
                _ => String::new(),
            };
            let re = Regex::new(&pattern, &flags)
                .ok_or_else(|| FeelError(format!("invalid regex '{pattern}'")))?;
            Ok(Str(re.replace_all(&input, &replacement)))
        }
        "split" => {
            let input = want_str(&arg(&args, 0), "split")?;
            let pattern = want_str(&arg(&args, 1), "split")?;
            let re = Regex::new(&pattern, "")
                .ok_or_else(|| FeelError(format!("invalid regex '{pattern}'")))?;
            Ok(List(re.split(&input).into_iter().map(Str).collect()))
        }
        "string join" => string_join(&args),
        "trim" => Ok(Str(want_str(&arg(&args, 0), "trim")?.trim().to_string())),
        "extract" => {
            let input = want_str(&arg(&args, 0), "extract")?;
            let pattern = want_str(&arg(&args, 1), "extract")?;
            let re = Regex::new(&pattern, "")
                .ok_or_else(|| FeelError(format!("invalid regex '{pattern}'")))?;
            Ok(List(re.find_all(&input).into_iter().map(Str).collect()))
        }
        "uuid" => Ok(Str(uuid_v4())),
        "to base64" => Ok(Str(base64_encode(
            want_str(&arg(&args, 0), "to base64")?.as_bytes(),
        ))),
        "from base64" => Ok(base64_decode(&want_str(&arg(&args, 0), "from base64")?)
            .and_then(|b| String::from_utf8(b).ok())
            .map(Str)
            .unwrap_or(Null)),

        // --- list ---
        "list contains" => {
            let list = want_list(&arg(&args, 0), "list contains")?;
            let e = arg(&args, 1);
            Ok(Bool(list.iter().any(|x| super::eval::feel_eq(x, &e))))
        }
        "count" => Ok(Int(want_list(&arg(&args, 0), "count")?.len() as i64)),
        "min" => reduce_num(&list_or_varargs(&args), "min", |a, b| a.min(b)),
        "max" => reduce_num(&list_or_varargs(&args), "max", |a, b| a.max(b)),
        "sum" => {
            let items = list_or_varargs(&args);
            let mut total = 0.0;
            for it in &items {
                total += want_num(it, "sum")?;
            }
            Ok(FeelVal::num(total))
        }
        "product" => {
            let items = list_or_varargs(&args);
            if items.is_empty() {
                return Ok(Null);
            }
            let mut total = 1.0;
            for it in &items {
                total *= want_num(it, "product")?;
            }
            Ok(FeelVal::num(total))
        }
        "mean" => {
            let items = list_or_varargs(&args);
            if items.is_empty() {
                return Ok(Null);
            }
            let mut total = 0.0;
            for it in &items {
                total += want_num(it, "mean")?;
            }
            Ok(FeelVal::num(total / items.len() as f64))
        }
        "median" => median(&list_or_varargs(&args)),
        "stddev" => stddev(&list_or_varargs(&args)),
        "mode" => mode(&list_or_varargs(&args)),
        "all" | "and" => bool_reduce(&list_or_varargs(&args), true),
        "any" | "or" => bool_reduce(&list_or_varargs(&args), false),
        "sublist" => sublist(&args),
        "append" => {
            let mut list = want_list(&arg(&args, 0), "append")?;
            list.extend(args.into_iter().skip(1));
            Ok(List(list))
        }
        "concatenate" => {
            let mut out = Vec::new();
            for a in &args {
                out.extend(want_list(a, "concatenate")?);
            }
            Ok(List(out))
        }
        "insert before" => {
            let mut list = want_list(&arg(&args, 0), "insert before")?;
            let pos = want_num(&arg(&args, 1), "insert before")? as i64;
            let item = arg(&args, 2);
            let idx = (pos - 1).clamp(0, list.len() as i64) as usize;
            list.insert(idx, item);
            Ok(List(list))
        }
        "remove" => {
            let mut list = want_list(&arg(&args, 0), "remove")?;
            let pos = want_num(&arg(&args, 1), "remove")? as i64 - 1;
            if pos >= 0 && (pos as usize) < list.len() {
                list.remove(pos as usize);
            }
            Ok(List(list))
        }
        "reverse" => {
            let mut list = want_list(&arg(&args, 0), "reverse")?;
            list.reverse();
            Ok(List(list))
        }
        "index of" => {
            let list = want_list(&arg(&args, 0), "index of")?;
            let m = arg(&args, 1);
            let out: Vec<FeelVal> = list
                .iter()
                .enumerate()
                .filter(|(_, x)| super::eval::feel_eq(x, &m))
                .map(|(i, _)| Int(i as i64 + 1))
                .collect();
            Ok(List(out))
        }
        "union" => {
            let mut out: Vec<FeelVal> = Vec::new();
            for a in &args {
                for x in want_list(a, "union")? {
                    if !out.iter().any(|y| super::eval::feel_eq(&x, y)) {
                        out.push(x);
                    }
                }
            }
            Ok(List(out))
        }
        "distinct values" => {
            let list = want_list(&arg(&args, 0), "distinct values")?;
            let mut out: Vec<FeelVal> = Vec::new();
            for x in list {
                if !out.iter().any(|y| super::eval::feel_eq(&x, y)) {
                    out.push(x);
                }
            }
            Ok(List(out))
        }
        "duplicate values" => {
            let list = want_list(&arg(&args, 0), "duplicate values")?;
            let mut out: Vec<FeelVal> = Vec::new();
            for x in &list {
                let n = list.iter().filter(|y| super::eval::feel_eq(x, y)).count();
                if n > 1 && !out.iter().any(|y| super::eval::feel_eq(x, y)) {
                    out.push(x.clone());
                }
            }
            Ok(List(out))
        }
        "flatten" => {
            let mut out = Vec::new();
            flatten(&arg(&args, 0), &mut out);
            Ok(List(out))
        }
        "sort" => sort(&args, base),
        "is empty" => Ok(Bool(want_list(&arg(&args, 0), "is empty")?.is_empty())),
        "partition" => partition(&args),

        // --- numeric ---
        "decimal" => {
            let n = want_num(&arg(&args, 0), "decimal")?;
            let scale = want_num(&arg(&args, 1), "decimal")? as i32;
            let mode = match args.get(2) {
                Some(v) => rounding_mode(&want_str(v, "decimal")?)?,
                None => RoundMode::HalfEven,
            };
            Ok(FeelVal::num(round_scale(n, scale, mode)))
        }
        "floor" => {
            let n = want_num(&arg(&args, 0), "floor")?;
            match args.get(1) {
                Some(v) => {
                    let scale = want_num(v, "floor")? as i32;
                    let f = 10f64.powi(scale);
                    Ok(FeelVal::num((n * f).floor() / f))
                }
                None => Ok(FeelVal::num(n.floor())),
            }
        }
        "ceiling" => {
            let n = want_num(&arg(&args, 0), "ceiling")?;
            match args.get(1) {
                Some(v) => {
                    let scale = want_num(v, "ceiling")? as i32;
                    let f = 10f64.powi(scale);
                    Ok(FeelVal::num((n * f).ceil() / f))
                }
                None => Ok(FeelVal::num(n.ceil())),
            }
        }
        "abs" => match arg(&args, 0) {
            YmDur(d) => Ok(YmDur(YmDuration::new(d.months.abs()))),
            DtDur(d) => Ok(DtDur(DtDuration::from_nanos(d.nanos.abs()))),
            v => Ok(FeelVal::num(want_num(&v, "abs")?.abs())),
        },
        "modulo" => {
            let a = want_num(&arg(&args, 0), "modulo")?;
            let b = want_num(&arg(&args, 1), "modulo")?;
            if b == 0.0 {
                Ok(Null)
            } else {
                Ok(FeelVal::num(a - b * (a / b).floor()))
            }
        }
        "sqrt" => {
            let n = want_num(&arg(&args, 0), "sqrt")?;
            Ok(if n < 0.0 {
                Null
            } else {
                FeelVal::num(n.sqrt())
            })
        }
        "log" => Ok(FeelVal::num(want_num(&arg(&args, 0), "log")?.ln())),
        "exp" => Ok(FeelVal::num(want_num(&arg(&args, 0), "exp")?.exp())),
        "even" => Ok(Bool(want_num(&arg(&args, 0), "even")? as i64 % 2 == 0)),
        "odd" => Ok(Bool(want_num(&arg(&args, 0), "odd")? as i64 % 2 != 0)),
        "round up" => round_builtin(&args, RoundMode::Up),
        "round down" => round_builtin(&args, RoundMode::Down),
        "round half up" => round_builtin(&args, RoundMode::HalfUp),
        "round half down" => round_builtin(&args, RoundMode::HalfDown),
        "random number" => Ok(Double(rand_unit())),

        // --- context ---
        "context put" | "put" => context_put(&args),
        "context merge" | "put all" => {
            let mut out = BTreeMap::new();
            for a in list_or_varargs(&args) {
                if let Context(m) = a {
                    out.extend(m);
                }
            }
            Ok(Context(out))
        }
        "get or else" => Ok(match arg(&args, 0) {
            Null => arg(&args, 1),
            v => v,
        }),
        "is defined" => Ok(Bool(!matches!(arg(&args, 0), Null))),
        "assert" => assert_builtin(&args),

        // --- temporal ---
        "now" => Ok(now_datetime().map(DateTime).unwrap_or(Null)),
        "today" => Ok(now_datetime().map(|dt| Date(dt.date)).unwrap_or(Null)),
        "day of week" => Ok(with_date(&arg(&args, 0), |d| {
            Str(WEEKDAYS[(d.weekday() - 1) as usize].to_string())
        })),
        "day of year" => Ok(with_date(&arg(&args, 0), |d| Int(d.day_of_year() as i64))),
        "week of year" => Ok(with_date(&arg(&args, 0), |d| Int(d.week_of_year() as i64))),
        "month of year" => Ok(with_date(&arg(&args, 0), |d| {
            Str(MONTHS[(d.month - 1) as usize].to_string())
        })),
        "last day of month" => Ok(with_date(&arg(&args, 0), |d| {
            let last = days_in(d.year, d.month);
            match super::temporal::Date::new(d.year, d.month, last) {
                Some(date) => Date(date),
                None => Null,
            }
        })),

        // --- misc ---
        "is blank" => Ok(Bool(match arg(&args, 0) {
            Str(s) => s.trim().is_empty(),
            Null => true,
            _ => false,
        })),

        // --- range / interval (Allen's algebra) ---
        "before" | "after" | "meets" | "met by" | "overlaps" | "overlaps before"
        | "overlaps after" | "finishes" | "finished by" | "includes" | "during" | "starts"
        | "started by" | "coincides" => Ok(interval(name, &arg(&args, 0), &arg(&args, 1))),

        // --- Camunda agentic extension ---
        // `fromAi(value, description?, type?, schema?, options?)` tags a value as
        // AI-generated. The FEEL engine returns the value unchanged; the remaining
        // arguments are metadata the connector/job worker uses to build the tool
        // schema advertised to the model.
        "fromAi" => Ok(arg(&args, 0)),

        other => Err(FeelError(format!("unknown function '{other}'"))),
    }
}

// --- helpers ----------------------------------------------------------------

const WEEKDAYS: [&str; 7] = [
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday",
    "Sunday",
];
const MONTHS: [&str; 12] = [
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
];

fn days_in(y: i32, m: u32) -> u32 {
    match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if (y % 4 == 0 && y % 100 != 0) || y % 400 == 0 => 29,
        2 => 28,
        _ => 30,
    }
}

fn with_date(v: &FeelVal, f: impl Fn(&Date) -> FeelVal) -> FeelVal {
    match v {
        FeelVal::Date(d) => f(d),
        FeelVal::DateTime(dt) => f(&dt.date),
        _ => FeelVal::Null,
    }
}

fn reduce_num(
    items: &[FeelVal],
    ctx: &str,
    f: impl Fn(f64, f64) -> f64,
) -> Result<FeelVal, FeelError> {
    if items.is_empty() {
        return Ok(FeelVal::Null);
    }
    let mut acc = want_num(&items[0], ctx)?;
    for it in &items[1..] {
        acc = f(acc, want_num(it, ctx)?);
    }
    Ok(FeelVal::num(acc))
}

fn bool_reduce(items: &[FeelVal], all: bool) -> Result<FeelVal, FeelError> {
    let mut result = all;
    for it in items {
        match it {
            FeelVal::Bool(b) => {
                if all {
                    result &= b;
                } else {
                    result |= b;
                }
            }
            FeelVal::Null => {}
            other => {
                return Err(FeelError(format!(
                    "{}: expected booleans, got {}",
                    if all { "all" } else { "any" },
                    other.type_name()
                )))
            }
        }
    }
    Ok(FeelVal::Bool(result))
}

fn median(items: &[FeelVal]) -> Result<FeelVal, FeelError> {
    if items.is_empty() {
        return Ok(FeelVal::Null);
    }
    let mut nums: Vec<f64> = items
        .iter()
        .map(|v| want_num(v, "median"))
        .collect::<Result<_, _>>()?;
    nums.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let n = nums.len();
    Ok(if n % 2 == 1 {
        FeelVal::num(nums[n / 2])
    } else {
        FeelVal::num((nums[n / 2 - 1] + nums[n / 2]) / 2.0)
    })
}

fn stddev(items: &[FeelVal]) -> Result<FeelVal, FeelError> {
    let n = items.len();
    if n < 2 {
        return Ok(FeelVal::Null);
    }
    let nums: Vec<f64> = items
        .iter()
        .map(|v| want_num(v, "stddev"))
        .collect::<Result<_, _>>()?;
    let mean = nums.iter().sum::<f64>() / n as f64;
    let var = nums.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / (n as f64 - 1.0);
    Ok(FeelVal::num(var.sqrt()))
}

fn mode(items: &[FeelVal]) -> Result<FeelVal, FeelError> {
    if items.is_empty() {
        return Ok(FeelVal::List(Vec::new()));
    }
    let nums: Vec<f64> = items
        .iter()
        .map(|v| want_num(v, "mode"))
        .collect::<Result<_, _>>()?;
    let mut counts: Vec<(f64, usize)> = Vec::new();
    for &x in &nums {
        if let Some(entry) = counts.iter_mut().find(|(v, _)| *v == x) {
            entry.1 += 1;
        } else {
            counts.push((x, 1));
        }
    }
    let max = counts.iter().map(|(_, c)| *c).max().unwrap_or(0);
    let mut modes: Vec<f64> = counts
        .into_iter()
        .filter(|(_, c)| *c == max)
        .map(|(v, _)| v)
        .collect();
    modes.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    Ok(FeelVal::List(modes.into_iter().map(FeelVal::num).collect()))
}

fn sublist(args: &[FeelVal]) -> Result<FeelVal, FeelError> {
    let list = want_list(&arg(args, 0), "sublist")?;
    let start = want_num(&arg(args, 1), "sublist")? as i64;
    let len = list.len() as i64;
    let from = if start > 0 {
        start - 1
    } else if start < 0 {
        len + start
    } else {
        return Ok(FeelVal::List(Vec::new()));
    };
    if from < 0 || from >= len {
        return Ok(FeelVal::List(Vec::new()));
    }
    let count = match args.get(2) {
        Some(v) => want_num(v, "sublist")? as i64,
        None => len - from,
    };
    let to = (from + count).min(len).max(from);
    Ok(FeelVal::List(list[from as usize..to as usize].to_vec()))
}

fn flatten(v: &FeelVal, out: &mut Vec<FeelVal>) {
    match v {
        FeelVal::List(items) => {
            for it in items {
                flatten(it, out);
            }
        }
        other => out.push(other.clone()),
    }
}

fn string_join(args: &[FeelVal]) -> Result<FeelVal, FeelError> {
    let list = want_list(&arg(args, 0), "string join")?;
    let sep = match args.get(1) {
        Some(FeelVal::Str(s)) => s.clone(),
        _ => String::new(),
    };
    let mut parts = Vec::new();
    for it in list {
        match it {
            FeelVal::Str(s) => parts.push(s),
            FeelVal::Null => {}
            other => {
                return Err(FeelError(format!(
                    "string join: expected strings, got {}",
                    other.type_name()
                )))
            }
        }
    }
    Ok(FeelVal::Str(parts.join(&sep)))
}

fn sort(args: &[FeelVal], base: &HashMap<String, Value>) -> Result<FeelVal, FeelError> {
    let mut list = want_list(&arg(args, 0), "sort")?;
    match args.get(1) {
        Some(FeelVal::Function(f)) => {
            let f = f.clone();
            list.sort_by(
                |a, b| match super::eval::apply(&f, vec![a.clone(), b.clone()], base) {
                    Ok(FeelVal::Bool(true)) => std::cmp::Ordering::Less,
                    Ok(FeelVal::Bool(false)) => std::cmp::Ordering::Greater,
                    _ => std::cmp::Ordering::Equal,
                },
            );
        }
        _ => {
            list.sort_by(|a, b| super::eval::compare(a, b).unwrap_or(std::cmp::Ordering::Equal));
        }
    }
    Ok(FeelVal::List(list))
}

fn partition(args: &[FeelVal]) -> Result<FeelVal, FeelError> {
    let list = want_list(&arg(args, 0), "partition")?;
    let size = want_num(&arg(args, 1), "partition")? as usize;
    if size == 0 {
        return Ok(FeelVal::Null);
    }
    Ok(FeelVal::List(
        list.chunks(size)
            .map(|c| FeelVal::List(c.to_vec()))
            .collect(),
    ))
}

fn context_put(args: &[FeelVal]) -> Result<FeelVal, FeelError> {
    let mut map = match arg(args, 0) {
        FeelVal::Context(m) => m,
        other => {
            return Err(FeelError(format!(
                "context put: expected a context, got {}",
                other.type_name()
            )))
        }
    };
    // context put(ctx, key, value) or context put(ctx, [keys], value) nested.
    match (arg(args, 1), args.get(2)) {
        (FeelVal::Str(k), Some(v)) => {
            map.insert(k, v.clone());
        }
        (FeelVal::List(keys), Some(v)) => {
            put_nested(&mut map, &keys, v.clone());
        }
        _ => {}
    }
    Ok(FeelVal::Context(map))
}

fn put_nested(map: &mut BTreeMap<String, FeelVal>, keys: &[FeelVal], value: FeelVal) {
    let Some(FeelVal::Str(k)) = keys.first() else {
        return;
    };
    if keys.len() == 1 {
        map.insert(k.clone(), value);
    } else {
        let entry = map
            .entry(k.clone())
            .or_insert_with(|| FeelVal::Context(BTreeMap::new()));
        if let FeelVal::Context(inner) = entry {
            put_nested(inner, &keys[1..], value);
        }
    }
}

#[derive(Clone, Copy)]
enum RoundMode {
    Up,
    Down,
    HalfUp,
    HalfDown,
    HalfEven,
    Ceiling,
    Floor,
}

fn round_builtin(args: &[FeelVal], mode: RoundMode) -> Result<FeelVal, FeelError> {
    let n = want_num(&arg(args, 0), "round")?;
    let scale = match args.get(1) {
        Some(v) => want_num(v, "round")? as i32,
        None => 0,
    };
    Ok(FeelVal::num(round_scale(n, scale, mode)))
}

fn round_scale(n: f64, scale: i32, mode: RoundMode) -> f64 {
    let factor = 10f64.powi(scale);
    let x = n * factor;
    let rounded = match mode {
        RoundMode::Up => {
            if x >= 0.0 {
                x.ceil()
            } else {
                x.floor()
            }
        }
        RoundMode::Down => x.trunc(),
        RoundMode::HalfUp => x.abs().round() * x.signum(),
        RoundMode::HalfDown => {
            let frac = x.abs().fract();
            if frac > 0.5 {
                x.abs().ceil() * x.signum()
            } else {
                x.abs().floor() * x.signum()
            }
        }
        RoundMode::HalfEven => {
            let r = x.round();
            if (x - x.trunc()).abs() == 0.5 && (r as i64) % 2 != 0 {
                r - x.signum()
            } else {
                r
            }
        }
        RoundMode::Ceiling => x.ceil(),
        RoundMode::Floor => x.floor(),
    };
    rounded / factor
}

fn number_from(args: &[FeelVal]) -> Result<FeelVal, FeelError> {
    let raw = match arg(args, 0) {
        FeelVal::Str(s) => s,
        FeelVal::Int(i) => return Ok(FeelVal::Int(i)),
        FeelVal::Double(d) => return Ok(FeelVal::Double(d)),
        _ => return Ok(FeelVal::Null),
    };
    let grouping = match args.get(1) {
        Some(FeelVal::Str(s)) => s.clone(),
        _ => String::new(),
    };
    let decimal = match args.get(2) {
        Some(FeelVal::Str(s)) => s.clone(),
        _ => ".".to_string(),
    };
    let mut s = raw;
    if !grouping.is_empty() {
        s = s.replace(&grouping, "");
    }
    if decimal != "." {
        s = s.replace(&decimal, ".");
    }
    Ok(s.parse::<f64>().map(FeelVal::num).unwrap_or(FeelVal::Null))
}

fn context_from(v: &FeelVal) -> Result<FeelVal, FeelError> {
    let list = want_list(v, "context")?;
    let mut map = BTreeMap::new();
    for entry in list {
        if let FeelVal::Context(e) = entry {
            if let (Some(FeelVal::Str(k)), Some(val)) = (e.get("key"), e.get("value")) {
                map.insert(k.clone(), val.clone());
            }
        }
    }
    Ok(FeelVal::Context(map))
}

fn substring(args: &[FeelVal]) -> Result<FeelVal, FeelError> {
    let s: Vec<char> = want_str(&arg(args, 0), "substring")?.chars().collect();
    let start = want_num(&arg(args, 1), "substring")? as i64;
    let len = s.len() as i64;
    let from = if start > 0 {
        start - 1
    } else if start < 0 {
        len + start
    } else {
        0
    };
    if from < 0 || from >= len {
        return Ok(FeelVal::Str(String::new()));
    }
    let count = match args.get(2) {
        Some(v) if !matches!(v, FeelVal::Null) => want_num(v, "substring")? as i64,
        _ => len - from,
    };
    let to = (from + count.max(0)).min(len);
    Ok(FeelVal::Str(s[from as usize..to as usize].iter().collect()))
}

// --- temporal builtins ------------------------------------------------------

fn date_builtin(args: &[FeelVal]) -> Result<FeelVal, FeelError> {
    match (arg(args, 0), arg(args, 1), arg(args, 2)) {
        (FeelVal::Str(s), FeelVal::Null, _) => {
            Ok(Date::parse(&s).map(FeelVal::Date).unwrap_or(FeelVal::Null))
        }
        (FeelVal::DateTime(dt), FeelVal::Null, _) => Ok(FeelVal::Date(dt.date)),
        (y, m, d) if y.is_number() => {
            let date = Date::new(
                want_num(&y, "date")? as i32,
                want_num(&m, "date")? as u32,
                want_num(&d, "date")? as u32,
            );
            Ok(date.map(FeelVal::Date).unwrap_or(FeelVal::Null))
        }
        _ => Ok(FeelVal::Null),
    }
}

fn time_builtin(args: &[FeelVal]) -> Result<FeelVal, FeelError> {
    match arg(args, 0) {
        FeelVal::Str(s) => Ok(Time::parse(&s).map(FeelVal::Time).unwrap_or(FeelVal::Null)),
        FeelVal::DateTime(dt) => Ok(FeelVal::Time(dt.time)),
        h if h.is_number() => {
            let mut t = Time::new(
                want_num(&h, "time")? as u32,
                want_num(&arg(args, 1), "time")? as u32,
                want_num(&arg(args, 2), "time")? as u32,
                0,
            );
            if let FeelVal::DtDur(off) = arg(args, 3) {
                t.offset_seconds = Some((off.nanos / 1_000_000_000) as i32);
            }
            Ok(FeelVal::Time(t))
        }
        _ => Ok(FeelVal::Null),
    }
}

fn datetime_builtin(args: &[FeelVal]) -> Result<FeelVal, FeelError> {
    match (arg(args, 0), arg(args, 1)) {
        (FeelVal::Str(s), FeelVal::Null) => Ok(DateTime::parse(&s)
            .map(FeelVal::DateTime)
            .unwrap_or(FeelVal::Null)),
        (FeelVal::Date(d), FeelVal::Time(t)) => {
            Ok(FeelVal::DateTime(DateTime { date: d, time: t }))
        }
        (FeelVal::Date(d), FeelVal::Null) => Ok(FeelVal::DateTime(DateTime {
            date: d,
            time: Time::new(0, 0, 0, 0),
        })),
        _ => Ok(FeelVal::Null),
    }
}

/// Parses any ISO duration string into a duration value (years/months or
/// days/time).
pub fn parse_duration(s: &str) -> Option<FeelVal> {
    if let Some(d) = YmDuration::parse(s) {
        if s.contains('Y') || (s.contains('M') && !s.contains('T') && !s.contains('D')) {
            return Some(FeelVal::YmDur(d));
        }
    }
    DtDuration::parse(s).map(FeelVal::DtDur)
}

/// Infers the temporal type of an `@"…"` literal or a `date and time` string.
pub fn parse_temporal(s: &str) -> Option<FeelVal> {
    if s.starts_with('P') || s.starts_with("-P") || s.starts_with("+P") {
        return parse_duration(s);
    }
    if s.contains('T') || s.contains('t') {
        return DateTime::parse(s).map(FeelVal::DateTime);
    }
    if s.contains(':') {
        return Time::parse(s).map(FeelVal::Time);
    }
    Date::parse(s).map(FeelVal::Date)
}

fn years_months_between(a: &FeelVal, b: &FeelVal) -> Result<FeelVal, FeelError> {
    let (from, to) = match (date_of(a), date_of(b)) {
        (Some(x), Some(y)) => (x, y),
        _ => return Ok(FeelVal::Null),
    };
    let mut months =
        (to.year as i64 - from.year as i64) * 12 + (to.month as i64 - from.month as i64);
    if to.day < from.day && months > 0 {
        months -= 1;
    } else if to.day > from.day && months < 0 {
        months += 1;
    }
    Ok(FeelVal::YmDur(YmDuration::new(months)))
}

fn date_of(v: &FeelVal) -> Option<Date> {
    match v {
        FeelVal::Date(d) => Some(*d),
        FeelVal::DateTime(dt) => Some(dt.date),
        _ => None,
    }
}

fn now_datetime() -> Option<DateTime> {
    let secs = SystemTime::now().duration_since(UNIX_EPOCH).ok()?.as_secs() as i64;
    let days = secs.div_euclid(86_400);
    let sod = secs.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    Some(DateTime {
        date: Date { year, month, day },
        time: Time {
            hour: (sod / 3600) as u32,
            minute: ((sod / 60) % 60) as u32,
            second: (sod % 60) as u32,
            nano: 0,
            offset_seconds: Some(0),
            zone: None,
        },
    })
}

// --- minimal JSON -----------------------------------------------------------

fn to_json(v: &FeelVal) -> String {
    match v {
        FeelVal::Null => "null".to_string(),
        FeelVal::Bool(b) => b.to_string(),
        FeelVal::Int(i) => i.to_string(),
        FeelVal::Double(d) => crate::model::format_double(*d),
        FeelVal::Str(s) => json_string(s),
        FeelVal::List(items) => {
            let parts: Vec<String> = items.iter().map(to_json).collect();
            format!("[{}]", parts.join(","))
        }
        FeelVal::Context(m) => {
            let parts: Vec<String> = m
                .iter()
                .map(|(k, v)| format!("{}:{}", json_string(k), to_json(v)))
                .collect();
            format!("{{{}}}", parts.join(","))
        }
        other => json_string(&other.to_feel_string().unwrap_or_default()),
    }
}

fn json_string(s: &str) -> String {
    let mut out = String::from("\"");
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            _ => out.push(c),
        }
    }
    out.push('"');
    out
}

fn from_json(s: &str) -> Option<FeelVal> {
    let chars: Vec<char> = s.chars().collect();
    let mut pos = 0;
    let v = json_value(&chars, &mut pos)?;
    json_ws(&chars, &mut pos);
    if pos == chars.len() {
        Some(v)
    } else {
        None
    }
}

fn json_ws(c: &[char], pos: &mut usize) {
    while *pos < c.len() && c[*pos].is_whitespace() {
        *pos += 1;
    }
}

fn json_value(c: &[char], pos: &mut usize) -> Option<FeelVal> {
    json_ws(c, pos);
    match c.get(*pos)? {
        '{' => json_object(c, pos),
        '[' => json_array(c, pos),
        '"' => Some(FeelVal::Str(json_str(c, pos)?)),
        't' => json_lit(c, pos, "true", FeelVal::Bool(true)),
        'f' => json_lit(c, pos, "false", FeelVal::Bool(false)),
        'n' => json_lit(c, pos, "null", FeelVal::Null),
        _ => json_number(c, pos),
    }
}

fn json_lit(c: &[char], pos: &mut usize, lit: &str, v: FeelVal) -> Option<FeelVal> {
    for ch in lit.chars() {
        if c.get(*pos) != Some(&ch) {
            return None;
        }
        *pos += 1;
    }
    Some(v)
}

fn json_number(c: &[char], pos: &mut usize) -> Option<FeelVal> {
    let start = *pos;
    while *pos < c.len() && matches!(c[*pos], '0'..='9' | '-' | '+' | '.' | 'e' | 'E') {
        *pos += 1;
    }
    let text: String = c[start..*pos].iter().collect();
    text.parse::<f64>().ok().map(FeelVal::num)
}

fn json_str(c: &[char], pos: &mut usize) -> Option<String> {
    *pos += 1; // opening quote
    let mut s = String::new();
    while *pos < c.len() {
        let ch = c[*pos];
        *pos += 1;
        match ch {
            '"' => return Some(s),
            '\\' => {
                let e = c.get(*pos)?;
                *pos += 1;
                match e {
                    '"' => s.push('"'),
                    '\\' => s.push('\\'),
                    '/' => s.push('/'),
                    'n' => s.push('\n'),
                    'r' => s.push('\r'),
                    't' => s.push('\t'),
                    'u' => {
                        let hex: String = c.get(*pos..*pos + 4)?.iter().collect();
                        *pos += 4;
                        let code = u32::from_str_radix(&hex, 16).ok()?;
                        s.push(char::from_u32(code)?);
                    }
                    other => s.push(*other),
                }
            }
            _ => s.push(ch),
        }
    }
    None
}

fn json_array(c: &[char], pos: &mut usize) -> Option<FeelVal> {
    *pos += 1; // [
    let mut items = Vec::new();
    json_ws(c, pos);
    if c.get(*pos) == Some(&']') {
        *pos += 1;
        return Some(FeelVal::List(items));
    }
    loop {
        items.push(json_value(c, pos)?);
        json_ws(c, pos);
        match c.get(*pos) {
            Some(',') => {
                *pos += 1;
            }
            Some(']') => {
                *pos += 1;
                return Some(FeelVal::List(items));
            }
            _ => return None,
        }
    }
}

fn json_object(c: &[char], pos: &mut usize) -> Option<FeelVal> {
    *pos += 1; // {
    let mut map = BTreeMap::new();
    json_ws(c, pos);
    if c.get(*pos) == Some(&'}') {
        *pos += 1;
        return Some(FeelVal::Context(map));
    }
    loop {
        json_ws(c, pos);
        if c.get(*pos) != Some(&'"') {
            return None;
        }
        let key = json_str(c, pos)?;
        json_ws(c, pos);
        if c.get(*pos) != Some(&':') {
            return None;
        }
        *pos += 1;
        let value = json_value(c, pos)?;
        map.insert(key, value);
        json_ws(c, pos);
        match c.get(*pos) {
            Some(',') => {
                *pos += 1;
            }
            Some('}') => {
                *pos += 1;
                return Some(FeelVal::Context(map));
            }
            _ => return None,
        }
    }
}

// --- new-parity helpers -----------------------------------------------------

/// Maps a `decimal(n, scale, mode)` rounding-mode string (feel-scala's
/// `java.math.RoundingMode` names) to a [`RoundMode`].
fn rounding_mode(name: &str) -> Result<RoundMode, FeelError> {
    match name {
        "UP" => Ok(RoundMode::Up),
        "DOWN" => Ok(RoundMode::Down),
        "CEILING" => Ok(RoundMode::Ceiling),
        "FLOOR" => Ok(RoundMode::Floor),
        "HALF_UP" => Ok(RoundMode::HalfUp),
        "HALF_DOWN" => Ok(RoundMode::HalfDown),
        "HALF_EVEN" | "UNNECESSARY" => Ok(RoundMode::HalfEven),
        other => Err(FeelError(format!(
            "decimal: unknown rounding mode '{other}'"
        ))),
    }
}

/// A dependency-free `[0, 1)` double for `random number()`, seeded from the
/// wall clock and a monotonic counter (nondeterministic, like `now()`).
fn rand_unit() -> f64 {
    (next_random() >> 11) as f64 / (1u64 << 53) as f64
}

/// A single 64-bit draw from a SplitMix64 generator seeded per call from the
/// clock and a process-wide counter, so successive calls differ.
fn next_random() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut z = nanos
        .wrapping_add(n.wrapping_mul(0x9E37_79B9_7F4A_7C15))
        .wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// Generates a random RFC-4122 version-4 UUID string.
fn uuid_v4() -> String {
    let a = next_random();
    let b = next_random();
    let mut bytes = [0u8; 16];
    bytes[..8].copy_from_slice(&a.to_be_bytes());
    bytes[8..].copy_from_slice(&b.to_be_bytes());
    bytes[6] = (bytes[6] & 0x0f) | 0x40; // version 4
    bytes[8] = (bytes[8] & 0x3f) | 0x80; // variant 10
    let h: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
    format!(
        "{}-{}-{}-{}-{}",
        &h[0..8],
        &h[8..12],
        &h[12..16],
        &h[16..20],
        &h[20..32]
    )
}

const B64: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

/// Standard Base64 encoding (with `=` padding), matching `java.util.Base64`.
fn base64_encode(input: &[u8]) -> String {
    let mut out = String::with_capacity(input.len().div_ceil(3) * 4);
    for chunk in input.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = *chunk.get(1).unwrap_or(&0) as u32;
        let b2 = *chunk.get(2).unwrap_or(&0) as u32;
        let n = (b0 << 16) | (b1 << 8) | b2;
        out.push(B64[(n >> 18) as usize & 63] as char);
        out.push(B64[(n >> 12) as usize & 63] as char);
        out.push(if chunk.len() > 1 {
            B64[(n >> 6) as usize & 63] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            B64[n as usize & 63] as char
        } else {
            '='
        });
    }
    out
}

/// Standard Base64 decoding; returns `None` on any invalid input.
fn base64_decode(input: &str) -> Option<Vec<u8>> {
    let val = |c: u8| -> Option<u32> {
        match c {
            b'A'..=b'Z' => Some((c - b'A') as u32),
            b'a'..=b'z' => Some((c - b'a' + 26) as u32),
            b'0'..=b'9' => Some((c - b'0' + 52) as u32),
            b'+' => Some(62),
            b'/' => Some(63),
            _ => None,
        }
    };
    let cleaned: Vec<u8> = input.bytes().filter(|b| !b.is_ascii_whitespace()).collect();
    let stripped: &[u8] = cleaned
        .strip_suffix(b"==")
        .or_else(|| cleaned.strip_suffix(b"="))
        .unwrap_or(&cleaned);
    let mut out = Vec::with_capacity(stripped.len() / 4 * 3);
    let mut acc = 0u32;
    let mut bits = 0u32;
    for &c in stripped {
        let v = val(c)?;
        acc = (acc << 6) | v;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((acc >> bits) as u8);
        }
    }
    Some(out)
}

/// `assert(value, condition)` / `assert(value, condition, cause)`: returns
/// `value` when `condition` is `true`, otherwise raises an error.
fn assert_builtin(args: &[FeelVal]) -> Result<FeelVal, FeelError> {
    let value = arg(args, 0);
    let ok = matches!(arg(args, 1), FeelVal::Bool(true));
    if ok {
        Ok(value)
    } else {
        let cause = match args.get(2) {
            Some(FeelVal::Str(s)) => s.clone(),
            _ => "The condition is not fulfilled".to_string(),
        };
        Err(FeelError(format!("assert failed: {cause}")))
    }
}

// --- range / interval functions (Allen's algebra) ---------------------------

/// The `(start, start_closed, end, end_closed)` view of a bounded range, or
/// `None` if either bound is open-ended (unbounded ranges are not comparable
/// under the interval functions).
fn range_parts(r: &Range) -> Option<(&FeelVal, bool, &FeelVal, bool)> {
    match (&r.start, &r.end) {
        (Some(s), Some(e)) => Some((s, r.start_inclusive, e, r.end_inclusive)),
        _ => None,
    }
}

/// Whether a value is a valid FEEL range *point* (number, date, time,
/// date-time or duration — feel-scala excludes strings and booleans).
fn is_point(v: &FeelVal) -> bool {
    matches!(
        v,
        FeelVal::Int(_)
            | FeelVal::Double(_)
            | FeelVal::Date(_)
            | FeelVal::Time(_)
            | FeelVal::DateTime(_)
            | FeelVal::YmDur(_)
            | FeelVal::DtDur(_)
    )
}

fn lt(a: &FeelVal, b: &FeelVal) -> bool {
    compare(a, b) == Some(std::cmp::Ordering::Less)
}
fn gt(a: &FeelVal, b: &FeelVal) -> bool {
    compare(a, b) == Some(std::cmp::Ordering::Greater)
}
fn eqp(a: &FeelVal, b: &FeelVal) -> bool {
    compare(a, b) == Some(std::cmp::Ordering::Equal)
}

/// The reference point of an argument (a range's start, or the point itself),
/// used to decide whether the two arguments are type-comparable.
fn ref_point(v: &FeelVal) -> Option<&FeelVal> {
    match v {
        FeelVal::Range(r) => r.start.as_ref(),
        other if is_point(other) => Some(other),
        _ => None,
    }
}

/// Evaluates one of the 14 Allen interval-relation builtins, returning `null`
/// for incomparable or unsupported argument shapes (feel-scala semantics).
fn interval(name: &str, a: &FeelVal, b: &FeelVal) -> FeelVal {
    // Comparability: both reference points must exist, be points, and be of a
    // mutually comparable type.
    let comparable = match (ref_point(a), ref_point(b)) {
        (Some(pa), Some(pb)) => is_point(pa) && is_point(pb) && compare(pa, pb).is_some(),
        _ => false,
    };
    if !comparable {
        return FeelVal::Null;
    }
    let result = interval_bool(name, a, b);
    match result {
        Some(v) => FeelVal::Bool(v),
        None => FeelVal::Null,
    }
}

fn interval_bool(name: &str, a: &FeelVal, b: &FeelVal) -> Option<bool> {
    use FeelVal::Range as R;
    // Destructure ranges once where present.
    let ra = if let R(r) = a {
        Some(range_parts(r)?)
    } else {
        None
    };
    let rb = if let R(r) = b {
        Some(range_parts(r)?)
    } else {
        None
    };
    match name {
        "before" => match (ra, rb) {
            (Some((_, _, e1, e1c)), Some((s2, s2c, _, _))) => {
                Some(lt(e1, s2) || ((!e1c || !s2c) && eqp(e1, s2)))
            }
            (None, Some((s2, s2c, _, _))) => Some(lt(a, s2) || (eqp(a, s2) && !s2c)),
            (Some((_, _, e1, e1c)), None) => Some(lt(e1, b) || (eqp(e1, b) && !e1c)),
            (None, None) => Some(lt(a, b)),
        },
        "after" => match (ra, rb) {
            (Some((s1, s1c, _, _)), Some((_, _, e2, e2c))) => {
                Some(gt(s1, e2) || ((!s1c || !e2c) && eqp(s1, e2)))
            }
            (None, Some((_, _, e2, e2c))) => Some(gt(a, e2) || (eqp(a, e2) && !e2c)),
            (Some((s1, s1c, _, _)), None) => Some(gt(s1, b) || (eqp(s1, b) && !s1c)),
            (None, None) => Some(gt(a, b)),
        },
        "meets" => {
            let ((_, _, e1, e1c), (s2, s2c, _, _)) = (ra?, rb?);
            Some(e1c && s2c && eqp(e1, s2))
        }
        "met by" => {
            let ((s1, s1c, _, _), (_, _, e2, e2c)) = (ra?, rb?);
            Some(s1c && e2c && eqp(s1, e2))
        }
        "overlaps" => {
            let ((s1, s1c, e1, e1c), (s2, s2c, e2, e2c)) = (ra?, rb?);
            Some(
                (gt(e1, s2) || (eqp(e1, s2) && e1c && s2c))
                    && (lt(s1, e2) || (eqp(s1, e2) && s1c && e2c)),
            )
        }
        "overlaps before" => {
            let ((s1, s1c, e1, e1c), (s2, s2c, e2, e2c)) = (ra?, rb?);
            Some(
                (lt(s1, s2) || (eqp(s1, s2) && s1c && !s2c))
                    && (gt(e1, s2) || (eqp(e1, s2) && e1c && s2c))
                    && (lt(e1, e2) || (eqp(e1, e2) && (!e1c || e2c))),
            )
        }
        "overlaps after" => {
            let ((s1, s1c, e1, e1c), (s2, s2c, e2, e2c)) = (ra?, rb?);
            Some(
                (lt(s2, s1) || (eqp(s2, s1) && s2c && !s1c))
                    && (gt(e2, s1) || (eqp(e2, s1) && e2c && s1c))
                    && (lt(e2, e1) || (eqp(e2, e1) && (!e2c || e1c))),
            )
        }
        "finishes" => match (ra, rb) {
            (Some((s1, s1c, e1, e1c)), Some((s2, s2c, e2, e2c))) => {
                Some(e1c == e2c && eqp(e1, e2) && (gt(s1, s2) || (eqp(s1, s2) && (!s1c || s2c))))
            }
            (None, Some((_, _, e2, e2c))) => Some(e2c && eqp(e2, a)),
            _ => None,
        },
        "finished by" => match (ra, rb) {
            (Some((s1, s1c, e1, e1c)), Some((s2, s2c, e2, e2c))) => {
                Some(e1c == e2c && eqp(e1, e2) && (lt(s1, s2) || (eqp(s1, s2) && (s1c || !s2c))))
            }
            (Some((_, _, e1, e1c)), None) => Some(e1c && eqp(e1, b)),
            _ => None,
        },
        "includes" => match (ra, rb) {
            (Some((s1, s1c, e1, e1c)), Some((s2, s2c, e2, e2c))) => Some(
                (lt(s1, s2) || (eqp(s1, s2) && (s1c || !s2c)))
                    && (gt(e1, e2) || (eqp(e1, e2) && (e1c || !e2c))),
            ),
            (Some((s1, s1c, e1, e1c)), None) => {
                Some((lt(s1, b) && gt(e1, b)) || (eqp(s1, b) && s1c) || (eqp(e1, b) && e1c))
            }
            _ => None,
        },
        "during" => match (ra, rb) {
            (Some((s1, s1c, e1, e1c)), Some((s2, s2c, e2, e2c))) => Some(
                (lt(s2, s1) || (eqp(s2, s1) && (s2c || !s1c)))
                    && (gt(e2, e1) || (eqp(e2, e1) && (e2c || !e1c))),
            ),
            (None, Some((s2, s2c, e2, e2c))) => {
                Some((lt(s2, a) && gt(e2, a)) || (eqp(s2, a) && s2c) || (eqp(e2, a) && e2c))
            }
            _ => None,
        },
        "starts" => match (ra, rb) {
            (Some((s1, s1c, e1, e1c)), Some((s2, s2c, e2, e2c))) => {
                Some(eqp(s1, s2) && s1c == s2c && (lt(e1, e2) || (eqp(e1, e2) && (!e1c || e2c))))
            }
            (None, Some((s2, s2c, _, _))) => Some(eqp(s2, a) && s2c),
            _ => None,
        },
        "started by" => match (ra, rb) {
            (Some((s1, s1c, e1, e1c)), Some((s2, s2c, e2, e2c))) => {
                Some(eqp(s1, s2) && s1c == s2c && (lt(e2, e1) || (eqp(e2, e1) && (!e2c || e1c))))
            }
            (Some((s1, s1c, _, _)), None) => Some(eqp(s1, b) && s1c),
            _ => None,
        },
        "coincides" => match (ra, rb) {
            (Some((s1, s1c, e1, e1c)), Some((s2, s2c, e2, e2c))) => {
                Some(eqp(s1, s2) && s1c == s2c && eqp(e1, e2) && e1c == e2c)
            }
            (None, None) => Some(eqp(a, b)),
            _ => None,
        },
        _ => None,
    }
}
