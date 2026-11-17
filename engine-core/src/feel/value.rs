//! The internal FEEL value type.
//!
//! [`FeelVal`] is a superset of the engine [`Value`]: it adds the temporal
//! types, ranges and first-class functions that a faithful FEEL evaluator needs
//! while evaluating, but which the engine's variable store does not model. At
//! the API boundary ([`FeelVal::into_value`]) the extra types collapse to their
//! canonical FEEL string form — exactly how Zeebe serialises a temporal value
//! when it lands in a process variable.

use std::collections::{BTreeMap, HashSet};
use std::rc::Rc;

use super::ast::Node;
use super::temporal::{Date, DateTime, DtDuration, Time, YmDuration};
use crate::model::{format_double, Value};

/// A value produced while evaluating a FEEL expression.
#[derive(Clone, Debug, PartialEq)]
pub enum FeelVal {
    Null,
    Bool(bool),
    Int(i64),
    Double(f64),
    Str(String),
    List(Vec<FeelVal>),
    Context(BTreeMap<String, FeelVal>),
    Date(Date),
    Time(Time),
    DateTime(DateTime),
    YmDur(YmDuration),
    DtDur(DtDuration),
    Range(Box<Range>),
    Function(Func),
    /// A value carrying trusted `camunda.secrets.<name>` provenance (#1296).
    ///
    /// This is an *evaluation-internal* wrapper: when a trusted
    /// (`SECRET_REFERENCE`-kind) cluster variable is read, the evaluator wraps
    /// its value together with the secret names that value carries, so the
    /// provenance travels with the *individual value* rather than being
    /// recorded wholesale at the root read. A member/index/filter selection
    /// then surfaces only the names of the value actually selected — never the
    /// unselected siblings — and the wrapper is transparent to every FEEL
    /// operation (it delegates to the inner value). It never escapes an
    /// expression: [`FeelVal::into_value`] strips it.
    SecretRef {
        value: Box<FeelVal>,
        names: Rc<HashSet<String>>,
    },
}

/// A FEEL range/interval, used by interval literals and `in`/`between` tests.
#[derive(Clone, Debug, PartialEq)]
pub struct Range {
    pub start: Option<FeelVal>,
    pub start_inclusive: bool,
    pub end: Option<FeelVal>,
    pub end_inclusive: bool,
}

/// A callable FEEL value: either a named builtin or a user-defined lambda.
#[derive(Clone, Debug, PartialEq)]
pub enum Func {
    Builtin(&'static str),
    Lambda {
        params: Vec<String>,
        body: Rc<Node>,
        closure: Rc<BTreeMap<String, FeelVal>>,
    },
}

impl FeelVal {
    /// Builds a numeric value, narrowing a finite integral `f64` to [`FeelVal::Int`]
    /// (matching [`Value::number`]) and mapping non-finite results to `null`.
    pub fn num(n: f64) -> FeelVal {
        if !n.is_finite() {
            return FeelVal::Null;
        }
        if n.fract() == 0.0 && n.abs() < i64::MAX as f64 {
            FeelVal::Int(n as i64)
        } else {
            FeelVal::Double(n)
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self {
            FeelVal::Int(i) => Some(*i as f64),
            FeelVal::Double(d) => Some(*d),
            FeelVal::SecretRef { value, .. } => value.as_f64(),
            _ => None,
        }
    }

    pub fn is_number(&self) -> bool {
        match self {
            FeelVal::Int(_) | FeelVal::Double(_) => true,
            FeelVal::SecretRef { value, .. } => value.is_number(),
            _ => false,
        }
    }

    /// The boolean this value holds, if it is a boolean (FEEL never coerces).
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            FeelVal::Bool(b) => Some(*b),
            FeelVal::SecretRef { value, .. } => value.as_bool(),
            _ => None,
        }
    }

    pub fn type_name(&self) -> &'static str {
        match self {
            FeelVal::Null => "null",
            FeelVal::Bool(_) => "boolean",
            FeelVal::Int(_) | FeelVal::Double(_) => "number",
            FeelVal::Str(_) => "string",
            FeelVal::List(_) => "list",
            FeelVal::Context(_) => "context",
            FeelVal::Date(_) => "date",
            FeelVal::Time(_) => "time",
            FeelVal::DateTime(_) => "date and time",
            FeelVal::YmDur(_) => "years and months duration",
            FeelVal::DtDur(_) => "days and time duration",
            FeelVal::Range(_) => "range",
            FeelVal::Function(_) => "function",
            FeelVal::SecretRef { value, .. } => value.type_name(),
        }
    }

    /// Lifts an engine [`Value`] into the FEEL domain.
    pub fn from_value(v: Value) -> FeelVal {
        match v {
            Value::Null => FeelVal::Null,
            Value::Bool(b) => FeelVal::Bool(b),
            Value::Int(i) => FeelVal::Int(i),
            Value::Double(d) => FeelVal::Double(d),
            Value::Str(s) => FeelVal::Str(s),
            Value::List(items) => {
                FeelVal::List(items.into_iter().map(FeelVal::from_value).collect())
            }
            Value::Map(entries) => FeelVal::Context(
                entries
                    .into_iter()
                    .map(|(k, v)| (k, FeelVal::from_value(v)))
                    .collect(),
            ),
        }
    }

    /// Collapses back to an engine [`Value`]. Temporal values render as their
    /// canonical FEEL string; ranges and functions (which never legitimately
    /// escape an expression) become `null`.
    pub fn into_value(self) -> Value {
        match self {
            FeelVal::Null => Value::Null,
            FeelVal::Bool(b) => Value::Bool(b),
            FeelVal::Int(i) => Value::Int(i),
            FeelVal::Double(d) => Value::Double(d),
            FeelVal::Str(s) => Value::Str(s),
            FeelVal::List(items) => {
                Value::List(items.into_iter().map(FeelVal::into_value).collect())
            }
            FeelVal::Context(entries) => Value::Map(
                entries
                    .into_iter()
                    .map(|(k, v)| (k, v.into_value()))
                    .collect(),
            ),
            FeelVal::Date(d) => Value::Str(d.format()),
            FeelVal::Time(t) => Value::Str(t.format()),
            FeelVal::DateTime(dt) => Value::Str(dt.format()),
            FeelVal::YmDur(d) => Value::Str(d.format()),
            FeelVal::DtDur(d) => Value::Str(d.format()),
            FeelVal::Range(_) | FeelVal::Function(_) => Value::Null,
            // The provenance wrapper never escapes an expression (#1296).
            FeelVal::SecretRef { value, .. } => value.into_value(),
        }
    }

    /// The string rendering used by `eval_string` and the `string()` builtin.
    pub fn to_feel_string(&self) -> Option<String> {
        match self {
            FeelVal::Str(s) => Some(s.clone()),
            FeelVal::Int(i) => Some(i.to_string()),
            FeelVal::Double(d) => Some(format_double(*d)),
            FeelVal::Bool(b) => Some(b.to_string()),
            FeelVal::Date(d) => Some(d.format()),
            FeelVal::Time(t) => Some(t.format()),
            FeelVal::DateTime(dt) => Some(dt.format()),
            FeelVal::YmDur(d) => Some(d.format()),
            FeelVal::DtDur(d) => Some(d.format()),
            FeelVal::Null => Some("null".to_string()),
            FeelVal::List(items) => {
                let parts: Option<Vec<String>> = items.iter().map(|i| i.to_feel_string()).collect();
                parts.map(|p| format!("[{}]", p.join(", ")))
            }
            FeelVal::Context(_) | FeelVal::Range(_) | FeelVal::Function(_) => None,
            FeelVal::SecretRef { value, .. } => value.to_feel_string(),
        }
    }

    /// Strips the provenance wrapper, returning the plain inner value (#1296).
    /// Applied at the function-argument boundary: a value's provenance is
    /// surfaced when it is *selected*, and the wrapper is transparent to FEEL
    /// operators, but a builtin that pattern-matches on the concrete variant
    /// (e.g. `want_list`) must see the underlying value, not the wrapper.
    pub fn strip_provenance(self) -> FeelVal {
        match self {
            FeelVal::SecretRef { value, .. } => *value,
            other => other,
        }
    }
}

/// Re-taints `v` with the subset of `names` reachable within it (#1296). A
/// structured value keeps only the names under the selected path; a scalar
/// keeps the names that match its own reference text.
pub fn taint_with(v: FeelVal, names: &Rc<HashSet<String>>) -> FeelVal {
    if names.is_empty() {
        return v;
    }
    match v {
        FeelVal::Str(ref s) => {
            let name = super::secret_reference_name(s).map(str::to_string);
            match name {
                Some(n) if names.contains(n.as_str()) => FeelVal::SecretRef {
                    value: Box::new(v),
                    names: Rc::new(HashSet::from([n])),
                },
                _ => v,
            }
        }
        FeelVal::List(_) | FeelVal::Context(_) => {
            let sub: HashSet<String> = names
                .iter()
                .filter(|n| reachable_in(&v, n))
                .cloned()
                .collect();
            if sub.is_empty() {
                v
            } else {
                FeelVal::SecretRef {
                    value: Box::new(v),
                    names: Rc::new(sub),
                }
            }
        }
        other => other,
    }
}

/// `true` when the reference `camunda.secrets.<name>` is reachable in `v`.
fn reachable_in(v: &FeelVal, name: &str) -> bool {
    match v {
        FeelVal::Str(s) => super::secret_reference_name(s) == Some(name),
        FeelVal::List(items) => items.iter().any(|i| reachable_in(i, name)),
        FeelVal::Context(entries) => entries.values().any(|i| reachable_in(i, name)),
        FeelVal::SecretRef { value, .. } => reachable_in(value, name),
        _ => false,
    }
}
