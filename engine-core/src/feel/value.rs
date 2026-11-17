//! The internal FEEL value type.
//!
//! [`FeelVal`] is a superset of the engine [`Value`]: it adds the temporal
//! types, ranges and first-class functions that a faithful FEEL evaluator needs
//! while evaluating, but which the engine's variable store does not model. At
//! the API boundary ([`FeelVal::into_value`]) the extra types collapse to their
//! canonical FEEL string form — exactly how Zeebe serialises a temporal value
//! when it lands in a process variable.

use std::collections::BTreeMap;
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
            _ => None,
        }
    }

    pub fn is_number(&self) -> bool {
        matches!(self, FeelVal::Int(_) | FeelVal::Double(_))
    }

    /// The boolean this value holds, if it is a boolean (FEEL never coerces).
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            FeelVal::Bool(b) => Some(*b),
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
        }
    }
}
