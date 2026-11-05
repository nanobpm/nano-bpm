//! The FEEL evaluator: walks an [`ast::Node`] tree producing a [`FeelVal`].

use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap};
use std::rc::Rc;

use super::ast::{BinOp, CallArgs, Node, Quantifier, RangeNode};
use super::builtins;
use super::error::FeelError;
use super::temporal::{Date, DateTime, DtDuration, Time, YmDuration};
use super::value::{FeelVal, Func, Range};
use crate::model::Value;

/// A lexical scope: a stack of frames over the engine's base variable map.
pub struct Ctx<'a> {
    base: &'a HashMap<String, Value>,
    scopes: Vec<BTreeMap<String, FeelVal>>,
}

impl<'a> Ctx<'a> {
    pub fn new(base: &'a HashMap<String, Value>) -> Ctx<'a> {
        Ctx {
            base,
            scopes: Vec::new(),
        }
    }

    fn lookup(&self, name: &str) -> Option<FeelVal> {
        for frame in self.scopes.iter().rev() {
            if let Some(v) = frame.get(name) {
                return Some(v.clone());
            }
        }
        self.base.get(name).map(|v| FeelVal::from_value(v.clone()))
    }

    fn push(&mut self, frame: BTreeMap<String, FeelVal>) {
        self.scopes.push(frame);
    }

    fn pop(&mut self) {
        self.scopes.pop();
    }
}

/// Evaluates a parsed expression against the engine's variables.
pub fn evaluate(node: &Node, base: &HashMap<String, Value>) -> Result<FeelVal, FeelError> {
    let mut ctx = Ctx::new(base);
    eval_node(node, &mut ctx)
}

fn eval_node(node: &Node, ctx: &mut Ctx) -> Result<FeelVal, FeelError> {
    match node {
        Node::Null => Ok(FeelVal::Null),
        Node::BoolLit(b) => Ok(FeelVal::Bool(*b)),
        Node::NumLit(v, is_int) => Ok(if *is_int {
            FeelVal::Int(*v as i64)
        } else {
            FeelVal::num(*v)
        }),
        Node::StrLit(s) => Ok(FeelVal::Str(s.clone())),
        Node::AtLit(s) => builtins::parse_temporal(s)
            .ok_or_else(|| FeelError(format!("invalid temporal literal @\"{s}\""))),
        Node::Var(name) => Ok(ctx.lookup(name).unwrap_or_else(|| {
            if builtins::is_builtin(name) {
                FeelVal::Function(Func::Builtin(builtins::canonical(name)))
            } else {
                FeelVal::Null
            }
        })),
        Node::Member(obj, name) => {
            let v = eval_node(obj, ctx)?;
            Ok(member(&v, name))
        }
        Node::Index(obj, idx) => eval_index(obj, idx, ctx),
        Node::Neg(inner) => {
            let v = eval_node(inner, ctx)?;
            match v {
                FeelVal::Int(i) => Ok(FeelVal::num(-(i as f64))),
                FeelVal::Double(d) => Ok(FeelVal::num(-d)),
                FeelVal::YmDur(d) => Ok(FeelVal::YmDur(YmDuration::new(-d.months))),
                FeelVal::DtDur(d) => Ok(FeelVal::DtDur(DtDuration::from_nanos(-d.nanos))),
                // An ill-typed operand is a suppressed failure: `null` (#1392).
                _ => Ok(FeelVal::Null),
            }
        }
        Node::Not(inner) => match eval_node(inner, ctx)? {
            FeelVal::Bool(b) => Ok(FeelVal::Bool(!b)),
            // `null` and an ill-typed operand both yield `null` (#1392).
            _ => Ok(FeelVal::Null),
        },
        Node::Bin(op, lhs, rhs) => eval_binary(*op, lhs, rhs, ctx),
        Node::List(items) => {
            let values = items
                .iter()
                .map(|n| eval_node(n, ctx))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(FeelVal::List(values))
        }
        Node::Context(entries) => {
            let mut map = BTreeMap::new();
            ctx.push(BTreeMap::new());
            for (key, value_node) in entries {
                let v = eval_node(value_node, ctx)?;
                map.insert(key.clone(), v.clone());
                if let Some(scope) = ctx.scopes.last_mut() {
                    scope.insert(key.clone(), v);
                }
            }
            ctx.pop();
            Ok(FeelVal::Context(map))
        }
        Node::If(cond, then, otherwise) => match eval_node(cond, ctx)? {
            FeelVal::Bool(true) => eval_node(then, ctx),
            // Any non-`true` condition (false, null, a non-boolean) takes the
            // else branch, as feel-scala does (#1393).
            _ => eval_node(otherwise, ctx),
        },
        Node::For(clauses, body) => {
            let mut out = Vec::new();
            eval_for(clauses, body, ctx, &mut BTreeMap::new(), &mut out)?;
            Ok(FeelVal::List(out))
        }
        Node::Quant(q, clauses, body) => eval_quant(*q, clauses, body, ctx),
        Node::Between(x, lo, hi) => {
            let xv = eval_node(x, ctx)?;
            let lov = eval_node(lo, ctx)?;
            let hiv = eval_node(hi, ctx)?;
            // An incomparable bound (incl. `null`) is a suppressed failure.
            Ok(match (compare(&xv, &lov), compare(&xv, &hiv)) {
                (Some(lo), Some(hi)) => FeelVal::Bool(lo.is_ge() && hi.is_le()),
                _ => FeelVal::Null,
            })
        }
        Node::In(x, rhs) => {
            let xv = eval_node(x, ctx)?;
            let rv = eval_node(rhs, ctx)?;
            Ok(in_test(&xv, &rv))
        }
        Node::InstanceOf(x, ty) => {
            let xv = eval_node(x, ctx)?;
            Ok(FeelVal::Bool(instance_of(&xv, ty)))
        }
        Node::FuncDef(params, body) => {
            let mut closure = BTreeMap::new();
            for frame in &ctx.scopes {
                for (k, v) in frame {
                    closure.insert(k.clone(), v.clone());
                }
            }
            Ok(FeelVal::Function(Func::Lambda {
                params: params.clone(),
                body: Rc::new((**body).clone()),
                closure: Rc::new(closure),
            }))
        }
        Node::Range(r) => eval_range(r, ctx),
        Node::Call(callee, args) => eval_call(callee, args, ctx),
    }
}

fn eval_range(r: &RangeNode, ctx: &mut Ctx) -> Result<FeelVal, FeelError> {
    let start = match &r.start {
        Some(n) => Some(eval_node(n, ctx)?),
        None => None,
    };
    let end = match &r.end {
        Some(n) => Some(eval_node(n, ctx)?),
        None => None,
    };
    Ok(FeelVal::Range(Box::new(Range {
        start,
        start_inclusive: r.start_inclusive,
        end,
        end_inclusive: r.end_inclusive,
    })))
}

fn eval_for(
    clauses: &[(String, Node)],
    body: &Node,
    ctx: &mut Ctx,
    frame: &mut BTreeMap<String, FeelVal>,
    out: &mut Vec<FeelVal>,
) -> Result<(), FeelError> {
    if clauses.is_empty() {
        ctx.push(frame.clone());
        let v = eval_node(body, ctx);
        ctx.pop();
        out.push(v?);
        return Ok(());
    }
    let (name, source) = &clauses[0];
    ctx.push(frame.clone());
    let items = iterable(&eval_node(source, ctx)?);
    ctx.pop();
    for item in items {
        frame.insert(name.clone(), item);
        eval_for(&clauses[1..], body, ctx, frame, out)?;
    }
    frame.remove(name);
    Ok(())
}

fn eval_quant(
    q: Quantifier,
    clauses: &[(String, Node)],
    body: &Node,
    ctx: &mut Ctx,
) -> Result<FeelVal, FeelError> {
    let mut results = Vec::new();
    eval_for(clauses, body, ctx, &mut BTreeMap::new(), &mut results)?;
    let outcome = match q {
        Quantifier::Some => results.iter().any(|v| v == &FeelVal::Bool(true)),
        Quantifier::Every => results.iter().all(|v| v == &FeelVal::Bool(true)),
    };
    Ok(FeelVal::Bool(outcome))
}

/// Turns a value into an iteration source: lists iterate their items; a range of
/// integers expands to the inclusive sequence; anything else yields itself.
fn iterable(v: &FeelVal) -> Vec<FeelVal> {
    match v {
        FeelVal::List(items) => items.clone(),
        FeelVal::Range(r) => {
            if let (Some(FeelVal::Int(a)), Some(FeelVal::Int(b))) = (&r.start, &r.end) {
                let (a, b) = (*a, *b);
                if a <= b {
                    (a..=b).map(FeelVal::Int).collect()
                } else {
                    (b..=a).rev().map(FeelVal::Int).collect()
                }
            } else {
                vec![v.clone()]
            }
        }
        other => vec![other.clone()],
    }
}

fn eval_index(obj: &Node, idx: &Node, ctx: &mut Ctx) -> Result<FeelVal, FeelError> {
    let target = eval_node(obj, ctx)?;
    let list = match &target {
        FeelVal::List(items) => items.clone(),
        // A scalar behaves like a singleton list under a filter.
        other => vec![other.clone()],
    };
    // An index expression that resolves to a number in the current scope selects
    // a 1-based element (negative counts from the end).
    if let Ok(n) = eval_node(idx, ctx) {
        if let Some(f) = n.as_f64() {
            let len = list.len() as i64;
            let i = f as i64;
            let resolved = if i > 0 {
                i - 1
            } else if i < 0 {
                len + i
            } else {
                return Ok(FeelVal::Null);
            };
            if resolved < 0 || resolved >= len {
                return Ok(FeelVal::Null);
            }
            return Ok(list[resolved as usize].clone());
        }
    }
    // Otherwise it is a boolean filter evaluated per element.
    let mut out = Vec::new();
    for item in list {
        let mut frame = BTreeMap::new();
        if let FeelVal::Context(entries) = &item {
            for (k, v) in entries {
                frame.insert(k.clone(), v.clone());
            }
        }
        frame.insert("item".to_string(), item.clone());
        ctx.push(frame);
        let keep = eval_node(idx, ctx);
        ctx.pop();
        if keep? == FeelVal::Bool(true) {
            out.push(item);
        }
    }
    Ok(FeelVal::List(out))
}

fn eval_call(callee: &Node, args: &CallArgs, ctx: &mut Ctx) -> Result<FeelVal, FeelError> {
    // Resolve the callee to a function value, recognising bare builtin names that
    // are not shadowed by a variable.
    let func = match callee {
        Node::Var(name) if ctx.lookup(name).is_none() && builtins::is_builtin(name) => {
            FeelVal::Function(Func::Builtin(builtins::canonical(name)))
        }
        _ => eval_node(callee, ctx)?,
    };
    let FeelVal::Function(func) = func else {
        return Err(FeelError(format!(
            "cannot call a {} value",
            func.type_name()
        )));
    };

    let positional = match (&func, args) {
        (Func::Builtin(name), CallArgs::Named(named)) => {
            let params = builtins::params(name).ok_or_else(|| {
                FeelError(format!("function '{name}' does not accept named arguments"))
            })?;
            let mut slots: Vec<FeelVal> = vec![FeelVal::Null; params.len()];
            for (k, node) in named {
                let pos = params.iter().position(|p| *p == k).ok_or_else(|| {
                    FeelError(format!("unknown parameter '{k}' for function '{name}'"))
                })?;
                slots[pos] = eval_node(node, ctx)?;
            }
            slots
        }
        (Func::Lambda { params, .. }, CallArgs::Named(named)) => {
            let mut slots: Vec<FeelVal> = vec![FeelVal::Null; params.len()];
            for (k, node) in named {
                let pos = params
                    .iter()
                    .position(|p| p == k)
                    .ok_or_else(|| FeelError(format!("unknown parameter '{k}'")))?;
                slots[pos] = eval_node(node, ctx)?;
            }
            slots
        }
        (_, CallArgs::Positional(items)) => items
            .iter()
            .map(|n| eval_node(n, ctx))
            .collect::<Result<Vec<_>, _>>()?,
    };

    apply(&func, positional, ctx.base)
}

/// Invokes a function value with already-evaluated positional arguments.
pub fn apply(
    func: &Func,
    args: Vec<FeelVal>,
    base: &HashMap<String, Value>,
) -> Result<FeelVal, FeelError> {
    match func {
        Func::Builtin(name) => builtins::call(name, args, base),
        Func::Lambda {
            params,
            body,
            closure,
        } => {
            let mut ctx = Ctx::new(base);
            ctx.push((**closure).clone());
            let mut frame = BTreeMap::new();
            for (i, p) in params.iter().enumerate() {
                frame.insert(p.clone(), args.get(i).cloned().unwrap_or(FeelVal::Null));
            }
            ctx.push(frame);
            eval_node(body, &mut ctx)
        }
    }
}

// --- Member access ----------------------------------------------------------

fn member(v: &FeelVal, name: &str) -> FeelVal {
    match v {
        FeelVal::Context(entries) => entries.get(name).cloned().unwrap_or(FeelVal::Null),
        // Projection: `list.field` maps the access over each element.
        FeelVal::List(items) => FeelVal::List(items.iter().map(|i| member(i, name)).collect()),
        FeelVal::Date(d) => date_member(d, name),
        FeelVal::DateTime(dt) => datetime_member(dt, name),
        FeelVal::Time(t) => time_member(t, name),
        FeelVal::YmDur(d) => ym_member(d, name),
        FeelVal::DtDur(d) => dt_member(d, name),
        _ => FeelVal::Null,
    }
}

fn date_member(d: &Date, name: &str) -> FeelVal {
    match name {
        "year" => FeelVal::Int(d.year as i64),
        "month" => FeelVal::Int(d.month as i64),
        "day" => FeelVal::Int(d.day as i64),
        "weekday" => FeelVal::Int(d.weekday() as i64),
        _ => FeelVal::Null,
    }
}

fn time_member(t: &Time, name: &str) -> FeelVal {
    match name {
        "hour" => FeelVal::Int(t.hour as i64),
        "minute" => FeelVal::Int(t.minute as i64),
        "second" => FeelVal::Int(t.second as i64),
        "timezone" => t.zone.clone().map(FeelVal::Str).unwrap_or(FeelVal::Null),
        _ => FeelVal::Null,
    }
}

fn datetime_member(dt: &DateTime, name: &str) -> FeelVal {
    match name {
        "year" | "month" | "day" | "weekday" => date_member(&dt.date, name),
        "hour" | "minute" | "second" | "timezone" => time_member(&dt.time, name),
        _ => FeelVal::Null,
    }
}

fn ym_member(d: &YmDuration, name: &str) -> FeelVal {
    match name {
        "years" => FeelVal::Int(d.months / 12),
        "months" => FeelVal::Int(d.months % 12),
        _ => FeelVal::Null,
    }
}

fn dt_member(d: &DtDuration, name: &str) -> FeelVal {
    const NS: i128 = 1_000_000_000;
    let secs = d.nanos / NS;
    match name {
        "days" => FeelVal::Int((secs / 86_400) as i64),
        "hours" => FeelVal::Int(((secs / 3_600) % 24) as i64),
        "minutes" => FeelVal::Int(((secs / 60) % 60) as i64),
        "seconds" => FeelVal::Int((secs % 60) as i64),
        _ => FeelVal::Null,
    }
}

// --- in / instance of -------------------------------------------------------

/// `x in rhs`. A list tests membership (mismatched items simply do not match);
/// a range or a single value against an ill-typed `x` is a suppressed failure
/// (`null`), as in feel-scala (#1392); a `null` on either side of a single-value
/// test is a plain `false`.
fn in_test(x: &FeelVal, rhs: &FeelVal) -> FeelVal {
    match rhs {
        FeelVal::Range(r) => range_contains(r, x).map_or(FeelVal::Null, FeelVal::Bool),
        FeelVal::List(items) => FeelVal::Bool(items.iter().any(|item| match item {
            FeelVal::Range(r) => range_contains(r, x) == Some(true),
            other => feel_eq(x, other),
        })),
        other if matches!(x, FeelVal::Null) || matches!(other, FeelVal::Null) => {
            FeelVal::Bool(feel_eq(x, other))
        }
        other => eq_op(x, other),
    }
}

/// Range membership; `None` when `x` is incomparable with a bound.
fn range_contains(r: &Range, x: &FeelVal) -> Option<bool> {
    let lower_ok = match &r.start {
        None => true,
        Some(s) => match compare(x, s)? {
            Ordering::Greater => true,
            Ordering::Equal => r.start_inclusive,
            Ordering::Less => false,
        },
    };
    let upper_ok = match &r.end {
        None => true,
        Some(e) => match compare(x, e)? {
            Ordering::Less => true,
            Ordering::Equal => r.end_inclusive,
            Ordering::Greater => false,
        },
    };
    Some(lower_ok && upper_ok)
}

fn instance_of(v: &FeelVal, ty: &str) -> bool {
    match ty {
        "number" => v.is_number(),
        "string" => matches!(v, FeelVal::Str(_)),
        "boolean" => matches!(v, FeelVal::Bool(_)),
        "list" => matches!(v, FeelVal::List(_)),
        "context" => matches!(v, FeelVal::Context(_)),
        "date" => matches!(v, FeelVal::Date(_)),
        "time" => matches!(v, FeelVal::Time(_)),
        "date and time" => matches!(v, FeelVal::DateTime(_)),
        "years and months duration" => matches!(v, FeelVal::YmDur(_)),
        "days and time duration" => matches!(v, FeelVal::DtDur(_)),
        "duration" => matches!(v, FeelVal::YmDur(_) | FeelVal::DtDur(_)),
        "function" => matches!(v, FeelVal::Function(_)),
        "null" => matches!(v, FeelVal::Null),
        "any" => true,
        _ => false,
    }
}

// --- Binary operators -------------------------------------------------------

fn eval_binary(op: BinOp, lhs: &Node, rhs: &Node, ctx: &mut Ctx) -> Result<FeelVal, FeelError> {
    // `and`/`or` use three-valued logic and short-circuit where possible.
    match op {
        BinOp::And => {
            let l = eval_node(lhs, ctx)?;
            if l == FeelVal::Bool(false) {
                return Ok(FeelVal::Bool(false));
            }
            let r = eval_node(rhs, ctx)?;
            return Ok(ternary_and(&l, &r));
        }
        BinOp::Or => {
            let l = eval_node(lhs, ctx)?;
            if l == FeelVal::Bool(true) {
                return Ok(FeelVal::Bool(true));
            }
            let r = eval_node(rhs, ctx)?;
            return Ok(ternary_or(&l, &r));
        }
        _ => {}
    }

    let l = eval_node(lhs, ctx)?;
    let r = eval_node(rhs, ctx)?;
    // An operator applied to ill-typed operands is a *suppressed failure* in
    // feel-scala (the FEEL engine Zeebe embeds): the result is `null`, never an
    // evaluation error (#1392). Each operator therefore yields `Option`, and
    // `None` (the type error) is mapped to `null` in this single place.
    let out = match op {
        BinOp::Add => add(&l, &r),
        BinOp::Sub => sub(&l, &r),
        BinOp::Mul => mul(&l, &r),
        BinOp::Div => div(&l, &r),
        BinOp::Exp => match (l.as_f64(), r.as_f64()) {
            (Some(a), Some(b)) => Some(FeelVal::num(a.powf(b))),
            _ => None,
        },
        BinOp::Eq => Some(eq_op(&l, &r)),
        BinOp::Ne => Some(match eq_op(&l, &r) {
            FeelVal::Bool(b) => FeelVal::Bool(!b),
            other => other,
        }),
        BinOp::Lt => cmp_op(&l, &r, |o| o.is_lt()),
        BinOp::Le => cmp_op(&l, &r, |o| o.is_le()),
        BinOp::Gt => cmp_op(&l, &r, |o| o.is_gt()),
        BinOp::Ge => cmp_op(&l, &r, |o| o.is_ge()),
        BinOp::And | BinOp::Or => unreachable!("handled above"),
    };
    Ok(out.unwrap_or(FeelVal::Null))
}

const NANOS_PER_SEC: i128 = 1_000_000_000;

fn add(l: &FeelVal, r: &FeelVal) -> Option<FeelVal> {
    use FeelVal::*;
    if let (Some(a), Some(b)) = (l.as_f64(), r.as_f64()) {
        return Some(FeelVal::num(a + b));
    }
    match (l, r) {
        (Str(a), Str(b)) => Some(Str(format!("{a}{b}"))),
        // date/datetime + durations (commutative)
        (Date(d), YmDur(y)) | (YmDur(y), Date(d)) => Some(FeelVal::Date(d.add_months(y.months))),
        (Date(d), DtDur(dt)) | (DtDur(dt), Date(d)) => {
            let base = super::temporal::DateTime {
                date: *d,
                time: super::temporal::Time::new(0, 0, 0, 0),
            };
            Some(FeelVal::Date(base.add_dt(*dt).date))
        }
        (DateTime(d), YmDur(y)) | (YmDur(y), DateTime(d)) => {
            Some(FeelVal::DateTime(super::temporal::DateTime {
                date: d.date.add_months(y.months),
                time: d.time.clone(),
            }))
        }
        (DateTime(d), DtDur(dt)) | (DtDur(dt), DateTime(d)) => {
            Some(FeelVal::DateTime(d.add_dt(*dt)))
        }
        (Time(t), DtDur(dt)) | (DtDur(dt), Time(t)) => {
            let nanos = t.nano_of_day() + dt.nanos;
            Some(FeelVal::Time(time_from_nanos(nanos, t)))
        }
        (YmDur(a), YmDur(b)) => Some(FeelVal::YmDur(YmDuration::new(a.months + b.months))),
        (DtDur(a), DtDur(b)) => Some(FeelVal::DtDur(DtDuration::from_nanos(a.nanos + b.nanos))),
        _ => None,
    }
}

fn sub(l: &FeelVal, r: &FeelVal) -> Option<FeelVal> {
    use FeelVal::*;
    if let (Some(a), Some(b)) = (l.as_f64(), r.as_f64()) {
        return Some(FeelVal::num(a - b));
    }
    match (l, r) {
        (Date(a), Date(b)) => Some(FeelVal::DtDur(DtDuration::from_seconds(
            (a.epoch_day() - b.epoch_day()) * 86_400,
        ))),
        (DateTime(a), DateTime(b)) => Some(FeelVal::DtDur(DtDuration::from_nanos(
            a.epoch_nanos() - b.epoch_nanos(),
        ))),
        (Time(a), Time(b)) => Some(FeelVal::DtDur(DtDuration::from_nanos(
            a.nano_of_day() - b.nano_of_day(),
        ))),
        (Date(d), YmDur(y)) => Some(FeelVal::Date(d.add_months(-y.months))),
        (Date(d), DtDur(dt)) => {
            let base = super::temporal::DateTime {
                date: *d,
                time: super::temporal::Time::new(0, 0, 0, 0),
            };
            Some(FeelVal::Date(
                base.add_dt(DtDuration::from_nanos(-dt.nanos)).date,
            ))
        }
        (DateTime(d), YmDur(y)) => Some(FeelVal::DateTime(super::temporal::DateTime {
            date: d.date.add_months(-y.months),
            time: d.time.clone(),
        })),
        (DateTime(d), DtDur(dt)) => Some(FeelVal::DateTime(
            d.add_dt(DtDuration::from_nanos(-dt.nanos)),
        )),
        (Time(t), DtDur(dt)) => Some(FeelVal::Time(time_from_nanos(
            t.nano_of_day() - dt.nanos,
            t,
        ))),
        (YmDur(a), YmDur(b)) => Some(FeelVal::YmDur(YmDuration::new(a.months - b.months))),
        (DtDur(a), DtDur(b)) => Some(FeelVal::DtDur(DtDuration::from_nanos(a.nanos - b.nanos))),
        _ => None,
    }
}

fn mul(l: &FeelVal, r: &FeelVal) -> Option<FeelVal> {
    use FeelVal::*;
    if let (Some(a), Some(b)) = (l.as_f64(), r.as_f64()) {
        return Some(FeelVal::num(a * b));
    }
    match (l, r) {
        (YmDur(d), other) | (other, YmDur(d)) => other
            .as_f64()
            .map(|n| FeelVal::YmDur(YmDuration::new((d.months as f64 * n).round() as i64))),
        (DtDur(d), other) | (other, DtDur(d)) => other
            .as_f64()
            .map(|n| FeelVal::DtDur(DtDuration::from_nanos((d.nanos as f64 * n).round() as i128))),
        _ => None,
    }
}

fn div(l: &FeelVal, r: &FeelVal) -> Option<FeelVal> {
    use FeelVal::*;
    if let (Some(a), Some(b)) = (l.as_f64(), r.as_f64()) {
        return if b == 0.0 {
            Some(FeelVal::Null)
        } else {
            Some(FeelVal::num(a / b))
        };
    }
    match (l, r) {
        (YmDur(a), YmDur(b)) => {
            if b.months == 0 {
                Some(FeelVal::Null)
            } else {
                Some(FeelVal::num(a.months as f64 / b.months as f64))
            }
        }
        (DtDur(a), DtDur(b)) => {
            if b.nanos == 0 {
                Some(FeelVal::Null)
            } else {
                Some(FeelVal::num(a.nanos as f64 / b.nanos as f64))
            }
        }
        (YmDur(d), other) => match other.as_f64() {
            Some(x) if x != 0.0 => Some(FeelVal::YmDur(YmDuration::new(
                (d.months as f64 / x).round() as i64,
            ))),
            Some(_) => Some(FeelVal::Null),
            None => None,
        },
        (DtDur(d), other) => match other.as_f64() {
            Some(x) if x != 0.0 => Some(FeelVal::DtDur(DtDuration::from_nanos(
                (d.nanos as f64 / x).round() as i128,
            ))),
            Some(_) => Some(FeelVal::Null),
            None => None,
        },
        _ => None,
    }
}

fn time_from_nanos(nanos: i128, like: &Time) -> Time {
    let day = 86_400 * NANOS_PER_SEC;
    let n = nanos.rem_euclid(day);
    let nano = (n % NANOS_PER_SEC) as u32;
    let secs = n / NANOS_PER_SEC;
    Time {
        hour: ((secs / 3600) % 24) as u32,
        minute: ((secs / 60) % 60) as u32,
        second: (secs % 60) as u32,
        nano,
        offset_seconds: like.offset_seconds,
        zone: like.zone.clone(),
    }
}

// --- Equality & comparison --------------------------------------------------

pub fn feel_eq(l: &FeelVal, r: &FeelVal) -> bool {
    use FeelVal::*;
    match (l, r) {
        (a, b) if a.is_number() && b.is_number() => a.as_f64() == b.as_f64(),
        (List(a), List(b)) => a.len() == b.len() && a.iter().zip(b).all(|(x, y)| feel_eq(x, y)),
        (Context(a), Context(b)) => {
            a.len() == b.len()
                && a.iter()
                    .all(|(k, v)| b.get(k).map(|w| feel_eq(v, w)).unwrap_or(false))
        }
        (Date(_), Date(_))
        | (Time(_), Time(_))
        | (DateTime(_), DateTime(_))
        | (YmDur(_), YmDur(_))
        | (DtDur(_), DtDur(_)) => compare(l, r) == Some(Ordering::Equal),
        _ => l == r,
    }
}

pub fn compare(l: &FeelVal, r: &FeelVal) -> Option<Ordering> {
    use FeelVal::*;
    if let (Some(a), Some(b)) = (l.as_f64(), r.as_f64()) {
        return a.partial_cmp(&b);
    }
    match (l, r) {
        (Str(a), Str(b)) => Some(a.cmp(b)),
        (Date(a), Date(b)) => Some(a.cmp(b)),
        (Time(a), Time(b)) => Some(a.cmp(b)),
        (DateTime(a), DateTime(b)) => Some(a.cmp(b)),
        (YmDur(a), YmDur(b)) => Some(a.cmp(b)),
        (DtDur(a), DtDur(b)) => Some(a.cmp(b)),
        _ => None,
    }
}

fn cmp_op(l: &FeelVal, r: &FeelVal, f: impl Fn(Ordering) -> bool) -> Option<FeelVal> {
    compare(l, r).map(|o| FeelVal::Bool(f(o)))
}

/// The `=` operator. `null` against anything is a plain boolean; two non-null
/// values of different types are a suppressed failure (`null`), as in
/// feel-scala (#1394). Nested list / context items keep [`feel_eq`]'s
/// structural `false` on a mismatch, which feel-scala also does.
fn eq_op(l: &FeelVal, r: &FeelVal) -> FeelVal {
    let either_null = matches!(l, FeelVal::Null) || matches!(r, FeelVal::Null);
    if either_null || l.type_name() == r.type_name() {
        FeelVal::Bool(feel_eq(l, r))
    } else {
        FeelVal::Null
    }
}

fn ternary_and(l: &FeelVal, r: &FeelVal) -> FeelVal {
    match (l.as_bool(), r.as_bool()) {
        (Some(false), _) | (_, Some(false)) => FeelVal::Bool(false),
        (Some(true), Some(true)) => FeelVal::Bool(true),
        _ => FeelVal::Null,
    }
}

fn ternary_or(l: &FeelVal, r: &FeelVal) -> FeelVal {
    match (l.as_bool(), r.as_bool()) {
        (Some(true), _) | (_, Some(true)) => FeelVal::Bool(true),
        (Some(false), Some(false)) => FeelVal::Bool(false),
        _ => FeelVal::Null,
    }
}
