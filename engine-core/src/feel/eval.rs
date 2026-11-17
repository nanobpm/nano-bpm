//! The FEEL evaluator: walks an [`ast::Node`] tree producing a [`FeelVal`].

use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap, HashSet};
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
    /// The trusted secret names surfaced so far, when provenance is tracked
    /// (`None` = not tracked, the zero-cost default for every caller but the
    /// expression-evaluation endpoint). Shared (`Rc<RefCell>`) so a function
    /// call or builtin callback evaluating in a fresh `Ctx` still surfaces its
    /// reads into the same tracker. See [`SecretProvenance`].
    provenance: Option<Rc<RefCell<SecretProvenance>>>,
}

/// Evaluation-time secret-provenance tracker (#1296). Unlike a syntactic scan
/// of the expression text, this records a `camunda.secrets.<name>` reference
/// only when evaluation actually *surfaces* it — so a shadowed binding, a
/// skipped `if`/`and`/`or` branch, or an un-called function body never
/// contributes, and a reference nested inside a structured (object/list)
/// trusted value is found when a member/index access surfaces it.
///
/// "Trusted" is decided by the *binding*, not the syntax: a base-context
/// variable the host marked trusted (a `SECRET_REFERENCE` cluster variable)
/// contributes the references its value carries; a literal
/// `camunda.secrets.<name>` path or a local/body binding does not (the literal
/// path is recorded separately, as a *literal* reference).
#[derive(Default)]
pub struct SecretProvenance {
    /// Base-context variable names the host trusts to carry secret references
    /// (the `SECRET_REFERENCE`-kind cluster variables).
    trusted_roots: std::collections::HashSet<String>,
    /// Secret names surfaced by reading a trusted root's value.
    pub referenced: std::collections::HashSet<String>,
    /// Secret names surfaced by a literal `camunda.secrets.<name>` path.
    pub literal: std::collections::HashSet<String>,
}

impl SecretProvenance {
    /// Marks `name` (a base-context variable) as a trusted carrier of secret
    /// references. Only base-context roots can be trusted; a local or
    /// request-body binding of the same name shadows it and is NOT trusted.
    pub fn trust_root(&mut self, name: impl Into<String>) {
        self.trusted_roots.insert(name.into());
    }

    /// Records a literal `camunda.secrets.<name>` path surfacing.
    fn record_literal(&mut self, name: &str) {
        self.literal.insert(name.to_string());
    }

    /// Surfaces the trusted secret names a *selected* value carries into
    /// `referenced`. Called only when a [`FeelVal::SecretRef`] is the value a
    /// member/index/filter access actually selects — never for the unselected
    /// siblings of a structured trusted value (#1296).
    fn surface(&mut self, names: &HashSet<String>) {
        self.referenced.extend(names.iter().cloned());
    }
}

/// Computes the `camunda.secrets.<name>` reference names reachable in `v`
/// (without recording them). Used to taint a trusted root's value at the point
/// it is read, so the provenance travels with the individual value.
fn secret_names_in(v: &FeelVal) -> HashSet<String> {
    let mut out = HashSet::new();
    collect_secret_names(v, &mut out);
    out
}

fn collect_secret_names(v: &FeelVal, out: &mut HashSet<String>) {
    match v {
        FeelVal::Str(s) => {
            if let Some(name) = super::secret_reference_name(s) {
                out.insert(name.to_string());
            }
        }
        FeelVal::List(items) => items.iter().for_each(|i| collect_secret_names(i, out)),
        FeelVal::Context(entries) => entries.values().for_each(|i| collect_secret_names(i, out)),
        FeelVal::SecretRef { value, names } => {
            out.extend(names.iter().cloned());
            collect_secret_names(value, out);
        }
        _ => {}
    }
}

/// Records the trusted secret names `v` carries into the shared tracker, then
/// returns `v` unchanged. A no-op for any untainted value (#1296).
fn surface_selection(v: FeelVal, ctx: &Ctx) -> FeelVal {
    ctx.surface(&v);
    v
}

fn surface_names(v: &FeelVal, p: &mut SecretProvenance) {
    match v {
        FeelVal::SecretRef { names, .. } => p.surface(names),
        FeelVal::List(items) => items.iter().for_each(|i| surface_names(i, p)),
        FeelVal::Context(entries) => entries.values().for_each(|i| surface_names(i, p)),
        _ => {}
    }
}

impl<'a> Ctx<'a> {
    pub fn new(base: &'a HashMap<String, Value>) -> Ctx<'a> {
        Ctx {
            base,
            scopes: Vec::new(),
            provenance: None,
        }
    }

    /// Attaches a provenance tracker, returning the context so the caller can
    /// `let mut ctx = Ctx::new(base).with_provenance(prov);`.
    pub fn with_provenance(mut self, provenance: SecretProvenance) -> Ctx<'a> {
        self.provenance = Some(Rc::new(RefCell::new(provenance)));
        self
    }

    /// Consumes the context, returning the tracker (after evaluation).
    pub fn into_provenance(self) -> Option<SecretProvenance> {
        self.provenance.and_then(|rc| {
            // The tracker is shared with any in-flight function-call contexts;
            // by the time the caller consumes the top-level context, evaluation
            // has finished, so this is the sole owner.
            Rc::try_unwrap(rc).ok().map(RefCell::into_inner)
        })
    }

    /// Surfaces the trusted provenance of the expression's *final* result: when
    /// the whole expression evaluates to a tainted value (a bare trusted
    /// cluster variable), that value is what the caller substitutes, so its
    /// secret names are reported (#1296).
    pub fn surface_result(&self, v: &FeelVal) {
        self.surface(v);
    }

    /// Surfaces the trusted secret names `v` carries into the shared tracker
    /// (recursing into a tainted list/context). A no-op when provenance is not
    /// tracked or `v` is untainted (#1296).
    fn surface(&self, v: &FeelVal) {
        if let Some(p) = self.provenance.as_ref() {
            surface_names(v, &mut p.borrow_mut());
        }
    }

    /// `true` when `name` resolves to a base-context variable (not shadowed by
    /// any local scope) — the only bindings that can be trusted carriers.
    fn resolves_to_base(&self, name: &str) -> bool {
        !self
            .scopes
            .iter()
            .rev()
            .any(|frame| frame.contains_key(name))
            && self.base.contains_key(name)
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

/// Evaluates a parsed expression against an existing [`Ctx`] (used by
/// [`super::eval_with_provenance`] to attach a provenance tracker).
pub fn eval_node_pub(node: &Node, ctx: &mut Ctx) -> Result<FeelVal, FeelError> {
    eval_node(node, ctx)
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
        Node::Var(name) => {
            // #1296: a base-context variable the host marked trusted (a
            // `SECRET_REFERENCE` cluster variable) carries the secret
            // references its value holds — but ONLY when it actually resolves
            // to the base context (not shadowed by a local/body binding) and is
            // genuinely read here. Rather than recording every reachable
            // reference at the root (which would report unselected siblings),
            // the value is *tainted*: wrapped with the names it carries, so a
            // later member/index/filter selection surfaces only what it
            // actually selects. This is evaluation-driven, so a skipped branch
            // or a shadowed name never surfaces a reference.
            let is_trusted_root = ctx
                .provenance
                .as_ref()
                .is_some_and(|p| p.borrow().trusted_roots.contains(name.as_str()))
                && ctx.resolves_to_base(name);
            if is_trusted_root {
                let v = ctx.lookup(name).unwrap_or(FeelVal::Null);
                let names = secret_names_in(&v);
                return Ok(FeelVal::SecretRef {
                    value: Box::new(v),
                    names: Rc::new(names),
                });
            }
            Ok(ctx.lookup(name).unwrap_or_else(|| {
                if name == "camunda" {
                    // #1296 / Camunda 8.10: a `camunda.secrets.<name>` reference
                    // is a TRUSTED secret pointer. It is never resolved during
                    // FEEL evaluation (upstream resolves it at job activation);
                    // instead the path evaluates to its own reference text,
                    // mirroring Zeebe's `SecretReferenceEvaluationContext`, so
                    // the reference string — never a secret value — is what
                    // travels onward. The `secrets` member is a sentinel builtin
                    // marker that `member` maps to the reference text.
                    FeelVal::Context(BTreeMap::from([(
                        "secrets".to_string(),
                        FeelVal::Function(Func::Builtin("camunda.secrets")),
                    )]))
                } else if builtins::is_builtin(name) {
                    FeelVal::Function(Func::Builtin(builtins::canonical(name)))
                } else {
                    FeelVal::Null
                }
            }))
        }
        Node::Member(obj, name) => {
            let v = eval_node(obj, ctx)?;
            // #1296: a literal `camunda.secrets.<name>` path surfaces here, when
            // the sentinel marker's member is taken. Record it as a literal
            // (trusted) reference — but only on the actual evaluation path, so
            // a skipped branch or shadowed `camunda` binding never contributes.
            // `member` also records the sentinel within a recursive list
            // projection (`[camunda.secrets].NAME`), so scalar and list access
            // paths report the same trusted reference.
            if matches!(v, FeelVal::Function(Func::Builtin("camunda.secrets"))) {
                if let Some(p) = ctx.provenance.as_ref() {
                    p.borrow_mut().record_literal(name);
                }
            }
            let selected = member(&v, name, ctx);
            // A member access on a trusted value surfaces only that member's
            // subtree (path-scoped): `cfg.token` surfaces `cfg.token`'s names,
            // never the unselected siblings such as `cfg.safe`'s (#1296).
            Ok(surface_selection(selected, ctx))
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
    //
    // #1296: this outer-scope probe only *selects* an element — it must not
    // surface provenance for the elements it merely passes over, and a
    // non-numeric result (a boolean filter) discards the probe entirely before
    // the per-item re-evaluation below. Only the element a numeric index
    // actually selects surfaces its trusted provenance.
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
            let selected = list[resolved as usize].clone();
            return Ok(surface_selection(selected, ctx));
        }
    }
    // Otherwise it is a boolean filter evaluated per element. The probe above
    // already ran `idx` once in the outer scope and was discarded (it returned
    // no number); the per-item evaluation below is the authoritative read, with
    // the item's own bindings in scope — so a body-provided filter value, not a
    // shadowed trusted root, drives the selection.
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

    apply(&func, positional, ctx)
}

/// Invokes a function value with already-evaluated positional arguments.
///
/// The caller's `ctx` threads the shared secret-provenance tracker into the
/// function body (and any builtin callback), so a reference surfaced by an
/// *executed* function body is reported — while an un-called function body,
/// which never reaches `apply`, still contributes nothing (#1296).
pub fn apply(func: &Func, args: Vec<FeelVal>, ctx: &mut Ctx) -> Result<FeelVal, FeelError> {
    // A function argument crosses a boundary: its provenance was surfaced when
    // the value was *selected*, so strip the (transparent-to-operators) wrapper
    // here so a builtin pattern-matching on the concrete variant (e.g.
    // `want_list`) sees the underlying value, not a `SecretRef` (#1296).
    let args: Vec<FeelVal> = args.into_iter().map(FeelVal::strip_provenance).collect();
    match func {
        Func::Builtin(name) => builtins::call(name, args, ctx),
        Func::Lambda {
            params,
            body,
            closure,
        } => {
            let mut body_ctx = Ctx::new(ctx.base);
            body_ctx.provenance = ctx.provenance.clone();
            body_ctx.push((**closure).clone());
            let mut frame = BTreeMap::new();
            for (i, p) in params.iter().enumerate() {
                frame.insert(p.clone(), args.get(i).cloned().unwrap_or(FeelVal::Null));
            }
            body_ctx.push(frame);
            eval_node(body, &mut body_ctx)
        }
    }
}

// --- Member access ----------------------------------------------------------

fn member(v: &FeelVal, name: &str, ctx: &Ctx) -> FeelVal {
    match v {
        FeelVal::Context(entries) => entries.get(name).cloned().unwrap_or(FeelVal::Null),
        // #1296: `camunda.secrets.<name>` — the sentinel builtin marker the
        // `camunda` variable's `secrets` member carries — evaluates to its own
        // reference text (never resolved here; see the `Node::Var` arm). This
        // arm fires both for a direct access and for each element of a list
        // projection (`[camunda.secrets].NAME`), so the literal reference is
        // recorded here — the recursive-projection path the `Node::Member` arm
        // cannot see.
        FeelVal::Function(Func::Builtin("camunda.secrets")) => {
            if let Some(p) = ctx.provenance.as_ref() {
                p.borrow_mut().record_literal(name);
            }
            FeelVal::Str(format!("camunda.secrets.{name}"))
        }
        // Projection: `list.field` maps the access over each element.
        FeelVal::List(items) => FeelVal::List(items.iter().map(|i| member(i, name, ctx)).collect()),
        // A tainted value narrows the access to the member's own subtree, so a
        // selection surfaces only that member's names — never the unselected
        // siblings (#1296).
        FeelVal::SecretRef { value, names } => {
            let selected = member(value, name, ctx);
            super::value::taint_with(selected, names)
        }
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
    // #1296: a trusted value *consumed* by an operator is a genuine read of the
    // reference (e.g. a comparator `k = "x"` in a `sort` callback), so its
    // provenance surfaces here — the operator's boolean/number result is not
    // itself a reference, but the trusted value was used to produce it.
    ctx.surface(&l);
    ctx.surface(&r);
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
