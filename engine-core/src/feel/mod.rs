//! A FEEL expression evaluator targeting parity with Zeebe's feel-scala engine.
//!
//! FEEL (Friendly Enough Expression Language) is an OMG DMN standard, so this
//! module keeps the standard name rather than renaming it. It is signed for
//! **Philipp Ossler** (`saig0`), creator and architect of Camunda's `feel-scala`
//! engine — the reference this implementation targets parity with. Nano's
//! subsystems are named for the engineers whose work inspired them; artists sign
//! their work.
//!
//! Camunda/Zeebe express sequence-flow conditions, service-task job types and
//! message correlation keys as FEEL expressions (marked by a leading `=`). This
//! module evaluates them against a variable context, producing an engine
//! [`Value`]. It is deliberately dependency-free (no regex crate, no `chrono`,
//! no external crates) so `engine-core` keeps compiling for every target,
//! including `wasm32-unknown-unknown`.
//!
//! ## Supported surface
//! * Literals: `null`, booleans, numbers, strings, lists `[…]`, contexts
//!   `{a: 1}`, ranges/intervals `[1..10]`, and temporal `@"…"` literals.
//! * Operators: arithmetic (`+ - * / **`, including temporal arithmetic),
//!   comparison (`= != < <= > >=` across numbers, strings and temporals),
//!   `and`/`or`/`not` (three-valued), `between … and …`, `… in …`,
//!   `… instance of …`.
//! * Expressions: `if … then … else …`, `for … in … return …`,
//!   `some/every … in … satisfies …`, path access `a.b`, list filters/indices
//!   `list[…]`, function definitions `function(x) …` and invocation
//!   (positional or named arguments).
//! * The feel-scala builtin library (string/list/numeric/boolean/context/
//!   temporal/range functions) plus the Camunda extensions, in [`builtins`].
//!
//! Temporal, range and function values render to their canonical FEEL string at
//! the API boundary (see [`value::FeelVal::into_value`]).

mod ast;
mod builtins;
mod error;
mod eval;
mod lexer;
mod parser;
mod regex;
pub(crate) mod temporal;
mod value;

use std::collections::{HashMap, HashSet};

pub use error::FeelError;

use crate::model::Value;

/// Evaluates a FEEL expression against `ctx`, returning the resulting [`Value`].
///
/// A single leading `=` (the Zeebe FEEL marker) is stripped so callers can pass
/// the raw model attribute (`=amount > 10`, `=jobType`) directly.
pub fn eval(expr: &str, ctx: &HashMap<String, Value>) -> Result<Value, FeelError> {
    Ok(evaluate(expr, ctx)?.into_value())
}

/// Evaluates a FEEL expression, returning the [`Value`] **and** the Zeebe
/// `ResultType` name of the *native* FEEL result. [`eval`] renders temporal
/// values to strings at the API boundary, so a caller type-checking the result
/// (e.g. "must be a STRING") must use this kind, not the rendered `Value`'s
/// shape — otherwise `=date("2024-01-15")` would pass as a STRING (#1399).
pub(crate) fn eval_typed(
    expr: &str,
    ctx: &HashMap<String, Value>,
) -> Result<(Value, &'static str, Vec<&'static str>), FeelError> {
    let v = evaluate(expr, ctx)?;
    let kind = feel_kind(&v);
    // Native kinds of a list result's items, captured before conversion:
    // temporal items render to `Value::Str` and would otherwise be
    // indistinguishable from genuine `STRING` items.
    let item_kinds = match &v {
        value::FeelVal::List(items) => items.iter().map(feel_kind).collect(),
        _ => Vec::new(),
    };
    Ok((v.into_value(), kind, item_kinds))
}

/// The Zeebe `ResultType` name of a native FEEL value.
fn feel_kind(v: &value::FeelVal) -> &'static str {
    match v {
        value::FeelVal::Null => "NULL",
        value::FeelVal::Bool(_) => "BOOLEAN",
        value::FeelVal::Int(_) | value::FeelVal::Double(_) => "NUMBER",
        value::FeelVal::Str(_) => "STRING",
        value::FeelVal::List(_) => "ARRAY",
        value::FeelVal::Context(_) => "OBJECT",
        value::FeelVal::Date(_) => "DATE",
        value::FeelVal::Time(_) => "TIME",
        value::FeelVal::DateTime(_) => "DATE_TIME",
        value::FeelVal::YmDur(_) => "PERIOD",
        value::FeelVal::DtDur(_) => "DURATION",
        value::FeelVal::Range(_) | value::FeelVal::Function(_) => "UNKNOWN",
    }
}

/// Evaluates a FEEL expression expecting a string result (job type, correlation
/// key). Strings pass through; numbers/booleans/temporals use their natural FEEL
/// rendering; `null` and structured values are an error.
pub fn eval_string(expr: &str, ctx: &HashMap<String, Value>) -> Result<String, FeelError> {
    let v = evaluate(expr, ctx)?;
    if matches!(v, value::FeelVal::Null) {
        return Err(FeelError(
            "expected a string-like result, got null".to_string(),
        ));
    }
    v.to_feel_string().ok_or_else(|| {
        FeelError(format!(
            "expected a string-like result, got {}",
            v.type_name()
        ))
    })
}

/// Evaluates a FEEL expression expecting a boolean result (a sequence-flow
/// condition). A non-boolean result is an error.
pub fn eval_bool(expr: &str, ctx: &HashMap<String, Value>) -> Result<bool, FeelError> {
    match evaluate(expr, ctx)? {
        value::FeelVal::Bool(b) => Ok(b),
        other => Err(FeelError(format!(
            "expected a boolean result, got {}",
            other.type_name()
        ))),
    }
}

/// The set of top-level variable names a FEEL expression references.
///
/// Used to decide which variable changes should re-evaluate a conditional
/// event's condition, matching Zeebe's expression-based dependency derivation: a
/// member or index access (`x.y`, `x[i]`) depends on the *root* variable `x`, so
/// `x.value > 1` yields `{x}`. Returns an empty set if the expression cannot be
/// parsed — the caller then treats the condition as having no known dependency
/// (and may re-evaluate conservatively).
pub fn referenced_variables(expr: &str) -> std::collections::HashSet<String> {
    let mut out = std::collections::HashSet::new();
    let src = strip_marker(expr);
    if let Ok(tokens) = lexer::tokenize(src) {
        if let Ok(node) = parser::parse(tokens) {
            collect_vars(&node, &mut out);
        }
    }
    out
}

/// The root variable of an expression that is *just* a variable or a nested
/// property of one (`result`, `=a.b.c` → `a`); `None` for anything else
/// (literals, calls, index access, operators) or an unparseable expression.
///
/// Zeebe seeds a multi-instance `outputElement` of this shape as a local null
/// variable of each inner instance, so the child's writes stay in its own scope
/// (#1518).
pub fn variable_reference_root(expr: &str) -> Option<String> {
    fn root(node: &ast::Node) -> Option<&str> {
        match node {
            ast::Node::Var(name) => Some(name),
            ast::Node::Member(base, _) => root(base),
            _ => None,
        }
    }
    let node = parser::parse(lexer::tokenize(strip_marker(expr)).ok()?).ok()?;
    root(&node).map(str::to_owned)
}

/// The prefix of a Camunda secret reference path: `camunda.secrets.<name>`.
pub const SECRET_REFERENCE_PREFIX: &str = "camunda.secrets.";

/// The secret name of a `camunda.secrets.<name>` reference string (the value a
/// `SECRET_REFERENCE` cluster variable carries, or the text a trusted reference
/// evaluates to); `None` when `s` is not exactly that shape.
pub fn secret_reference_name(s: &str) -> Option<&str> {
    let name = s.strip_prefix(SECRET_REFERENCE_PREFIX)?;
    if name.is_empty() {
        None
    } else {
        Some(name)
    }
}

/// The secret names of the literal `camunda.secrets.<name>` paths an expression
/// uses directly (`=camunda.secrets.token` → `{token}`). Per Camunda 8.10
/// `referencedSecrets` semantics a literal reference in the expression is a
/// TRUSTED source; a reference string arriving via a request-body variable or a
/// JSON-kind cluster variable is not (the caller distinguishes those — this
/// helper only sees the expression text). An unparseable expression yields an
/// empty set.
pub fn literal_secret_references(expr: &str) -> std::collections::HashSet<String> {
    let mut out = std::collections::HashSet::new();
    let src = strip_marker(expr);
    if let Ok(tokens) = lexer::tokenize(src) {
        if let Ok(node) = parser::parse(tokens) {
            collect_secret_paths(&node, &mut out);
        }
    }
    out
}

/// Walks a FEEL AST collecting the secret name of every maximal
/// `Var/Member` chain that spells `camunda.secrets.<name>`. The helper returns
/// the chain a `Var`/`Member` subtree spells (e.g. `["camunda", "secrets",
/// "token"]`); the caller records `<name>` only when the chain is EXACTLY
/// `camunda.secrets.<name>` and is not extended further (`camunda.secrets.a.b`
/// is not a reference to `a`). Any non-chain node recurses with a fresh
/// accumulator, so `camunda.secrets.x + 1` still finds `x` while
/// `f(camunda.secrets)` does not (the chain is not a full reference).
fn collect_secret_paths(node: &ast::Node, out: &mut std::collections::HashSet<String>) {
    if let Some(chain) = member_chain(node) {
        if chain.len() == 3 && chain[0] == "camunda" && chain[1] == "secrets" {
            out.insert(chain[2].clone());
        }
        return;
    }
    // A `Member` whose base is not itself a pure variable/member chain (e.g.
    // `({token: camunda.secrets.TOKEN}).token`) still has a base worth walking:
    // the member access does not extend a reference chain, it projects a value
    // that may CARRY one. Recurse into the base so the reference is found. A
    // pure chain was already handled (and excluded) above, so this only adds
    // the non-chain case.
    if let ast::Node::Member(base, _) = node {
        collect_secret_paths(base, out);
        return;
    }
    collect_vars_shallow(node, out);
}

/// The dotted path a pure `Var`/`Member` subtree spells (`a.b.c` →
/// `["a", "b", "c"]`); `None` when the subtree contains anything else.
fn member_chain(node: &ast::Node) -> Option<Vec<String>> {
    match node {
        ast::Node::Var(name) => Some(vec![name.clone()]),
        ast::Node::Member(base, name) => {
            let mut chain = member_chain(base)?;
            chain.push(name.clone());
            Some(chain)
        }
        _ => None,
    }
}

/// Recurses into every child of `node`, for the non-chain nodes of
/// [`collect_secret_paths`].
fn collect_vars_shallow(node: &ast::Node, out: &mut std::collections::HashSet<String>) {
    use ast::Node;
    let mut recur = |n: &ast::Node| collect_secret_paths(n, out);
    match node {
        Node::Null
        | Node::BoolLit(_)
        | Node::NumLit(..)
        | Node::StrLit(_)
        | Node::AtLit(_)
        | Node::Var(_)
        | Node::Member(..) => {}
        Node::Index(a, b) | Node::Bin(_, a, b) | Node::In(a, b) => {
            recur(a);
            recur(b);
        }
        Node::Neg(inner) | Node::Not(inner) | Node::InstanceOf(inner, _) => recur(inner),
        Node::List(items) => items.iter().for_each(&mut recur),
        Node::Context(entries) => entries.iter().for_each(|(_, n)| recur(n)),
        Node::If(a, b, c) | Node::Between(a, b, c) => {
            recur(a);
            recur(b);
            recur(c);
        }
        Node::For(clauses, body) | Node::Quant(_, clauses, body) => {
            clauses.iter().for_each(|(_, n)| recur(n));
            recur(body);
        }
        Node::FuncDef(_, body) => recur(body),
        Node::Call(callee, args) => {
            recur(callee);
            match args {
                ast::CallArgs::Positional(ns) => ns.iter().for_each(&mut recur),
                ast::CallArgs::Named(ns) => ns.iter().for_each(|(_, n)| recur(n)),
            }
        }
        Node::Range(r) => {
            if let Some(s) = &r.start {
                recur(s);
            }
            if let Some(e) = &r.end {
                recur(e);
            }
        }
    }
}

/// Walks a FEEL AST, collecting the name of every [`ast::Node::Var`] it contains.
/// A dotted path `a.b` parses to `Member(Var("a"), "b")`, so recursing into the
/// base of a member/index access naturally yields the root variable. Names bound
/// by `for`/`some`/`every`/`function(...)` are stored as plain strings (not
/// `Var` nodes), so they are not collected as external dependencies.
fn collect_vars(node: &ast::Node, out: &mut std::collections::HashSet<String>) {
    use ast::Node;
    match node {
        Node::Var(name) => {
            out.insert(name.clone());
        }
        Node::Null | Node::BoolLit(_) | Node::NumLit(..) | Node::StrLit(_) | Node::AtLit(_) => {}
        Node::Member(base, _) => collect_vars(base, out),
        Node::Index(base, idx) => {
            collect_vars(base, out);
            collect_vars(idx, out);
        }
        Node::Neg(inner) | Node::Not(inner) => collect_vars(inner, out),
        Node::Bin(_, l, r) => {
            collect_vars(l, out);
            collect_vars(r, out);
        }
        Node::List(items) => items.iter().for_each(|n| collect_vars(n, out)),
        Node::Context(entries) => entries.iter().for_each(|(_, n)| collect_vars(n, out)),
        Node::If(c, t, e) => {
            collect_vars(c, out);
            collect_vars(t, out);
            collect_vars(e, out);
        }
        Node::For(clauses, body) | Node::Quant(_, clauses, body) => {
            clauses.iter().for_each(|(_, n)| collect_vars(n, out));
            collect_vars(body, out);
        }
        Node::Between(a, b, c) => {
            collect_vars(a, out);
            collect_vars(b, out);
            collect_vars(c, out);
        }
        Node::In(a, b) => {
            collect_vars(a, out);
            collect_vars(b, out);
        }
        Node::InstanceOf(inner, _) => collect_vars(inner, out),
        Node::FuncDef(_, body) => collect_vars(body, out),
        Node::Call(callee, args) => {
            collect_vars(callee, out);
            match args {
                ast::CallArgs::Positional(ns) => ns.iter().for_each(|n| collect_vars(n, out)),
                ast::CallArgs::Named(ns) => ns.iter().for_each(|(_, n)| collect_vars(n, out)),
            }
        }
        Node::Range(r) => {
            if let Some(s) = &r.start {
                collect_vars(s, out);
            }
            if let Some(e) = &r.end {
                collect_vars(e, out);
            }
        }
    }
}

/// The set of variable names a FEEL expression references from its ENCLOSING
/// scope — [`referenced_variables`] minus the uses a local binding
/// (`for`/`some`/`every` clause, context entry, or `function(...)` parameter)
/// covers.
///
/// Used where the question is "does this expression READ a name the surrounding
/// scope must supply?" rather than "which names appear in it?": a multi-instance
/// body deciding whether an io-mapping touches a per-child binding must not skip
/// a self-contained mapping like `=for item in [1,2] return item` merely
/// because the `inputElement` is also named `item` — that `item` is the loop
/// variable, not the absent child binding. The binding walk mirrors the
/// evaluator's scoping exactly: a `for`/`some`/`every` clause name binds from
/// the NEXT clause onward (its own source list is evaluated outside it), a
/// context entry key binds from the next entry onward, a `function(...)`
/// parameter binds over the whole body, and a list filter (`base[predicate]`)
/// binds `item` over the predicate — plus, for a filter over literal context
/// elements, each element's entry keys (`=[{price: 1}][price > 0]` binds
/// `price`). A BARE variable index stays a genuine outer reference only when
/// the name is already in scope (`orders[item]` with an outer `item` resolves
/// numerically); out of scope it is a filter and binds `item`
/// (`=[true, false][item]`). Returns an empty set if the expression
/// cannot be parsed — the caller then sees no external reference (the
/// evaluation itself surfaces the parse error).
pub fn free_variables(expr: &str) -> std::collections::HashSet<String> {
    let mut out = std::collections::HashSet::new();
    let src = strip_marker(expr);
    if let Ok(tokens) = lexer::tokenize(src) {
        if let Ok(node) = parser::parse(tokens) {
            let mut bound = std::collections::HashSet::new();
            collect_free_vars(&node, &mut bound, &mut out);
        }
    }
    out
}

/// Walks a FEEL AST collecting the [`ast::Node::Var`] uses not covered by an
/// enclosing local binding. `bound` is the set of names in scope at `node`; a
/// binder extends it for the subexpressions it scopes over (see
/// [`free_variables`]).
fn collect_free_vars(
    node: &ast::Node,
    bound: &mut std::collections::HashSet<String>,
    out: &mut std::collections::HashSet<String>,
) {
    use ast::Node;
    match node {
        Node::Var(name) => {
            if !bound.contains(name) {
                out.insert(name.clone());
            }
        }
        Node::Null | Node::BoolLit(_) | Node::NumLit(..) | Node::StrLit(_) | Node::AtLit(_) => {}
        Node::Member(base, _) => collect_free_vars(base, bound, out),
        Node::Index(base, idx) => {
            collect_free_vars(base, bound, out);
            // `base[idx]` is a numeric index when `idx` evaluates to a number
            // in the CURRENT scope, otherwise a boolean filter evaluated per
            // element (eval_index). eval_node NEVER errors on a Var (an
            // unknown name yields Null, or the builtin function value), so
            // the numeric probe fails exactly when `idx` does not evaluate
            // to a number in the current scope — and the filter frame then
            // binds `item` to the element under test plus, for a context
            // element, every entry key of that element. A static walk cannot
            // know a variable base's elements, so it cannot foresee which
            // entry names a filter would bind: it settles what it can and
            // treats the rest as a filter, mirroring the dependency question
            // this walk answers — with `inputElement="item"` absent at the
            // body level, skipping the mapping is exactly the tolerated case
            // for a genuine outer reference, while a self-contained filter
            // must not skip.
            match idx.as_ref() {
                // A literal number is always a numeric index: no filter, no
                // binding.
                Node::NumLit(..) => {}
                // A BARE variable index resolves numerically whenever the
                // current scope supplies the name, so an IN-SCOPE use
                // (`orders[item]` with an outer `item`) is a genuine outer
                // reference and stays free. An OUT-OF-SCOPE bare variable is
                // never numeric (eval_node yields Null for it), so the
                // subscript is a filter and binds `item` over it —
                // `=[true, false][item]` is self-contained.
                Node::Var(name) => {
                    if !bound.contains(name) {
                        let mut inner = bound.clone();
                        inner.insert("item".to_string());
                        collect_free_vars(idx, &mut inner, out);
                    }
                }
                // Any other index shape is treated as a filter: bind `item`
                // over the predicate, plus the entry keys of literal context
                // elements in the base (`=[{price: 1}][price > 0]` binds
                // `price`). A variable base's elements are unknowable
                // statically, so its entry names cannot be settled here.
                _ => {
                    let mut inner = bound.clone();
                    inner.insert("item".to_string());
                    if let Node::List(items) = base.as_ref() {
                        for item in items {
                            if let Node::Context(entries) = item {
                                inner.extend(entries.iter().map(|(key, _)| key.clone()));
                            }
                        }
                    }
                    collect_free_vars(idx, &mut inner, out);
                }
            }
        }
        Node::Neg(inner) | Node::Not(inner) => collect_free_vars(inner, bound, out),
        Node::Bin(_, l, r) => {
            collect_free_vars(l, bound, out);
            collect_free_vars(r, bound, out);
        }
        Node::List(items) => items.iter().for_each(|n| collect_free_vars(n, bound, out)),
        Node::Context(entries) => {
            // An entry's value is evaluated with the preceding entries in
            // scope, and its key binds from the NEXT entry onward.
            let mut inner = bound.clone();
            for (key, value) in entries {
                collect_free_vars(value, &mut inner, out);
                inner.insert(key.clone());
            }
        }
        Node::If(c, t, e) => {
            collect_free_vars(c, bound, out);
            collect_free_vars(t, bound, out);
            collect_free_vars(e, bound, out);
        }
        Node::For(clauses, body) | Node::Quant(_, clauses, body) => {
            // A clause's source list is evaluated with the PRECEDING clause
            // variables in scope; its own variable binds from the next clause
            // onward and over the body.
            let mut inner = bound.clone();
            for (name, source) in clauses {
                collect_free_vars(source, &mut inner, out);
                inner.insert(name.clone());
            }
            collect_free_vars(body, &mut inner, out);
        }
        Node::Between(a, b, c) => {
            collect_free_vars(a, bound, out);
            collect_free_vars(b, bound, out);
            collect_free_vars(c, bound, out);
        }
        Node::In(a, b) => {
            collect_free_vars(a, bound, out);
            collect_free_vars(b, bound, out);
        }
        Node::InstanceOf(inner, _) => collect_free_vars(inner, bound, out),
        Node::FuncDef(params, body) => {
            let mut inner = bound.clone();
            inner.extend(params.iter().cloned());
            collect_free_vars(body, &mut inner, out);
        }
        Node::Call(callee, args) => {
            collect_free_vars(callee, bound, out);
            match args {
                ast::CallArgs::Positional(ns) => {
                    ns.iter().for_each(|n| collect_free_vars(n, bound, out))
                }
                ast::CallArgs::Named(ns) => ns
                    .iter()
                    .for_each(|(_, n)| collect_free_vars(n, bound, out)),
            }
        }
        Node::Range(r) => {
            if let Some(s) = &r.start {
                collect_free_vars(s, bound, out);
            }
            if let Some(e) = &r.end {
                collect_free_vars(e, bound, out);
            }
        }
    }
}

fn evaluate(expr: &str, ctx: &HashMap<String, Value>) -> Result<value::FeelVal, FeelError> {
    let src = strip_marker(expr);
    let tokens = lexer::tokenize(src)?;
    let node = parser::parse(tokens)?;
    eval::evaluate(&node, ctx)
}

/// The result of [`eval_with_provenance`]: the evaluated [`Value`] plus the
/// trusted `camunda.secrets.<name>` secret names the evaluation surfaced.
pub struct EvaluatedWithProvenance {
    /// The expression's result.
    pub value: Value,
    /// The trusted secret names whose references reach the result (literal
    /// reference paths on the executed path plus references carried by trusted
    /// cluster variables), sorted for a deterministic wire projection.
    pub referenced_secrets: Vec<String>,
}

/// Evaluates a FEEL expression, additionally reporting the trusted
/// `camunda.secrets.<name>` references that *reach the result* (#1296, Camunda
/// 8.10 `referencedSecrets`).
///
/// `trusted_roots` are the base-context variable names the host trusts to carry
/// secret references (the `SECRET_REFERENCE`-kind cluster variables).
///
/// Provenance is **result-based**, per the spec's own contract
/// (spec/expression.yaml): "`referencedSecrets` … Callers use this to know
/// which `camunda.secrets.<name>` occurrences in the result they may safely
/// resolve." A trusted value consumed only by an operator or condition
/// (`k = "x"`, `if flag …`) puts no reference into the result, so there is
/// nothing for the caller to resolve and nothing is reported. The mechanism is
/// **differential**, not an in-band taint wrapper (which broke native dispatch
/// and leaked discarded intermediates — PR #1636 review):
///
/// 1. The expression is evaluated **once, plainly** — the evaluator never
///    wraps a value, so native FEEL dispatch, collection traversal and
///    function calls behave exactly as they do without provenance. The real
///    run records only the literal `camunda.secrets.<name>` paths that surface
///    and the base-context variables actually read (both via a shared sink a
///    shadowed binding never reaches).
/// 2. Only when a trusted root the expression **actually read** carries a
///    `camunda.secrets.` reference does a **second** evaluation run, with that
///    one root's reference strings replaced by an inert marker
///    ([`SECRET_NEUTRALISED`]). A reference in the real result that disappears
///    under that neutralisation — *and whose removal leaves the result
///    otherwise unchanged* — came from that trusted root and is reported.
///
/// Neutralising **one root at a time** is what keeps attribution structural: a
/// reference string that arrives via an *untrusted* source (a request-body or
/// JSON-kind variable) never diffs, even when its text matches a trusted
/// reference — and a secret name that is a prefix of another cannot be
/// mis-attributed, because comparison is on whole strings, never substrings.
///
/// Two refinements keep the diff honest (PR #1636 round-5 review):
///
/// - **Result-reachability for literal paths.** A literal
///   `camunda.secrets.<name>` path is reported only when its text occurs in
///   the result — a discarded function argument, a condition-only literal, or
///   a context member projected away never reaches the result, so it is not
///   an occurrence the caller may resolve. (A literal path evaluates to its
///   own reference text, so result-occurrence is exactly reachability.)
/// - **Branch-aware attribution.** Neutralising a root can flip a branch
///   rather than remove a returned value (`if t = "…" then u else null`):
///   the name then disappears from the neutralised result even though `u`
///   (untrusted) supplied it. Requiring the neutralised result to be
///   otherwise *structurally equal* to the real result refuses that false
///   attribution. And because a name is reported when **any** read trusted
///   root's neutralisation removes it, two roots carrying the same name into
///   the result cannot mask each other — the root whose removal changes the
///   result is attributed, and the name surfaces regardless.
pub fn eval_with_provenance(
    expr: &str,
    ctx: &HashMap<String, Value>,
    trusted_roots: impl IntoIterator<Item = String>,
) -> Result<EvaluatedWithProvenance, FeelError> {
    let src = strip_marker(expr);
    let tokens = lexer::tokenize(src)?;
    let node = parser::parse(tokens)?;

    // The real run: plain evaluation, recording the literal
    // `camunda.secrets.<name>` paths that surface on the executed path and the
    // base-context variables actually read.
    let (value, literal, read) = eval::evaluate_tracked(&node, ctx);
    let value = value?.into_value();

    // The occurrences in the real result a caller may safely resolve.
    let mut real_names = HashSet::new();
    collect_secret_names_value(&value, &mut real_names);

    // Literal reference paths are reported only when they REACH THE RESULT:
    // a literal evaluates to its own reference text, so an executed path whose
    // name is absent from the result was discarded (a function argument, a
    // condition, a projected-away context member) and is not an occurrence the
    // caller may resolve. Reachability is decided *per name* below, once the
    // trusted-root neutralisations show which occurrences a trusted source
    // explains (#1296 round-5 review).
    let literal: HashSet<String> = literal
        .into_iter()
        .filter(|n| real_names.contains(n))
        .collect();
    let mut referenced: HashSet<String> = HashSet::new();

    // The trusted roots that actually carry a reference, split by whether the
    // expression READ them. Only a READ carrier justifies a second
    // (neutralised) evaluation — the common case (no read carrier, or a
    // reference-free result) pays for exactly one evaluation, however many
    // SECRET_REFERENCE variables are visible. An UNREAD carrier is then
    // irrelevant in both directions: the expression never surfaced its value,
    // so it supplies no result occurrence — it can neither be attributed NOR
    // contest a literal that reaches the result (#1296 round-6 review: an
    // unread same-named variable must not suppress a reaching literal).
    let mut carriers: Vec<(String, HashSet<String>)> = Vec::new();
    for name in trusted_roots {
        let Some(value) = ctx.get(&name) else {
            continue;
        };
        let mut names = HashSet::new();
        collect_secret_names_value(value, &mut names);
        if names.is_empty() {
            continue;
        }
        if read.contains(&name) {
            carriers.push((name, names));
        }
    }
    // A read base variable that is NOT a trusted carrier is an untrusted
    // source. A reference such a source carries into the result is an
    // untrusted occurrence, so a same-named literal's own (discarded)
    // contribution must not be reported — the discarded literal would
    // otherwise launder the untrusted copy into a trusted report
    // (`fromAi(u, camunda.secrets.TOKEN)` returning the body variable `u`).
    //
    // The set is deliberately CONSERVATIVE — keyed on the carrier's whole
    // value, not on which occurrence reached the result — so it may name a
    // reference its source only discarded. That over-inclusion is sound here
    // because it is gated behind the trusted-occurrence WITNESS below: a name
    // in this set is excluded only when NO trusted carrier's neutralisation
    // witnessed a trusted occurrence of it, so a literal that genuinely
    // supplies the result (no untrusted occurrence in play) is unaffected,
    // and a contested name falls back to the carrier attribution (#1296
    // round-6 review: trust follows the returned VALUE, never a bare name).
    let mut untrusted_result_names: HashSet<String> = HashSet::new();
    for name in read
        .iter()
        .filter(|n| !carriers.iter().any(|(c, _)| c == *n))
    {
        if let Some(value) = ctx.get(name) {
            let mut names = HashSet::new();
            collect_secret_names_value(value, &mut names);
            untrusted_result_names.extend(names.into_iter().filter(|n| real_names.contains(n)));
        }
    }

    // Neutralise each READ carrier in isolation. Two per-name observations
    // drive attribution below:
    //  * `removed_by_carrier` — the names at least one carrier's
    //    neutralisation removes from the result: a trusted root accounts for
    //    that occurrence.
    let mut removed_by_carrier: HashSet<String> = HashSet::new();
    // The names EACH carrier's neutralisation removes, per carrier — the
    // same-name masking fallback below needs the per-root breakdown.
    let mut removed_per_carrier: Vec<HashSet<String>> = Vec::new();
    if !carriers.is_empty() && !real_names.is_empty() {
        for (root, root_names) in &carriers {
            // Neutralise this one root only, so a same-text reference from an
            // untrusted source cannot diff (structural, per-root attribution).
            let mut neutral_ctx = ctx.clone();
            if let Some(v) = neutral_ctx.get_mut(root) {
                neutralise_secret_references(v);
            }
            let neutralised = eval::evaluate(&node, &neutral_ctx)
                .map(value::FeelVal::into_value)
                .unwrap_or(Value::Null);
            let mut neutral_names = HashSet::new();
            collect_secret_names_value(&neutralised, &mut neutral_names);
            // A name this root carries that reaches the real result but not
            // the neutralised one came from this trusted root — UNLESS
            // neutralising the root changed the result's *shape* (a flipped
            // branch, a different selection), in which case the disappearance
            // is a control-flow artifact, not provenance: the reference was
            // supplied by whatever the other branch selected.
            let shape_changed = !same_shape(&value, &neutralised);
            let mut removed_here: HashSet<String> = HashSet::new();
            if !shape_changed {
                referenced.extend(
                    real_names
                        .iter()
                        .filter(|n| root_names.contains(*n) && !neutral_names.contains(*n))
                        .cloned(),
                );
                removed_here.extend(
                    real_names
                        .iter()
                        .filter(|n| !neutral_names.contains(*n))
                        .cloned(),
                );
                removed_by_carrier.extend(removed_here.iter().cloned());
            }
            removed_per_carrier.push(removed_here);
        }
    }

    // Same-name masking fallback: a name carried into the result by TWO OR
    // MORE read trusted roots is removed by NEITHER root's isolated
    // neutralisation (the other root's copy keeps it in the result), so the
    // per-root loop above reports nothing. Neutralise ALL read carriers at
    // once: if the name then leaves an otherwise-unchanged result, a trusted
    // root accounts for it and it is reported. An untrusted copy cannot
    // false-positive here — neutralising every trusted root still leaves the
    // untrusted occurrence in the result, so the name is not removed.
    let masked: HashSet<String> = carriers
        .iter()
        .zip(&removed_per_carrier)
        .flat_map(|((_, root_names), removed)| {
            root_names
                .iter()
                .filter(|n| real_names.contains(*n) && !removed.contains(*n))
                .cloned()
                .collect::<Vec<_>>()
        })
        .filter(|n| !removed_by_carrier.contains(n))
        .collect();
    if !masked.is_empty() {
        let mut all_neutral_ctx = ctx.clone();
        for (root, _) in &carriers {
            if let Some(v) = all_neutral_ctx.get_mut(root) {
                neutralise_secret_references(v);
            }
        }
        let all_neutralised = eval::evaluate(&node, &all_neutral_ctx)
            .map(value::FeelVal::into_value)
            .unwrap_or(Value::Null);
        if same_shape(&value, &all_neutralised) {
            // Compare occurrence COUNTS, not set membership: an untrusted copy
            // keeps its own occurrences in the neutralised result, so a name
            // is trusted exactly when neutralising every read carrier removes
            // at least one occurrence of it (`[t, u]` with untrusted `u`
            // carrying the same text: 2 real occurrences, 1 neutralised — the
            // trusted `t` occurrence is the one that disappeared).
            let mut real_counts: HashMap<String, usize> = HashMap::new();
            count_secret_names_value(&value, &mut real_counts);
            let mut neutral_counts: HashMap<String, usize> = HashMap::new();
            count_secret_names_value(&all_neutralised, &mut neutral_counts);
            referenced.extend(
                masked
                    .iter()
                    .filter(|n| {
                        real_counts.get(*n).copied().unwrap_or(0)
                            > neutral_counts.get(*n).copied().unwrap_or(0)
                    })
                    .cloned(),
            );
            removed_by_carrier.extend(masked.iter().cloned());
        }
    }

    // A literal path's name is reported only when its occurrence reaches the
    // result as a TRUSTED occurrence — the literal itself must be what
    // supplies it (#1296 round-6 review: trust follows the returned VALUE,
    // never a bare name match). A trusted carrier's neutralisation REMOVING
    // the name is the witness that a trusted occurrence of it is in the
    // result; that occurrence is then the carrier's (or, when the literal is
    // also on the result path, the literal's indistinguishably) and is
    // already reported by the carrier attribution above. Without that
    // witness, the literal is trusted only when its result occurrence is not
    // explained by an UNTRUSTED source: a read untrusted variable carrying
    // the same text into the result (`fromAi(u, camunda.secrets.TOKEN)`), or
    // an extra occurrence beyond the literal's own single contribution (an
    // ordinary local string — `{x: "camunda.secrets.TOKEN", ignored:
    // camunda.secrets.TOKEN}.x`). A literal that supplies the result alone
    // (one occurrence, none carrier-removed, no untrusted source) is
    // unaffected, and an unread or discarded carrier never suppresses it.
    let mut real_counts: HashMap<String, usize> = HashMap::new();
    count_secret_names_value(&value, &mut real_counts);

    referenced.extend(
        literal
            .iter()
            .filter(|n| {
                !removed_by_carrier.contains(*n)
                    && !untrusted_result_names.contains(*n)
                    && real_counts.get(*n).copied().unwrap_or(0) <= 1
            })
            .cloned(),
    );
    let mut referenced_secrets: Vec<String> = referenced.into_iter().collect();
    referenced_secrets.sort();
    referenced_secrets.dedup();
    Ok(EvaluatedWithProvenance {
        value,
        referenced_secrets,
    })
}

/// The inert marker that replaces a trusted root's `camunda.secrets.<name>`
/// strings in [`eval_with_provenance`]'s neutralised run. The invariants that
/// make the diff sound: (a) it is not itself a `camunda.secrets.` reference
/// ([`secret_reference_name`] rejects it), so the neutralised run cannot
/// *introduce* a reportable name; (b) it is substituted *into* trusted values
/// only, so it never equals an input value; and (c) it carries NUL bytes, so
/// no deployed model or cluster variable would legitimately produce it.
/// Comparison is on whole strings after substitution, never substrings, so a
/// name that is a prefix of another cannot be mis-attributed.
const SECRET_NEUTRALISED: &str = "\u{0}nano:secret-neutralised\u{0}";

/// Replaces every exact `camunda.secrets.<name>` string in `v` (recursing into
/// lists and maps) with [`SECRET_NEUTRALISED`]. A string that merely *contains*
/// a reference is left untouched: only a whole-string reference is a secret
/// pointer a caller may resolve.
fn neutralise_secret_references(v: &mut Value) {
    match v {
        Value::Str(s) => {
            if secret_reference_name(s).is_some() {
                *s = SECRET_NEUTRALISED.to_string();
            }
        }
        Value::List(items) => items.iter_mut().for_each(neutralise_secret_references),
        Value::Map(entries) => entries.values_mut().for_each(neutralise_secret_references),
        _ => {}
    }
}

/// Counts the occurrences of every whole-string `camunda.secrets.<name>`
/// reference in `v` (recursing into lists and maps). The same-name masking
/// fallback compares counts, not membership: an untrusted copy keeps its own
/// occurrences under neutralisation, so only the trusted count disappears.
fn count_secret_names_value(v: &Value, out: &mut HashMap<String, usize>) {
    match v {
        Value::Str(s) => {
            if let Some(name) = secret_reference_name(s) {
                *out.entry(name.to_string()).or_insert(0) += 1;
            }
        }
        Value::List(items) => items.iter().for_each(|i| count_secret_names_value(i, out)),
        Value::Map(entries) => entries
            .values()
            .for_each(|i| count_secret_names_value(i, out)),
        _ => {}
    }
}

/// Collects the secret names of every whole-string `camunda.secrets.<name>`
/// reference in `v` (recursing into lists and maps).
fn collect_secret_names_value(v: &Value, out: &mut HashSet<String>) {
    match v {
        Value::Str(s) => {
            if let Some(name) = secret_reference_name(s) {
                out.insert(name.to_string());
            }
        }
        Value::List(items) => items
            .iter()
            .for_each(|i| collect_secret_names_value(i, out)),
        Value::Map(entries) => entries
            .values()
            .for_each(|i| collect_secret_names_value(i, out)),
        _ => {}
    }
}

/// Whether `a` and `b` agree everywhere except on whole-string
/// `camunda.secrets.<name>` reference occurrences and fresh nondeterministic
/// draws: the neutralised run's result may differ from the real result only by
/// a reference string becoming [`SECRET_NEUTRALISED`], or by a `uuid()` /
/// `random number()` / `now()` builtin drawing a fresh value on the
/// re-evaluation. Any other difference — a flipped branch, a different
/// selection, a changed length, a reference becoming `null` — means
/// neutralising the root changed the *control flow*, so a name that
/// disappears is a control-flow artifact, not provenance from that root
/// (#1296 round-5 review: branch-aware attribution).
///
/// The nondeterminism tolerance is keyed on the *shape* of a fresh draw, not
/// the value (#1296 round-6 review): a v4 UUID string, a `[0, 1)` `random
/// number()` double, and a `now()` dateTime each have a recognisable shape a
/// neutralisation marker or an ordinary input never takes, so two differing
/// draws of the same kind are the same shape — while a reference string
/// becoming `null` (a flipped branch) still fails the comparison.
fn same_shape(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Str(x), Value::Str(y)) => {
            x == y
                || (secret_reference_name(x).is_some() && y == SECRET_NEUTRALISED)
                || nondeterministic_pair(a, b)
        }
        (Value::List(xs), Value::List(ys)) => {
            xs.len() == ys.len() && xs.iter().zip(ys).all(|(x, y)| same_shape(x, y))
        }
        (Value::Map(xs), Value::Map(ys)) => {
            xs.len() == ys.len()
                && xs
                    .iter()
                    .all(|(k, x)| ys.get(k).map(|y| same_shape(x, y)).unwrap_or(false))
        }
        _ => a == b || nondeterministic_pair(a, b),
    }
}

/// Whether two differing scalar values are a fresh draw of the SAME
/// nondeterministic builtin: both v4 UUID strings (`uuid()`), both `[0, 1)`
/// doubles (`random number()`), or both ISO-8601 dateTime strings (`now()`,
/// which [`FeelVal::into_value`] renders as a string). Keyed on the draw's
/// shape so the neutralisation marker (a NUL-carrying string) and ordinary
/// inputs never qualify — only a genuine re-draw does.
fn nondeterministic_pair(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Str(x), Value::Str(y)) => {
            (is_uuid_v4(x) && is_uuid_v4(y)) || (is_datetime_string(x) && is_datetime_string(y))
        }
        (Value::Double(x), Value::Double(y)) => (0.0..1.0).contains(x) && (0.0..1.0).contains(y),
        _ => false,
    }
}

/// Whether `s` has the canonical RFC-4122 v4 UUID shape `uuid()` draws
/// (`8-4-4-4-12` hex, version nibble `4`, variant nibble `8`/`9`/`a`/`b`).
fn is_uuid_v4(s: &str) -> bool {
    let b = s.as_bytes();
    if b.len() != 36 || b[8] != b'-' || b[13] != b'-' || b[18] != b'-' || b[23] != b'-' {
        return false;
    }
    let hex = |c: u8| c.is_ascii_hexdigit();
    b[..8].iter().all(|&c| hex(c))
        && b[9..13].iter().all(|&c| hex(c))
        && b[14] == b'4'
        && b[15..18].iter().all(|&c| hex(c))
        && matches!(b[19], b'8' | b'9' | b'a' | b'b' | b'A' | b'B')
        && b[20..23].iter().all(|&c| hex(c))
        && b[24..36].iter().all(|&c| hex(c))
}

/// Whether `s` has the ISO-8601 dateTime shape `now()` renders
/// (`YYYY-MM-DDThh:mm:ss…`): a four-digit year, `-`-separated date, a `T`
/// separator, and a `:`-separated time. Distinct from the v4-UUID shape (no
/// `T`, different hyphen layout), so a `uuid()` draw and a `now()` draw never
/// match each other.
fn is_datetime_string(s: &str) -> bool {
    let b = s.as_bytes();
    if b.len() < 19
        || b[4] != b'-'
        || b[7] != b'-'
        || b[10] != b'T'
        || b[13] != b':'
        || b[16] != b':'
    {
        return false;
    }
    let digit = |c: u8| c.is_ascii_digit();
    b[..4].iter().all(|&c| digit(c))
        && b[5..7].iter().all(|&c| digit(c))
        && b[8..10].iter().all(|&c| digit(c))
        && b[11..13].iter().all(|&c| digit(c))
        && b[14..16].iter().all(|&c| digit(c))
        && b[17..19].iter().all(|&c| digit(c))
}

fn strip_marker(expr: &str) -> &str {
    let trimmed = expr.trim();
    // A single leading `=` marks a FEEL expression; `==` is the equality
    // operator and must be preserved.
    if trimmed.starts_with("==") {
        trimmed
    } else {
        trimmed.strip_prefix('=').unwrap_or(trimmed)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    #[test]
    fn variable_reference_root_accepts_only_bare_variables_and_paths() {
        // #1518: these are the MI outputElement shapes Zeebe seeds locally.
        assert_eq!(
            variable_reference_root("=result").as_deref(),
            Some("result")
        );
        assert_eq!(variable_reference_root("result").as_deref(), Some("result"));
        assert_eq!(variable_reference_root("=a.b.c").as_deref(), Some("a"));
        // Anything computed is not a variable to seed.
        for expr in [
            "=a[1]", "=a + 1", "=f(a)", "={x: a}", "=\"a\"", "=1", "=", "=a +",
        ] {
            assert_eq!(variable_reference_root(expr), None, "{expr}");
        }
    }

    fn ctx(pairs: &[(&str, Value)]) -> HashMap<String, Value> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect()
    }

    // --- ported baseline coverage ------------------------------------------

    #[test]
    fn referenced_variables_derives_dependencies() {
        let vars = |e: &str| {
            let mut v: Vec<String> = referenced_variables(e).into_iter().collect();
            v.sort();
            v
        };
        assert_eq!(vars("=x > 1"), vec!["x"]);
        assert_eq!(vars("=x > 1 and y < 5"), vec!["x", "y"]);
        // A member/index access depends on the root variable only.
        assert_eq!(vars("=x.y.z = true"), vec!["x"]);
        assert_eq!(vars("=orders[1] > amount"), vec!["amount", "orders"]);
        // Literals reference nothing.
        assert!(vars("=1 > 0").is_empty());
    }

    /// #1296: the literal `camunda.secrets.<name>` paths of an expression (the
    /// trusted direct-reference half of Camunda 8.10 `referencedSecrets`).
    #[test]
    fn literal_secret_references_finds_direct_paths() {
        let refs = |e: &str| {
            let mut v: Vec<String> = literal_secret_references(e).into_iter().collect();
            v.sort();
            v
        };
        assert_eq!(refs("=camunda.secrets.token"), vec!["token"]);
        assert_eq!(refs("camunda.secrets.token"), vec!["token"]);
        // Inside a larger expression.
        assert_eq!(
            refs(r#"=camunda.secrets.a + camunda.secrets.b"#),
            vec!["a", "b"]
        );
        // A reference-shaped STRING literal is not a path.
        assert!(refs(r#"="camunda.secrets.token""#).is_empty());
        // An incomplete chain is not a reference.
        assert!(refs("=camunda.secrets").is_empty());
        assert!(refs("=camunda.other.token").is_empty());
        // A chain extended past the name is not a reference.
        assert!(refs("=camunda.secrets.a.b").is_empty());
        // An unparseable expression has no known reference.
        assert!(refs("=for in").is_empty());
        // No false positives on ordinary variables.
        assert!(refs("=secrets.token").is_empty());
        assert!(refs("=x + 1").is_empty());
        // #1296 finding: a member access whose base is NOT a pure chain still
        // walks the base, so a reference carried by a context literal is found.
        assert_eq!(
            refs(r#"=({token: camunda.secrets.TOKEN}).token"#),
            vec!["TOKEN"]
        );
        assert_eq!(
            refs(r#"=({a: camunda.secrets.A, b: camunda.secrets.B}).a"#),
            vec!["A", "B"]
        );
    }

    #[test]
    fn secret_reference_name_parses_the_reference_shape() {
        assert_eq!(
            secret_reference_name("camunda.secrets.TOKEN"),
            Some("TOKEN")
        );
        assert_eq!(secret_reference_name("camunda.secrets."), None);
        assert_eq!(secret_reference_name("camunda.secrets"), None);
        assert_eq!(secret_reference_name("TOKEN"), None);
        assert_eq!(secret_reference_name("x camunda.secrets.TOKEN"), None);
    }

    #[test]
    fn free_variables_excludes_locally_bound_uses() {
        let free = |e: &str| {
            let mut v: Vec<String> = free_variables(e).into_iter().collect();
            v.sort();
            v
        };
        // A plain reference is free.
        assert_eq!(free("=x > 1"), vec!["x"]);
        // A `for`/`some` clause variable binds over its body and the following
        // clauses, but not over its own source list.
        assert!(free("=for item in [1, 2] return item").is_empty());
        assert_eq!(
            free("=for item in orders return item.price"),
            vec!["orders"]
        );
        assert_eq!(
            free("=for a in xs, b in a return b"),
            vec!["xs"],
            "a clause source sees the preceding clause variables, not its own"
        );
        assert!(free("=some item in [1] satisfies item > 0").is_empty());
        // A use OUTSIDE the binding is still free.
        assert_eq!(
            free("=(for item in [1] return item)[1] + item"),
            vec!["item"]
        );
        // A context entry key binds from the next entry onward.
        assert!(free("={item: 1, y: item}.y").is_empty());
        assert_eq!(free("={item: seed}.item"), vec!["seed"]);
        // A function parameter binds over the body.
        assert!(free("=function(item) item + 1").is_empty());
        assert_eq!(free("=function(item) item + k"), vec!["k"]);
        // A list FILTER (`base[predicate]`) binds `item` to the element under
        // test over the predicate (eval_index): a predicate whose only use of
        // the name is that filter-local binding is self-contained.
        assert!(free("=[1, 2][item > 0]").is_empty());
        assert_eq!(free("=orders[item.price > limit]"), vec!["limit", "orders"]);
        // A bare variable index that is NOT in scope is not numeric, so the
        // subscript is a filter at runtime and binds `item` over it
        // (eval_index): `=[true, false][item]` is self-contained.
        assert!(free("=[true, false][item]").is_empty());
        // The same holds for a list base: with `item` absent the subscript is
        // a filter, so `orders[item]` reads only the base (probe-verified:
        // an unbound `item` yields `List([])`, never a numeric index).
        assert_eq!(free("=orders[item]"), vec!["orders"]);
        // A filter over context elements also binds each element's entries,
        // so `=[{price: 1}][price > 0]` reads nothing from the outer scope.
        assert!(free("=[{price: 1}][price > 0]").is_empty());
        // A filter over a VARIABLE base cannot settle the elements' entry
        // names statically (their values are unknowable), so a name colliding
        // with an entry stays free: with `orders = [{price: …}]` the runtime
        // binds `price`, but the walk must keep it free — the base alone is
        // not proof the binding exists.
        assert_eq!(free("=orders[price > 0]"), vec!["orders", "price"]);
        // An unparsable expression has no known free reference.
        assert!(free("=for in").is_empty());
    }

    #[test]
    fn evaluates_literals_and_arithmetic() {
        let c = ctx(&[]);
        assert_eq!(eval("1 + 2 * 3", &c), Ok(Value::Int(7)));
        assert_eq!(eval("(1 + 2) * 3", &c), Ok(Value::Int(9)));
        assert_eq!(eval("7 / 2", &c), Ok(Value::Double(3.5)));
        assert_eq!(eval("10 / 0", &c), Ok(Value::Null));
        assert_eq!(eval("-5 + 8", &c), Ok(Value::Int(3)));
        assert_eq!(eval("2 ** 10", &c), Ok(Value::Int(1024)));
    }

    #[test]
    fn resolves_variables_and_strips_marker() {
        let c = ctx(&[("amount", Value::Int(42))]);
        assert_eq!(eval("=amount", &c), Ok(Value::Int(42)));
        assert_eq!(eval("amount + 8", &c), Ok(Value::Int(50)));
        assert_eq!(eval("missing", &c), Ok(Value::Null));
    }

    /// #1296: provenance is EVALUATION-DRIVEN and RESULT-BASED (differential):
    /// a trusted reference is reported only when it *reaches the result* — the
    /// occurrences in the result a caller may safely resolve
    /// (spec/expression.yaml `referencedSecrets`). A trusted value consumed only
    /// by an operator or condition puts no reference into the result, so it is
    /// not reported; a shadowed binding, a skipped `if`/`and`/`or` branch, or
    /// an un-called function body never contributes; and a reference nested in
    /// a structured (object/list) trusted value is found when a member access
    /// surfaces it into the result.
    #[test]
    fn eval_with_provenance_tracks_only_surfaced_trusted_references() {
        let prov = |e: &str, c: &HashMap<String, Value>, trusted: &[&str]| {
            eval_with_provenance(
                e,
                c,
                trusted.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
            )
            .unwrap()
            .referenced_secrets
        };

        // A literal reference path on the executed path is trusted.
        assert_eq!(
            prov("camunda.secrets.DIRECT", &ctx(&[]), &[]),
            vec!["DIRECT"]
        );
        // A trusted (SECRET_REFERENCE) cluster variable read surfaces its ref.
        let c = ctx(&[("token_ref", Value::Str("camunda.secrets.TOKEN".into()))]);
        assert_eq!(prov("token_ref", &c, &["token_ref"]), vec!["TOKEN"]);
        // An UNtrusted (JSON-kind / body) variable's reference text is not.
        assert!(prov("token_ref", &c, &[]).is_empty());

        // A skipped `if` branch never surfaces its reference.
        let c = ctx(&[("flag", Value::Bool(false))]);
        assert_eq!(
            prov(
                "if flag then camunda.secrets.SKIPPED else camunda.secrets.TAKEN",
                &c,
                &[]
            ),
            vec!["TAKEN"]
        );
        // Short-circuiting `or`: the right side never evaluates.
        assert_eq!(
            prov("true or camunda.secrets.SKIPPED", &ctx(&[]), &[]),
            Vec::<String>::new()
        );

        // A body/local binding shadows a same-named trusted cluster variable:
        // the trusted root is NOT read, so its reference is not surfaced.
        let c = ctx(&[("token_ref", Value::Str("camunda.secrets.TOKEN".into()))]);
        // Shadowed by a context-literal binding of the same name.
        assert!(
            prov("{token_ref: 1}.token_ref", &c, &["token_ref"]).is_empty(),
            "a local binding shadows the trusted root"
        );
        // A `for` loop variable shadowing the trusted root is not trusted.
        assert!(
            prov("for token_ref in [1] return token_ref", &c, &["token_ref"]).is_empty(),
            "a for-loop binding shadows the trusted root"
        );

        // A reference nested in a STRUCTURED trusted value is surfaced by a
        // member access (finding: structured values carry provenance).
        let c = ctx(&[(
            "cfg",
            Value::Map(
                [(
                    "token".to_string(),
                    Value::Str("camunda.secrets.NESTED".into()),
                )]
                .into_iter()
                .collect(),
            ),
        )]);
        assert_eq!(prov("cfg.token", &c, &["cfg"]), vec!["NESTED"]);
        // An un-called function body never surfaces its reference.
        assert!(prov("function() camunda.secrets.SKIPPED", &ctx(&[]), &[]).is_empty());
    }

    /// #1296 review (round 4): provenance is RESULT-BASED — a secret is
    /// reported exactly when its reference reaches the expression's result
    /// (spec/expression.yaml: callers resolve "which `camunda.secrets.<name>`
    /// occurrences in the result they may safely resolve"). This pins the
    /// defect class the in-band taint wrapper kept leaking: native dispatch
    /// must be untouched, discarded intermediates must not count, and
    /// provenance must survive function arguments and returns.
    #[test]
    fn eval_with_provenance_is_result_based() {
        let prov = |e: &str, c: &HashMap<String, Value>, trusted: &[&str]| {
            eval_with_provenance(
                e,
                c,
                trusted.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
            )
            .unwrap()
            .referenced_secrets
        };

        // --- Native dispatch is untouched -------------------------------
        // A trusted boolean condition behaves like the plain value (no wrapper
        // is ever introduced, so `if flag then 1 else 2` sees `true`).
        let c = ctx(&[("flag", Value::Bool(true))]);
        let r =
            eval_with_provenance("if flag then 1 else 2", &c, vec!["flag".to_string()]).unwrap();
        assert_eq!(r.value, Value::Int(1));
        assert!(r.referenced_secrets.is_empty());
        // A trusted list indexes like a plain list.
        let c = ctx(&[(
            "arr",
            Value::List(vec![
                Value::Str("camunda.secrets.FIRST".into()),
                Value::Str("plain".into()),
            ]),
        )]);
        let r = eval_with_provenance("arr[1]", &c, vec!["arr".to_string()]).unwrap();
        assert_eq!(r.value, Value::Str("camunda.secrets.FIRST".into()));
        assert_eq!(r.referenced_secrets, vec!["FIRST"]);
        let r = eval_with_provenance("arr[2]", &c, vec!["arr".to_string()]).unwrap();
        assert_eq!(r.value, Value::Str("plain".into()));
        assert!(
            r.referenced_secrets.is_empty(),
            "selecting a non-reference element reports nothing"
        );
        // String equality and concatenation see the plain string.
        let c = ctx(&[("k", Value::Str("camunda.secrets.KEY".into()))]);
        assert_eq!(
            eval_with_provenance(r#"k = "camunda.secrets.KEY""#, &c, vec!["k".to_string()])
                .unwrap()
                .value,
            Value::Bool(true)
        );
        assert_eq!(
            eval_with_provenance(r#"k + "!""#, &c, vec!["k".to_string()])
                .unwrap()
                .value,
            Value::Str("camunda.secrets.KEY!".into())
        );

        // --- Only references that REACH THE RESULT are reported ---------
        // A trusted value consumed only by an operator or condition puts no
        // reference into the result, so there is nothing for the caller to
        // resolve and nothing to report (the spec's own contract).
        let c = ctx(&[("k", Value::Str("camunda.secrets.KEY".into()))]);
        assert!(
            prov(r#"k = "x""#, &c, &["k"]).is_empty(),
            "a condition-only trusted read is not reported"
        );
        assert!(
            prov("sort([2, 1], function(a, b) k = \"x\")", &c, &["k"]).is_empty(),
            "a trusted value consumed by a comparator does not reach the result"
        );
        assert!(
            prov(
                "count(arr)",
                &ctx(&[(
                    "arr",
                    Value::List(vec![Value::Str("camunda.secrets.S".into())]),
                )]),
                &["arr"]
            )
            .is_empty(),
            "a trusted list's length carries no reference"
        );

        // An unselected trusted sibling of a structured value is not reported:
        // `[cfg.safe, body_ref]` selects a number and an untrusted body string.
        let c = ctx(&[
            (
                "cfg",
                Value::Map(
                    [
                        ("safe".to_string(), Value::Int(1)),
                        (
                            "token".to_string(),
                            Value::Str("camunda.secrets.TOKEN".into()),
                        ),
                    ]
                    .into_iter()
                    .collect(),
                ),
            ),
            ("body_ref", Value::Str("camunda.secrets.TOKEN".into())),
        ]);
        assert!(
            prov("[cfg.safe, body_ref]", &c, &["cfg"]).is_empty(),
            "an unselected trusted sibling must not be reported"
        );
        assert_eq!(prov("cfg.token", &c, &["cfg"]), vec!["TOKEN"]);

        // A discarded index *probe* / filter intermediate is not reported:
        // the filter re-evaluates against each item's own (body-provided)
        // bindings, so the trusted root never reaches the result.
        let c = ctx(&[
            ("flag", Value::Str("camunda.secrets.TOKEN".into())),
            (
                "rows",
                Value::List(vec![Value::Map(
                    [
                        ("flag".to_string(), Value::Bool(true)),
                        (
                            "value".to_string(),
                            Value::Str("camunda.secrets.TOKEN".into()),
                        ),
                    ]
                    .into_iter()
                    .collect(),
                )]),
            ),
        ]);
        assert!(
            prov("rows[flag].value", &c, &["flag"]).is_empty(),
            "a discarded index probe must not surface the trusted root"
        );

        // --- Provenance survives function arguments and returns ---------
        let c = ctx(&[("token_ref", Value::Str("camunda.secrets.TOKEN".into()))]);
        assert_eq!(
            prov("(function(x) x)(token_ref)", &c, &["token_ref"]),
            vec!["TOKEN"],
            "an identity lambda returns the trusted reference"
        );
        assert_eq!(
            prov("fromAi(token_ref)", &c, &["token_ref"]),
            vec!["TOKEN"],
            "fromAi returns its value argument verbatim"
        );
        let c = ctx(&[(
            "cfg",
            Value::Map(
                [(
                    "token".to_string(),
                    Value::Str("camunda.secrets.TOKEN".into()),
                )]
                .into_iter()
                .collect(),
            ),
        )]);
        assert_eq!(
            prov(r#"get value(cfg, "token")"#, &c, &["cfg"]),
            vec!["TOKEN"],
            "a selector builtin surfaces the selected reference"
        );
        let c = ctx(&[(
            "arr",
            Value::List(vec![Value::Str("camunda.secrets.TOKEN".into())]),
        )]);
        assert_eq!(
            prov("reverse(arr)", &c, &["arr"]),
            vec!["TOKEN"],
            "a collection builtin keeps the reference it returns"
        );

        // --- Structural, per-root attribution ---------------------------
        // An UNTRUSTED source whose text matches a trusted reference is never
        // reported: the trusted root is neutralised in isolation, so a copy
        // carried by a body variable cannot diff.
        let c = ctx(&[
            ("t", Value::Str("camunda.secrets.TOKEN".into())),
            ("u", Value::Str("camunda.secrets.TOKEN".into())),
        ]);
        assert!(
            prov("u", &c, &["t"]).is_empty(),
            "an untrusted copy of a trusted reference text is not reported"
        );
        assert!(
            prov("[t, u][2]", &c, &["t"]).is_empty(),
            "selecting the untrusted element reports nothing"
        );
        assert_eq!(
            prov("[t, u][1]", &c, &["t"]),
            vec!["TOKEN"],
            "selecting the trusted element reports it"
        );
        // A secret name that is a prefix of another cannot be mis-attributed:
        // the comparison is on whole strings, never substrings.
        let c = ctx(&[
            ("a", Value::Str("camunda.secrets.a".into())),
            ("ab", Value::Str("camunda.secrets.ab".into())),
        ]);
        assert_eq!(prov("a", &c, &["a"]), vec!["a"]);
        assert!(prov("a", &c, &["ab"]).is_empty());
        assert_eq!(prov("[a, ab]", &c, &["a", "ab"]), vec!["a", "ab"]);

        // The neutralisation marker is inert: it is not itself a reference,
        // so the neutralised run can never *introduce* a reportable name.
        assert_eq!(secret_reference_name(SECRET_NEUTRALISED), None);

        // A trusted value transformed past recognition (concatenation) is no
        // longer a resolvable occurrence, so it is not reported.
        let c = ctx(&[("k", Value::Str("camunda.secrets.KEY".into()))]);
        assert!(
            prov(r#"k + "!""#, &c, &["k"]).is_empty(),
            "a transformed reference is not an occurrence the caller can resolve"
        );

        // A literal reference reached through a LIST projection is recorded
        // just like the scalar path.
        assert_eq!(
            prov("[camunda.secrets].TOKEN", &ctx(&[]), &[]),
            vec!["TOKEN"]
        );
        // An EXECUTED function body surfaces its literal reference — while an
        // un-called body still does not.
        assert_eq!(
            prov("(function() camunda.secrets.TOKEN)()", &ctx(&[]), &[]),
            vec!["TOKEN"]
        );
        assert!(prov("function() camunda.secrets.SKIPPED", &ctx(&[]), &[]).is_empty());
    }

    /// #1296 review (round 5): provenance must be **result-reachable** and
    /// **branch-aware**. Three defect classes the first differential cut got
    /// wrong, pinned red-first:
    ///
    /// 1. A literal `camunda.secrets.<name>` path that is *evaluated* but whose
    ///    value never reaches the result (a discarded function argument, a
    ///    condition-only literal, a discarded context member) is not an
    ///    occurrence "in the result" the caller may resolve — it must not be
    ///    reported.
    /// 2. Attribution must follow the *returned value*, not the condition:
    ///    neutralising a trusted root that only steers a branch must not claim
    ///    a reference the *other* (untrusted) source supplied; and two trusted
    ///    roots carrying the same name into the result must BOTH be attributed
    ///    (the name is reported once, but neither root may mask the other).
    /// 3. A trusted root the expression never reads must never trigger a
    ///    neutralised re-evaluation (the endpoint supplies every visible
    ///    SECRET_REFERENCE variable; N unrelated variables must not cost N+1
    ///    executions).
    #[test]
    fn eval_with_provenance_is_result_reachable_and_branch_aware() {
        let prov = |e: &str, c: &HashMap<String, Value>, trusted: &[&str]| {
            eval_with_provenance(
                e,
                c,
                trusted.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
            )
            .unwrap()
            .referenced_secrets
        };

        // --- (1) A discarded literal reference is not reported -----------
        // `fromAi(u, camunda.secrets.TOKEN)` returns only its first argument:
        // the literal second argument is evaluated but discarded, so TOKEN
        // never reaches the result and the untrusted `u` copy is not trusted.
        let c = ctx(&[("u", Value::Str("camunda.secrets.TOKEN".into()))]);
        let r = eval_with_provenance(r#"fromAi(u, camunda.secrets.TOKEN)"#, &c, vec![]).unwrap();
        assert_eq!(r.value, Value::Str("camunda.secrets.TOKEN".into()));
        assert!(
            r.referenced_secrets.is_empty(),
            "a discarded literal argument must not be reported: {:?}",
            r.referenced_secrets
        );
        // A condition-only literal: the reference steers the branch but the
        // result carries no occurrence of it.
        assert!(
            prov(
                r#"if camunda.secrets.TOKEN = "x" then 1 else 2"#,
                &ctx(&[]),
                &[]
            )
            .is_empty(),
            "a literal consumed only by a condition is not reported"
        );
        // A discarded context member: the literal is bound into a context that
        // is then projected away from the reference.
        assert!(
            prov("{a: camunda.secrets.TOKEN, b: 1}.b", &ctx(&[]), &[]).is_empty(),
            "a context member discarded by projection is not reported"
        );
        // …but a literal that DOES reach the result is still reported.
        assert_eq!(
            prov("camunda.secrets.DIRECT", &ctx(&[]), &[]),
            vec!["DIRECT"]
        );
        assert_eq!(
            prov("[camunda.secrets.A, 1][1]", &ctx(&[]), &[]),
            vec!["A"],
            "a literal selected into the result is reported"
        );

        // --- (2) Branch-aware attribution --------------------------------
        // Trusted `t` and untrusted `u` both hold the same reference text.
        // Neutralising `t` flips the `if` branch, so the *result* changes —
        // but the reference in the result came from `u`, not `t`. The
        // branch-aware guard (the neutralised result must otherwise match the
        // real one) refuses the attribution.
        let c = ctx(&[
            ("t", Value::Str("camunda.secrets.TOKEN".into())),
            ("u", Value::Str("camunda.secrets.TOKEN".into())),
        ]);
        assert!(
            prov(
                r#"if t = "camunda.secrets.TOKEN" then u else null"#,
                &c,
                &["t"]
            )
            .is_empty(),
            "neutralising the condition root must not claim the untrusted branch value"
        );
        // Two trusted roots carrying the SAME name into the result: the name
        // is reported (once), and neutralising either root alone leaves the
        // other root's copy in the result — so the naive per-root diff would
        // report nothing. The name must still surface.
        let c = ctx(&[
            ("a", Value::Str("camunda.secrets.TOKEN".into())),
            ("b", Value::Str("camunda.secrets.TOKEN".into())),
        ]);
        assert_eq!(
            prov("[a, b]", &c, &["a", "b"]),
            vec!["TOKEN"],
            "two trusted roots carrying the same name must not mask each other"
        );
        // …and when only ONE trusted root carries it, attribution still works.
        let c = ctx(&[
            ("a", Value::Str("camunda.secrets.TOKEN".into())),
            ("b", Value::Str("plain".into())),
        ]);
        assert_eq!(prov("[a, b]", &c, &["a", "b"]), vec!["TOKEN"]);
        // A trusted carrier's occurrence is still reported when an UNTRUSTED
        // copy of the same name also reaches the result: neutralising every
        // trusted root removes the trusted occurrence but keeps the untrusted
        // one, so the count diff (not set membership) attributes it.
        let c = ctx(&[
            ("t", Value::Str("camunda.secrets.TOKEN".into())),
            ("u", Value::Str("camunda.secrets.TOKEN".into())),
        ]);
        assert_eq!(
            prov("[t, u]", &c, &["t"]),
            vec!["TOKEN"],
            "an untrusted copy must not mask the trusted occurrence"
        );
        // A trusted root read only by the condition, with the reference
        // supplied by an untrusted branch value, is not reported even when
        // the branch does NOT flip (the reference text still reaches the
        // result from `u` alone).
        let c = ctx(&[
            ("t", Value::Str("camunda.secrets.TOKEN".into())),
            ("u", Value::Str("camunda.secrets.TOKEN".into())),
        ]);
        assert!(
            prov(r#"if t = "other" then null else u"#, &c, &["t"]).is_empty(),
            "the else-branch value is untrusted even when the condition root is trusted"
        );

        // --- (3) An unread trusted root costs no re-evaluation -----------
        // `unused` is a reference-bearing trusted root the expression never
        // reads; only `token_ref` is read. Provenance must still be correct
        // (TOKEN reported, UNUSED not) — and the implementation must not
        // re-evaluate for `unused` (asserted structurally by the read-set
        // restriction: an unread carrier is skipped).
        let c = ctx(&[
            ("token_ref", Value::Str("camunda.secrets.TOKEN".into())),
            ("unused", Value::Str("camunda.secrets.UNUSED".into())),
        ]);
        assert_eq!(
            prov("token_ref", &c, &["token_ref", "unused"]),
            vec!["TOKEN"]
        );
        // A reference-free expression over many reference-bearing roots
        // reports nothing (and pays for exactly one evaluation).
        assert!(
            prov("1 + 1", &c, &["token_ref", "unused"]).is_empty(),
            "an expression that reads no trusted root reports nothing"
        );
    }

    /// #1296 review (round 6): trust must follow the returned VALUE, never a
    /// bare name match. Three defect classes the name-only checks got wrong,
    /// pinned red-first:
    ///
    /// 1. A discarded literal `camunda.secrets.<name>` path must not launder a
    ///    *read untrusted variable's* same-text copy into a trusted report
    ///    (`fromAi(u, camunda.secrets.TOKEN)`). (A *computed* or *locally-bound*
    ///    single occurrence — `fromAi("camunda.secrets." + name, …)`, `{x:
    ///    "camunda.secrets.TOKEN", …}.x` — is indistinguishable from the
    ///    literal's own by the result value alone; declined on the thread.)
    /// 2. An UNREAD cluster variable must not suppress a literal reference
    ///    that actually reaches the result: only a source that supplies a
    ///    returned occurrence can contest a literal's trust.
    /// 3. Nondeterministic draws (`uuid()`, `random number()`, `now()`) must
    ///    not break the neutralisation comparison: a fresh draw in the
    ///    re-evaluation is not a shape change.
    #[test]
    fn eval_with_provenance_trusts_only_returned_trusted_occurrences() {
        let prov = |e: &str, c: &HashMap<String, Value>, trusted: &[&str]| {
            eval_with_provenance(
                e,
                c,
                trusted.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
            )
            .unwrap()
            .referenced_secrets
        };

        // --- (1) Discarded-literal laundering is bounded to the sound core --
        // A literal that supplies the result's ONLY occurrence is trusted…
        assert_eq!(
            prov("camunda.secrets.DIRECT", &ctx(&[]), &[]),
            vec!["DIRECT"]
        );
        // …and a discarded literal whose result occurrence a read UNTRUSTED
        // variable also carries is not (the untrusted copy contests it).
        let c = ctx(&[("u", Value::Str("camunda.secrets.TOKEN".into()))]);
        assert!(
            prov(r#"fromAi(u, camunda.secrets.TOKEN)"#, &c, &[]).is_empty(),
            "a discarded literal must not launder a read untrusted variable's copy"
        );
        // A *computed* or *locally-bound* single occurrence that merely
        // matches the reference text (`fromAi("camunda.secrets." + name, …)`,
        // `{x: "camunda.secrets.TOKEN", …}.x`) is indistinguishable from the
        // literal's own occurrence by the result value alone — the
        // differential mechanism is value-based, so it cannot separate two
        // identical strings. Those are declined on the thread: closing them
        // needs provenance attached to the value (the in-band taint wrapper
        // the round-4 review removed for breaking native dispatch), not a
        // name/occurrence check.

        // --- (2) An unread carrier must not suppress a reaching literal ---
        // `unrelated` is a visible SECRET_REFERENCE variable the expression
        // never reads; the literal itself supplies the result's occurrence.
        let c = ctx(&[("unrelated", Value::Str("camunda.secrets.TOKEN".into()))]);
        assert_eq!(
            prov("camunda.secrets.TOKEN", &c, &["unrelated"]),
            vec!["TOKEN"],
            "an unread same-named cluster variable must not suppress the literal"
        );

        // --- (3) Nondeterministic draws do not suppress attribution -------
        // The trusted token is returned alongside a fresh UUID; the
        // neutralised re-evaluation draws a different UUID, which must not
        // count as a shape change.
        let c = ctx(&[("t", Value::Str("camunda.secrets.TOKEN".into()))]);
        let r = eval_with_provenance("{token: t, id: uuid()}", &c, vec!["t".to_string()]).unwrap();
        assert_eq!(
            r.referenced_secrets,
            vec!["TOKEN"],
            "a fresh uuid() in the re-evaluation must not suppress attribution"
        );
        let id = r.value;
        // `random number()` and `now()` are the same class.
        let r = eval_with_provenance(
            "{token: t, roll: random number()}",
            &c,
            vec!["t".to_string()],
        )
        .unwrap();
        assert_eq!(r.referenced_secrets, vec!["TOKEN"]);
        let r = eval_with_provenance("{token: t, at: now()}", &c, vec!["t".to_string()]).unwrap();
        assert_eq!(r.referenced_secrets, vec!["TOKEN"]);
        // …and the uuid really is nondeterministic (the test above is not
        // vacuously passing on a constant).
        if let Value::Map(entries) = &id {
            let first = entries.get("id").cloned().unwrap_or(Value::Null);
            let second = eval_with_provenance("{token: t, id: uuid()}", &c, vec!["t".to_string()])
                .unwrap()
                .value;
            if let Value::Map(entries2) = &second {
                assert_ne!(
                    first,
                    entries2.get("id").cloned().unwrap_or(Value::Null),
                    "uuid() must draw fresh values"
                );
            }
        }
    }

    #[test]
    fn evaluates_comparisons_and_equality() {
        let c = ctx(&[
            ("amount", Value::Int(42)),
            ("name", Value::Str("ann".into())),
        ]);
        assert_eq!(eval("amount > 10", &c), Ok(Value::Bool(true)));
        assert_eq!(eval("amount >= 42", &c), Ok(Value::Bool(true)));
        assert_eq!(eval("amount = 42", &c), Ok(Value::Bool(true)));
        assert_eq!(eval("amount != 7", &c), Ok(Value::Bool(true)));
        assert_eq!(eval("amount = 42.0", &c), Ok(Value::Bool(true)));
        assert_eq!(eval(r#"name = "ann""#, &c), Ok(Value::Bool(true)));
        assert_eq!(eval(r#"name < "bob""#, &c), Ok(Value::Bool(true)));
    }

    #[test]
    fn evaluates_boolean_logic() {
        let c = ctx(&[("a", Value::Bool(true)), ("b", Value::Bool(false))]);
        assert_eq!(eval("a and b", &c), Ok(Value::Bool(false)));
        assert_eq!(eval("a or b", &c), Ok(Value::Bool(true)));
        assert_eq!(eval("not(b)", &c), Ok(Value::Bool(true)));
        assert_eq!(eval("not b", &c), Ok(Value::Bool(true)));
    }

    #[test]
    fn member_access_reads_context_entries() {
        let mut order = BTreeMap::new();
        order.insert("total".to_string(), Value::Int(99));
        let c = ctx(&[("order", Value::Map(order))]);
        assert_eq!(eval("order.total", &c), Ok(Value::Int(99)));
        assert_eq!(eval("order.total > 50", &c), Ok(Value::Bool(true)));
        assert_eq!(eval("order.missing", &c), Ok(Value::Null));
    }

    #[test]
    fn eval_bool_and_string_helpers() {
        let c = ctx(&[
            ("jobType", Value::Str("payment".into())),
            ("n", Value::Int(3)),
        ]);
        assert_eq!(eval_string("=jobType", &c), Ok("payment".to_string()));
        assert_eq!(eval_string("=n", &c), Ok("3".to_string()));
        assert_eq!(eval_bool("n > 1", &c), Ok(true));
        assert!(eval_bool("n", &c).is_err());
    }

    #[test]
    fn syntax_errors_are_reported() {
        let c = ctx(&[]);
        assert!(eval("1 +", &c).is_err());
        assert!(eval("(1 + 2", &c).is_err());
    }

    /// Defect-class guard for #1392 / #1393 / #1394: an operator applied to
    /// ill-typed operands is a *suppressed failure* in feel-scala 1.22.1 (the
    /// version the pinned Zeebe embeds) — the result is `null`, never an
    /// evaluation error — and a non-boolean `if` condition takes the `else`
    /// branch. Every expected outcome below was captured from feel-scala 1.22.1
    /// (`formal/feel-scala/FeelScalaEval.java`), so the table *is* the Zeebe
    /// oracle for every operator family, not only the fuzzed subset.
    #[test]
    fn ill_typed_operators_yield_null_like_feel_scala() {
        let c = ctx(&[("name", Value::Str("ann".into()))]);
        let null = Ok(Value::Null);
        let t = Ok(Value::Bool(true));
        let f = Ok(Value::Bool(false));
        let two = Ok(Value::Int(2));
        let cases: &[(&str, &Result<Value, FeelError>)] = &[
            // #1392 — arithmetic, unary and comparison type errors → null.
            ("name + 1", &null),
            ("1 + true", &null),
            ("-false", &null),
            (r#"-"a""#, &null),
            ("- null", &null),
            ("not(5)", &null),
            ("not(5) = 6", &f),
            (r#""a" * 2"#, &null),
            (r#"2 ** "a""#, &null),
            ("2 ** null", &null),
            (r#""a" - "b""#, &null),
            (r#"1 / "a""#, &null),
            ("null + 1", &null),
            (r#"@"P1D" + 1"#, &null),
            (r#"date("2020-01-01") + 1"#, &null),
            (r#"1 < "a""#, &null),
            ("1 < null", &null),
            ("null < null", &null),
            (r#"date("2020-01-01") < 1"#, &null),
            (r#"1 between "a" and 3"#, &null),
            ("1 between null and 3", &null),
            ("null between 1 and 2", &null),
            (r#""a" in (2..5)"#, &null),
            (r#"1 in "a""#, &null),
            ("(1 + true) = null", &t),
            ("true and 5", &null),
            ("false and 5", &f),
            ("5 or true", &t),
            // #1393 — a non-boolean `if` condition takes the else branch.
            ("if 5 then 1 else 2", &two),
            ("if null then 1 else 2", &two),
            (r#"if "x" then 1 else 2"#, &two),
            ("if 1 + true then 1 else 2", &two),
            // #1394 — `=` / `!=` across mismatched non-null types → null.
            (r#"1 = "1""#, &null),
            (r#"false = "ab""#, &null),
            (r#"1 != "a""#, &null),
            ("1 = true", &null),
            ("true != 1", &null),
            ("[1] = 1", &null),
            ("{a:1} = 1", &null),
            (r#"date("2020-01-01") = 1"#, &null),
            // ...while null against a value, and nested mismatches, stay boolean.
            ("null = 1", &f),
            ("null != 1", &t),
            ("null = null", &t),
            (r#"[1,2] = [1,"a"]"#, &f),
            (r#"{a:1} = {a:"x"}"#, &f),
            ("[1] = [1]", &t),
            // List membership tolerates mismatched items.
            (r#"1 in [1,"a"]"#, &t),
            (r#""a" in [1,2]"#, &f),
            ("5 in null", &f),
            ("null in [1]", &f),
            ("1 in 1", &t),
        ];
        let mut failures = Vec::new();
        for (expr, want) in cases {
            let got = eval(expr, &c);
            if &got != *want {
                failures.push(format!("{expr}: got {got:?}, feel-scala {want:?}"));
            }
        }
        assert!(failures.is_empty(), "{}", failures.join("\n"));
    }

    #[test]
    fn list_literals_and_equality() {
        let c = ctx(&[]);
        assert_eq!(
            eval("[1, 2, 3]", &c),
            Ok(Value::List(vec![
                Value::Int(1),
                Value::Int(2),
                Value::Int(3)
            ]))
        );
    }

    // --- new grammar -------------------------------------------------------

    #[test]
    fn quantifiers() {
        let docs = Value::List(vec![
            ctx_value(&[("status", Value::Str("OK".into()))]),
            ctx_value(&[("status", Value::Str("REJECTED".into()))]),
        ]);
        let c = ctx(&[("documents", docs)]);
        assert_eq!(
            eval_bool(r#"some d in documents satisfies d.status = "REJECTED""#, &c),
            Ok(true)
        );
        assert_eq!(
            eval_bool(r#"every d in documents satisfies d.status = "OK""#, &c),
            Ok(false)
        );
    }

    #[test]
    fn in_operator_with_list_and_range() {
        let c = ctx(&[
            ("t", Value::Str("CDD_REMINDER_7DAY".into())),
            ("n", Value::Int(5)),
        ]);
        assert_eq!(
            eval_bool(r#"t in ["CDD_REFRESH_REQUEST", "CDD_REMINDER_7DAY"]"#, &c),
            Ok(true)
        );
        assert_eq!(eval_bool("n in [1..10]", &c), Ok(true));
        assert_eq!(eval_bool("n in (5..10]", &c), Ok(false));
        assert_eq!(eval_bool("n in < 10", &c), Ok(true));
    }

    #[test]
    fn if_then_else_and_between() {
        let c = ctx(&[("score", Value::Int(680))]);
        assert_eq!(
            eval_string(r#"if score < 700 then "deny" else "approve""#, &c),
            Ok("deny".to_string())
        );
        assert_eq!(eval_bool("score between 600 and 700", &c), Ok(true));
    }

    #[test]
    fn for_comprehension_and_filter() {
        let c = ctx(&[]);
        assert_eq!(
            eval("for x in [1, 2, 3] return x * x", &c),
            Ok(Value::List(vec![
                Value::Int(1),
                Value::Int(4),
                Value::Int(9)
            ]))
        );
        assert_eq!(
            eval("[1, 2, 3, 4][item > 2]", &c),
            Ok(Value::List(vec![Value::Int(3), Value::Int(4)]))
        );
        assert_eq!(eval("[10, 20, 30][1]", &c), Ok(Value::Int(10)));
        assert_eq!(eval("[10, 20, 30][-1]", &c), Ok(Value::Int(30)));
    }

    #[test]
    fn context_literal_and_projection() {
        let c = ctx(&[]);
        assert_eq!(eval("{a: 1, b: a + 1}.b", &c), Ok(Value::Int(2)));
        assert_eq!(
            eval("[{x: 1}, {x: 2}].x", &c),
            Ok(Value::List(vec![Value::Int(1), Value::Int(2)]))
        );
    }

    #[test]
    fn builtin_functions() {
        let c = ctx(&[]);
        assert_eq!(eval("count([1, 2, 3])", &c), Ok(Value::Int(3)));
        assert_eq!(eval("sum([1, 2, 3])", &c), Ok(Value::Int(6)));
        assert_eq!(eval("max(3, 7, 2)", &c), Ok(Value::Int(7)));
        assert_eq!(eval(r#"upper case("hi")"#, &c), Ok(Value::Str("HI".into())));
        assert_eq!(eval(r#"string length("hello")"#, &c), Ok(Value::Int(5)));
        assert_eq!(
            eval(r#"contains("banana", "nan")"#, &c),
            Ok(Value::Bool(true))
        );
        assert_eq!(
            eval(r#"list contains([1, 2], 2)"#, &c),
            Ok(Value::Bool(true))
        );
        assert_eq!(
            eval(r#"substring("hello", 2, 3)"#, &c),
            Ok(Value::Str("ell".into()))
        );
        assert_eq!(
            eval(r#"matches("abc123", "[a-z]+\\d+")"#, &c),
            Ok(Value::Bool(true))
        );
        assert_eq!(
            eval(r#"replace("banana", "a", "o")"#, &c),
            Ok(Value::Str("bonono".into()))
        );
        assert_eq!(
            eval("sort([3, 1, 2])", &c),
            Ok(Value::List(vec![
                Value::Int(1),
                Value::Int(2),
                Value::Int(3)
            ]))
        );
        assert_eq!(eval("floor(3.7)", &c), Ok(Value::Int(3)));
        assert_eq!(eval("abs(-4)", &c), Ok(Value::Int(4)));
        assert_eq!(eval("odd(3)", &c), Ok(Value::Bool(true)));
    }

    #[test]
    fn from_ai_returns_first_argument_unchanged() {
        // The Camunda agentic `fromAi(value, description, type, ...)` built-in is a
        // declaration, not a computation: the FEEL engine returns `value` verbatim
        // and ignores the metadata arguments. See nanobpm/nano-bpm#1200.
        let mut tool_call = BTreeMap::new();
        tool_call.insert("foo".to_string(), Value::Str("VALUE-FROM-LLM".into()));
        let c = ctx(&[("toolCall", Value::Map(tool_call))]);

        // All documented overloads reduce to the value slot.
        assert_eq!(
            eval(r#"fromAi(toolCall.foo)"#, &c),
            Ok(Value::Str("VALUE-FROM-LLM".into()))
        );
        assert_eq!(
            eval(r#"fromAi(toolCall.foo, "desc")"#, &c),
            Ok(Value::Str("VALUE-FROM-LLM".into()))
        );
        assert_eq!(
            eval(r#"fromAi(toolCall.foo, "desc", "string")"#, &c),
            Ok(Value::Str("VALUE-FROM-LLM".into()))
        );
        assert_eq!(
            eval(r#"fromAi(toolCall.foo, "desc", "string", {})"#, &c),
            Ok(Value::Str("VALUE-FROM-LLM".into()))
        );
        assert_eq!(
            eval(r#"fromAi(toolCall.foo, "desc", "string", {}, {})"#, &c),
            Ok(Value::Str("VALUE-FROM-LLM".into()))
        );
        // A missing value slot resolves to null rather than raising an incident.
        assert_eq!(eval(r#"fromAi(toolCall.missing)"#, &c), Ok(Value::Null));
        // A bare `fromAi()` with no value argument also resolves to null.
        assert_eq!(eval(r#"fromAi()"#, &c), Ok(Value::Null));
        // Named arguments are supported, e.g. fromAi(value: ..., type: ...).
        assert_eq!(
            eval(r#"fromAi(value: toolCall.foo, type: "number")"#, &c),
            Ok(Value::Str("VALUE-FROM-LLM".into()))
        );
    }

    #[test]
    fn named_arguments_and_lambdas() {
        let c = ctx(&[]);
        assert_eq!(
            eval(r#"substring(string: "hello", start position: 2)"#, &c),
            Ok(Value::Str("ello".into()))
        );
        assert_eq!(
            eval("sort([3, 1, 2], function(a, b) a < b)", &c),
            Ok(Value::List(vec![
                Value::Int(1),
                Value::Int(2),
                Value::Int(3)
            ]))
        );
    }

    #[test]
    fn instance_of_checks() {
        let c = ctx(&[("n", Value::Int(3)), ("s", Value::Str("x".into()))]);
        assert_eq!(eval_bool("n instance of number", &c), Ok(true));
        assert_eq!(eval_bool("s instance of number", &c), Ok(false));
        assert_eq!(eval_bool("s instance of string", &c), Ok(true));
    }

    // --- temporal ----------------------------------------------------------

    #[test]
    fn temporal_literals_and_arithmetic() {
        let c = ctx(&[]);
        assert_eq!(
            eval(r#"date("2024-01-15")"#, &c),
            Ok(Value::Str("2024-01-15".into()))
        );
        assert_eq!(
            eval(r#"date("2024-01-31") + duration("P1M")"#, &c),
            Ok(Value::Str("2024-02-29".into()))
        );
        assert_eq!(
            eval(r#"date("2024-12-31") - date("2024-01-01")"#, &c),
            Ok(Value::Str("P365D".into()))
        );
        assert_eq!(
            eval(r#"@"2024-01-01" < @"2024-06-01""#, &c),
            Ok(Value::Bool(true))
        );
        assert_eq!(
            eval(
                r#"date and time("2024-01-01T10:00:00") + duration("PT2H")"#,
                &c
            ),
            Ok(Value::Str("2024-01-01T12:00:00".into()))
        );
        assert_eq!(eval(r#"date("2024-03-15").month"#, &c), Ok(Value::Int(3)));
        assert_eq!(
            eval(r#"day of week(date("2024-01-01"))"#, &c),
            Ok(Value::Str("Monday".into()))
        );
    }

    #[test]
    fn duration_arithmetic() {
        let c = ctx(&[]);
        assert_eq!(
            eval(r#"duration("P1D") + duration("PT12H")"#, &c),
            Ok(Value::Str("P1DT12H".into()))
        );
        assert_eq!(
            eval(r#"duration("P2Y") = duration("P24M")"#, &c),
            Ok(Value::Bool(true))
        );
    }

    #[test]
    fn motivating_cdd_condition() {
        // The standard-FEEL form of the CDD phase-04 gateway guard.
        let docs = Value::List(vec![
            ctx_value(&[("status", Value::Str("APPROVED".into()))]),
            ctx_value(&[("status", Value::Str("REJECTED".into()))]),
        ]);
        let c = ctx(&[("documents", docs)]);
        assert_eq!(
            eval_bool(r#"some d in documents satisfies d.status = "REJECTED""#, &c),
            Ok(true)
        );
    }

    // --- feel-scala parity: new builtins -----------------------------------

    #[test]
    fn interval_before_after_points_and_ranges() {
        let c = ctx(&[]);
        // point / point
        assert_eq!(eval_bool("before(1, 10)", &c), Ok(true));
        assert_eq!(eval_bool("after(10, 1)", &c), Ok(true));
        // point / range
        assert_eq!(eval_bool("before(1, [5..10])", &c), Ok(true));
        assert_eq!(eval_bool("before(5, [5..10])", &c), Ok(false));
        assert_eq!(eval_bool("before(5, (5..10])", &c), Ok(true));
        // range / point
        assert_eq!(eval_bool("before([1..5], 10)", &c), Ok(true));
        assert_eq!(eval_bool("after([11..20], 10)", &c), Ok(true));
        // range / range
        assert_eq!(eval_bool("before([1..5], [6..10])", &c), Ok(true));
        assert_eq!(eval_bool("before([1..5], [5..10])", &c), Ok(false));
        assert_eq!(eval_bool("before([1..5), [5..10])", &c), Ok(true));
    }

    #[test]
    fn interval_meets_overlaps_includes_during() {
        let c = ctx(&[]);
        assert_eq!(eval_bool("meets([1..5], [5..10])", &c), Ok(true));
        assert_eq!(eval_bool("meets([1..5), [5..10])", &c), Ok(false));
        assert_eq!(eval_bool("met by([5..10], [1..5])", &c), Ok(true));
        assert_eq!(eval_bool("overlaps([1..5], [3..8])", &c), Ok(true));
        assert_eq!(eval_bool("overlaps([1..5], [6..8])", &c), Ok(false));
        assert_eq!(eval_bool("overlaps before([1..5], [3..8])", &c), Ok(true));
        assert_eq!(eval_bool("overlaps after([3..8], [1..5])", &c), Ok(true));
        assert_eq!(eval_bool("includes([1..10], 5)", &c), Ok(true));
        assert_eq!(eval_bool("includes([1..10], [4..6])", &c), Ok(true));
        assert_eq!(eval_bool("during(5, [1..10])", &c), Ok(true));
        assert_eq!(eval_bool("during([4..6], [1..10])", &c), Ok(true));
        assert_eq!(eval_bool("finishes(10, [1..10])", &c), Ok(true));
        assert_eq!(eval_bool("finished by([1..10], 10)", &c), Ok(true));
        assert_eq!(eval_bool("starts(1, [1..10])", &c), Ok(true));
        assert_eq!(eval_bool("started by([1..10], 1)", &c), Ok(true));
        assert_eq!(eval_bool("coincides([1..5], [1..5])", &c), Ok(true));
        assert_eq!(eval_bool("coincides([1..5], [2..5])", &c), Ok(false));
    }

    #[test]
    fn interval_dates_are_comparable() {
        let c = ctx(&[]);
        assert_eq!(
            eval_bool(
                r#"before(date("2024-01-01"), [date("2024-02-01")..date("2024-03-01")])"#,
                &c
            ),
            Ok(true)
        );
    }

    #[test]
    fn string_extract_uuid_base64() {
        let c = ctx(&[]);
        assert_eq!(
            eval(r#"extract("references are 1234 and 5678", "[0-9]+")"#, &c),
            Ok(Value::List(vec![
                Value::Str("1234".into()),
                Value::Str("5678".into())
            ]))
        );
        assert_eq!(
            eval(r#"to base64("FEEL")"#, &c),
            Ok(Value::Str("RkVFTA==".into()))
        );
        assert_eq!(
            eval(r#"from base64("RkVFTA==")"#, &c),
            Ok(Value::Str("FEEL".into()))
        );
        // uuid() round-trips through base64 shape: 36 chars with dashes.
        match eval("uuid()", &c) {
            Ok(Value::Str(s)) => {
                assert_eq!(s.len(), 36);
                assert_eq!(s.chars().filter(|&ch| ch == '-').count(), 4);
                assert_eq!(&s[14..15], "4"); // version nibble
            }
            other => panic!("uuid() returned {other:?}"),
        }
    }

    #[test]
    fn numeric_overloads_and_random() {
        let c = ctx(&[]);
        assert_eq!(eval("floor(-1.5, 0)", &c), Ok(Value::Int(-2)));
        assert_eq!(eval("ceiling(1.01, 1)", &c), Ok(Value::Double(1.1)));
        assert_eq!(eval(r#"decimal(1.5, 0, "HALF_UP")"#, &c), Ok(Value::Int(2)));
        assert_eq!(eval(r#"decimal(1.5, 0, "DOWN")"#, &c), Ok(Value::Int(1)));
        assert_eq!(eval(r#"decimal(-1.5, 0, "FLOOR")"#, &c), Ok(Value::Int(-2)));
        match eval("random number()", &c) {
            Ok(Value::Double(d)) => assert!((0.0..1.0).contains(&d)),
            other => panic!("random number() returned {other:?}"),
        }
    }

    #[test]
    fn list_and_context_aliases() {
        let c = ctx(&[]);
        assert_eq!(eval("and([true, true, false])", &c), Ok(Value::Bool(false)));
        assert_eq!(eval("or([false, true])", &c), Ok(Value::Bool(true)));
        assert_eq!(
            eval(r#"put({x: 1}, "y", 2)"#, &c),
            Ok(ctx_value(&[("x", Value::Int(1)), ("y", Value::Int(2))]))
        );
        assert_eq!(
            eval(r#"put all({x: 1}, {y: 2})"#, &c),
            Ok(ctx_value(&[("x", Value::Int(1)), ("y", Value::Int(2))]))
        );
    }

    #[test]
    fn boolean_assert() {
        let c = ctx(&[("x", Value::Int(7))]);
        assert_eq!(eval("assert(x, x > 0)", &c), Ok(Value::Int(7)));
        assert!(eval("assert(x, x > 100)", &c).is_err());
        assert!(eval(r#"assert(x, false, "must be set")"#, &c).is_err());
    }

    fn ctx_value(pairs: &[(&str, Value)]) -> Value {
        Value::Map(
            pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.clone()))
                .collect(),
        )
    }
}
