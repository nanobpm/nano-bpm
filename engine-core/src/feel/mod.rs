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

use std::collections::HashMap;

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
) -> Result<(Value, &'static str), FeelError> {
    let v = evaluate(expr, ctx)?;
    let kind = match &v {
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
    };
    Ok((v.into_value(), kind))
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
/// binds `item` over the predicate — except a BARE variable index
/// (`orders[item]`), which resolves numerically from the current scope and so
/// keeps `item` free. Returns an empty set if the expression
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
            // element with `item` bound to the element under test (eval_index).
            // A static walk cannot know the base's elements, so it cannot
            // foresee the context-entry names a filter would also bind — but
            // it can settle `item`: a BARE variable index (`orders[item]`)
            // resolves numerically whenever the outer scope supplies the name,
            // so it is a genuine outer reference and stays free; any other
            // index shape is treated as a filter, binding `item` over the
            // predicate. That mirrors the dependency question this walk
            // answers: with `inputElement="item"` absent at the body level,
            // `orders[item]` MUST stay free (skipping the mapping is exactly
            // the tolerated case), while `[1, 2][item > 0]` must not skip.
            if matches!(idx.as_ref(), Node::Var(_)) {
                collect_free_vars(idx, bound, out);
            } else {
                let mut inner = bound.clone();
                inner.insert("item".to_string());
                collect_free_vars(idx, &mut inner, out);
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
        // A bare numeric index is NOT a filter: `item` there is a genuine
        // outer reference and stays free.
        assert_eq!(free("=orders[item]"), vec!["item", "orders"]);
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
