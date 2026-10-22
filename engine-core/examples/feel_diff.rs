//! FEEL differential-fuzz checker (issue #1230).
//!
//! Reads a corpus produced by the Lean reference semantics (`lake exe feelfuzz`,
//! see `formal/lean/`) — tab-separated `expression \t context \t
//! reference-outcome` rows — re-evaluates each expression with this crate's
//! `feel::eval`, and asserts the canonical outcome matches the Lean reference.
//! Any divergence exits non-zero with the offending row: a mismatch is a real
//! FEEL semantics drift between the Rust engine and the formal reference, never
//! tolerated and never retried.
//!
//! Usage: `feel_diff <corpus-file>` (or `-` / stdin).

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "non-production target (integration tests / examples); the production deny-unwrap gate (#1357) applies to the library crate only"
)]
use std::collections::HashMap;
use std::io::{self, Read};

use nanobpmn_engine_core::feel;
use nanobpmn_engine_core::Value;

/// Renders a resulting engine [`Value`] to the canonical form the Lean reference
/// also emits. Kept byte-identical to `Feel.Val.canon` / `Feel.Outcome.canon`.
fn canon_value(v: &Value) -> String {
    match v {
        Value::Null => "null".to_string(),
        Value::Bool(b) => format!("bool:{b}"),
        Value::Int(i) => format!("num:{i}"),
        Value::Double(d) => {
            // Mirror engine-core's `format_double`: an integral finite double
            // renders as its integer, matching a Lean `num:<decimal>`.
            if d.is_finite() && d.fract() == 0.0 && d.abs() < i64::MAX as f64 {
                format!("num:{}", *d as i64)
            } else {
                format!("num:{d}")
            }
        }
        Value::Str(s) => format!("str:{s}"),
        // The fuzz domain never produces these; surface them so a stray one is a
        // visible mismatch rather than a silent pass.
        Value::List(_) => "other:list".to_string(),
        Value::Map(_) => "other:context".to_string(),
    }
}

/// Canonical outcome of evaluating `expr` in `ctx` with the Rust engine.
fn canon_outcome(expr: &str, ctx: &HashMap<String, Value>) -> String {
    match feel::eval(expr, ctx) {
        Ok(v) => format!("ok:{}", canon_value(&v)),
        Err(_) => "err".to_string(),
    }
}

/// Parses a `name=type:value` (or `name=null`) context encoding into engine
/// variables.
fn parse_ctx(enc: &str) -> Result<HashMap<String, Value>, String> {
    let mut ctx = HashMap::new();
    if enc.is_empty() {
        return Ok(ctx);
    }
    for piece in enc.split(';') {
        let (name, rhs) = piece
            .split_once('=')
            .ok_or_else(|| format!("bad context piece {piece:?}"))?;
        let value = if rhs == "null" {
            Value::Null
        } else if let Some(b) = rhs.strip_prefix("bool:") {
            // Only the literal `true`/`false` are valid; any other suffix is a
            // malformed row and must fail the gate, not be silently coerced to
            // `false` (Copilot review of PR #1253).
            match b {
                "true" => Value::Bool(true),
                "false" => Value::Bool(false),
                _ => return Err(format!("bad bool {b:?}")),
            }
        } else if let Some(i) = rhs.strip_prefix("int:") {
            Value::Int(i.parse().map_err(|_| format!("bad int {i:?}"))?)
        } else if let Some(s) = rhs.strip_prefix("str:") {
            Value::Str(s.to_string())
        } else {
            return Err(format!("bad context value {rhs:?}"));
        };
        ctx.insert(name.to_string(), value);
    }
    Ok(ctx)
}

/// Checks every non-blank row of `input` against the Rust FEEL evaluator,
/// writing any divergence diagnostics to `err`. Returns `Ok(checked)` with the
/// number of cases verified, or `Err(message)` on any failure — a divergence, a
/// malformed row, **or** a corpus that contained no cases at all. The
/// zero-checked case is a failure on purpose: an empty or truncated corpus (a
/// crashed `feelfuzz`, a short read) would otherwise leave `failures == 0` and
/// pass the differential gate without verifying a single FEEL case.
fn check_corpus(input: &str, mut err: impl std::fmt::Write) -> Result<usize, String> {
    let mut checked = 0usize;
    let mut failures = 0usize;
    for (lineno, line) in input.lines().enumerate() {
        if line.is_empty() {
            continue;
        }
        let mut fields = line.split('\t');
        let expr = fields.next().unwrap_or("");
        let enc = fields.next().unwrap_or("");
        let expected = fields.next().unwrap_or("");
        if fields.next().is_some() {
            let _ = writeln!(err, "line {}: too many fields", lineno + 1);
            failures += 1;
            continue;
        }
        // A blank expression is a truncated/malformed row, not a real generated
        // case: `feel::eval("")` returns `Err`, so a `\t\terr` row would match
        // `expected == "err"`, increment `checked`, and satisfy the row-count
        // gate without evaluating a single generated expression. Reject it
        // before differential evaluation (Copilot review of PR #1253).
        if expr.trim().is_empty() {
            let _ = writeln!(err, "line {}: blank expression", lineno + 1);
            failures += 1;
            continue;
        }
        let ctx = match parse_ctx(enc) {
            Ok(c) => c,
            Err(e) => {
                let _ = writeln!(err, "line {}: {e}", lineno + 1);
                failures += 1;
                continue;
            }
        };
        let got = canon_outcome(expr, &ctx);
        checked += 1;
        if got != expected {
            failures += 1;
            let _ = writeln!(
                err,
                "DIVERGENCE line {}:\n  expr:     {expr}\n  context:  {enc}\n  expected (Lean): {expected}\n  got (Rust):      {got}",
                lineno + 1
            );
        }
    }

    if failures > 0 {
        return Err(format!("{failures} divergence(s) over {checked} case(s)"));
    }
    // An empty or all-blank corpus must never pass as success: a broken
    // `feelfuzz` executable or a truncated corpus would otherwise make the
    // differential gate green without checking a single FEEL case.
    if checked == 0 {
        return Err(
            "corpus contained no FEEL cases to check (empty or truncated) — refusing to pass the differential gate"
                .to_string(),
        );
    }
    Ok(checked)
}

fn main() {
    let path = std::env::args().nth(1).unwrap_or_else(|| "-".to_string());
    let mut input = String::new();
    if path == "-" {
        io::stdin().read_to_string(&mut input).expect("read stdin");
    } else {
        input = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {path}: {e}"));
    }

    let mut diagnostics = String::new();
    match check_corpus(&input, &mut diagnostics) {
        Ok(checked) => {
            eprint!("{diagnostics}");
            println!("ok: {checked} FEEL cases agree with the Lean reference");
        }
        Err(message) => {
            eprint!("{diagnostics}");
            eprintln!("FAIL: {message}");
            std::process::exit(1);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::check_corpus;

    fn run(input: &str) -> Result<usize, String> {
        check_corpus(input, &mut String::new())
    }

    #[test]
    fn empty_corpus_is_rejected() {
        // Regression: an empty (or truncated) corpus must NOT pass the gate — a
        // crashed `feelfuzz` would otherwise leave zero checked cases and a
        // false green (Copilot review of PR #1253).
        let err = run("").expect_err("empty corpus must fail");
        assert!(err.contains("no FEEL cases"), "unexpected message: {err}");
    }

    #[test]
    fn all_blank_corpus_is_rejected() {
        let err = run("\n\n\n").expect_err("all-blank corpus must fail");
        assert!(err.contains("no FEEL cases"), "unexpected message: {err}");
    }

    #[test]
    fn agreeing_row_passes() {
        // `1 + 1` evaluates to num:2 with an empty context.
        let checked = run("1 + 1\t\tok:num:2").expect("agreeing row must pass");
        assert_eq!(checked, 1);
    }

    #[test]
    fn diverging_row_fails() {
        let err = run("1 + 1\t\tok:num:3").expect_err("diverging row must fail");
        assert!(err.contains("divergence"), "unexpected message: {err}");
    }

    #[test]
    fn malformed_row_fails() {
        let err = run("a\tb\tc\td").expect_err("row with too many fields must fail");
        assert!(
            err.contains("divergence") || err.contains("over"),
            "unexpected message: {err}"
        );
    }

    #[test]
    fn malformed_bool_payload_is_rejected() {
        // Regression: a `bool:` payload that is neither `true` nor `false` is a
        // malformed row and must fail the gate, not be coerced to `false`
        // (Copilot review of PR #1253).
        let mut diags = String::new();
        let err = check_corpus("x\tv=bool:garbage\tok:bool:false", &mut diags)
            .expect_err("malformed bool must fail");
        assert!(
            err.contains("divergence") || err.contains("over"),
            "unexpected message: {err}"
        );
        assert!(
            diags.contains("bad bool"),
            "unexpected diagnostics: {diags}"
        );
    }

    #[test]
    fn bool_false_payload_is_accepted() {
        // `false` must still parse and drive an agreeing check.
        let checked = run("v\tv=bool:false\tok:bool:false").expect("bool:false must parse");
        assert_eq!(checked, 1);
    }

    #[test]
    fn blank_expression_row_is_rejected() {
        // Regression: a truncated/malformed row with a blank expression must NOT
        // pass the gate. `feel::eval("")` returns `Err`, so a `\t\terr` row would
        // otherwise match `expected == "err"`, increment `checked`, and satisfy
        // the row-count gate without evaluating a generated expression (Copilot
        // review of PR #1253).
        let mut diags = String::new();
        let err = check_corpus("\t\terr", &mut diags).expect_err("blank expression must fail");
        assert!(
            err.contains("over") || err.contains("no FEEL cases"),
            "unexpected message: {err}"
        );
        assert!(
            diags.contains("blank expression"),
            "unexpected diagnostics: {diags}"
        );
    }

    #[test]
    fn whitespace_only_expression_row_is_rejected() {
        // A whitespace-only expression is likewise not a real generated case.
        let mut diags = String::new();
        let err =
            check_corpus("   \t\terr", &mut diags).expect_err("whitespace expression must fail");
        assert!(
            diags.contains("blank expression"),
            "unexpected diagnostics: {diags}"
        );
        let _ = err;
    }
}
