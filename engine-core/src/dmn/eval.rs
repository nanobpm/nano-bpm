//! Native evaluation of a [`DecisionRequirementsGraph`] on Nano's FEEL engine.
//!
//! Decision-table input entries are DMN *unary tests* — a distinct grammar from
//! full FEEL expressions (`> 190`, `"a","b"`, `[1..10]`, `not(…)`, `-`). They are
//! evaluated here on top of [`crate::feel`] against the value of each input
//! expression, honouring every standard hit policy (UNIQUE, FIRST, PRIORITY, ANY,
//! COLLECT with SUM/MIN/MAX/COUNT aggregation, RULE ORDER, OUTPUT ORDER).
//! Required decisions are evaluated first and their results bound into the
//! context, so a decision requirements graph evaluates as a whole.
//!
//! This is Nano's native DMN decision engine, signed for **Sebastian Menski**,
//! creator of Camunda's `engine-dmn`.

use std::collections::{HashMap, HashSet};

use super::model::{
    Aggregation, Decision, DecisionEvaluationResult, DecisionLogic, DecisionRequirementsGraph,
    DecisionTable, DecisionType, EvaluatedDecision, EvaluatedInput, EvaluatedOutput,
    EvaluationFailure, HitPolicy, MatchedRule, OutputClause,
};
use super::parser::split_top_level_commas;
use crate::feel;
use crate::model::Value;

/// The reserved context variable the input value is bound to while evaluating a
/// unary test (and the target of the DMN input marker `?`).
const INPUT_VAR: &str = "__dmn_input__";

/// Evaluates the decision `decision_id` in `drg` against the variable context
/// `input`, evaluating any required decisions first.
///
/// Never panics and never returns an `Err`: an unresolvable decision, an
/// expression error or a hit-policy violation is reported as a
/// [`DecisionEvaluationResult`] with a [`EvaluationFailure`].
pub fn evaluate(
    drg: &DecisionRequirementsGraph,
    decision_id: &str,
    input: &HashMap<String, Value>,
) -> DecisionEvaluationResult {
    let mut ctx = input.clone();
    let mut evaluated = Vec::new();
    let mut done = HashSet::new();
    let mut on_stack = HashSet::new();

    match eval_decision(
        drg,
        decision_id,
        &mut ctx,
        &mut evaluated,
        &mut done,
        &mut on_stack,
    ) {
        Ok(output) => DecisionEvaluationResult {
            decision_id: decision_id.to_string(),
            decision_output: output,
            evaluated_decisions: evaluated,
            failure: None,
        },
        Err(failure) => DecisionEvaluationResult {
            decision_id: decision_id.to_string(),
            decision_output: Value::Null,
            evaluated_decisions: evaluated,
            failure: Some(failure),
        },
    }
}

/// Recursively evaluates a decision and its requirements, binding each result
/// into `ctx` under the decision's result name. Appends an [`EvaluatedDecision`]
/// for every decision it evaluates (including a failing one).
fn eval_decision(
    drg: &DecisionRequirementsGraph,
    decision_id: &str,
    ctx: &mut HashMap<String, Value>,
    evaluated: &mut Vec<EvaluatedDecision>,
    done: &mut HashSet<String>,
    on_stack: &mut HashSet<String>,
) -> Result<Value, EvaluationFailure> {
    let decision = drg.decision(decision_id).ok_or_else(|| EvaluationFailure {
        message: format!("no decision found with id '{decision_id}'"),
        failed_decision_id: decision_id.to_string(),
    })?;

    if done.contains(decision_id) {
        // Already evaluated via another requirement path; its result is in ctx.
        return Ok(ctx
            .get(decision.result_name())
            .cloned()
            .unwrap_or(Value::Null));
    }
    if !on_stack.insert(decision_id.to_string()) {
        // Audit the decision that closes the cycle so the trail ends with the
        // failed decision (the invariant the FAILED-row projection relies on).
        evaluated.push(failed_entry(decision));
        return Err(EvaluationFailure {
            message: format!(
                "the decision requirements graph has a cycle involving decision '{decision_id}'"
            ),
            failed_decision_id: decision_id.to_string(),
        });
    }

    for required in &decision.required_decisions {
        if drg.decision(required).is_none() {
            // An unresolvable requirement fails the *requiring* decision, which
            // is audited as the last evaluated decision.
            on_stack.remove(decision_id);
            evaluated.push(failed_entry(decision));
            return Err(EvaluationFailure {
                message: format!(
                    "decision '{}' requires decision '{required}', which is not in the decision requirements graph",
                    decision.id
                ),
                failed_decision_id: decision.id.clone(),
            });
        }
        // A failing requirement has already audited itself as the last entry.
        eval_decision(drg, required, ctx, evaluated, done, on_stack)?;
    }

    let result = eval_logic(decision, ctx, evaluated);
    on_stack.remove(decision_id);

    match result {
        Ok(output) => {
            ctx.insert(decision.result_name().to_string(), output.clone());
            done.insert(decision_id.to_string());
            Ok(output)
        }
        Err(failure) => {
            // Audit the decision that failed as the last evaluated decision, as
            // Zeebe's DMN audit log does (its `failedDecisionId` is the last
            // evaluated decision, which the exporter marks FAILED). It has no
            // output; its partial inputs/rules are not retained.
            evaluated.push(failed_entry(decision));
            Err(failure)
        }
    }
}

/// The audit entry for a decision that failed: no output, no partial
/// inputs/rules.
fn failed_entry(decision: &Decision) -> EvaluatedDecision {
    EvaluatedDecision {
        decision_id: decision.id.clone(),
        decision_name: decision.name.clone(),
        decision_type: decision.decision_type(),
        decision_output: Value::Null,
        evaluated_inputs: Vec::new(),
        matched_rules: Vec::new(),
        decision_key: 0,
        decision_version: 0,
    }
}

/// Evaluates a single decision's logic (not its requirements), recording an
/// [`EvaluatedDecision`].
fn eval_logic(
    decision: &Decision,
    ctx: &HashMap<String, Value>,
    evaluated: &mut Vec<EvaluatedDecision>,
) -> Result<Value, EvaluationFailure> {
    match &decision.logic {
        DecisionLogic::LiteralExpression(text) => {
            let output = feel::eval(text, ctx).map_err(|e| EvaluationFailure {
                message: format!(
                    "failed to evaluate literal expression of decision '{}': {}",
                    decision.id, e.0
                ),
                failed_decision_id: decision.id.clone(),
            })?;
            evaluated.push(EvaluatedDecision {
                decision_id: decision.id.clone(),
                decision_name: decision.name.clone(),
                decision_type: DecisionType::LiteralExpression,
                decision_output: output.clone(),
                evaluated_inputs: Vec::new(),
                matched_rules: Vec::new(),
                decision_key: 0,
                decision_version: 0,
            });
            Ok(output)
        }
        DecisionLogic::DecisionTable(table) => eval_table(decision, table, ctx, evaluated),
        DecisionLogic::Unsupported(kind) => Err(EvaluationFailure {
            message: format!(
                "decision '{}' has an unsupported decision type '{}'",
                decision.id, kind
            ),
            failed_decision_id: decision.id.clone(),
        }),
    }
}

fn eval_table(
    decision: &Decision,
    table: &DecisionTable,
    ctx: &HashMap<String, Value>,
    evaluated: &mut Vec<EvaluatedDecision>,
) -> Result<Value, EvaluationFailure> {
    let fail = |message: String| EvaluationFailure {
        message,
        failed_decision_id: decision.id.clone(),
    };

    // 1. Evaluate the input expressions once.
    let mut evaluated_inputs = Vec::with_capacity(table.inputs.len());
    let mut input_values = Vec::with_capacity(table.inputs.len());
    for input in &table.inputs {
        let value = if input.expression.trim().is_empty() {
            Value::Null
        } else {
            feel::eval(&input.expression, ctx).map_err(|e| {
                fail(format!(
                    "failed to evaluate input expression '{}' of decision '{}': {}",
                    input.expression, decision.id, e.0
                ))
            })?
        };
        evaluated_inputs.push(EvaluatedInput {
            input_id: input.id.clone(),
            input_name: input.label.clone(),
            input_value: value.clone(),
        });
        input_values.push(value);
    }

    // 2. Find matching rules and evaluate their outputs.
    let mut matched: Vec<RuleMatch> = Vec::new();
    for (rule_index, rule) in table.rules.iter().enumerate() {
        let mut all_match = true;
        for (col, entry) in rule.input_entries.iter().enumerate() {
            let input_value = input_values.get(col).unwrap_or(&Value::Null);
            let matches = unary_test_matches(entry, input_value, ctx)
                .map_err(|e| fail(format!("in decision '{}': {}", decision.id, e)))?;
            if !matches {
                all_match = false;
                break;
            }
        }
        if !all_match {
            continue;
        }

        // Evaluate this rule's output entries.
        let mut per_output = Vec::with_capacity(table.outputs.len());
        let mut evaluated_outputs = Vec::with_capacity(table.outputs.len());
        for (i, output) in table.outputs.iter().enumerate() {
            let text = rule.output_entries.get(i).map(String::as_str).unwrap_or("");
            let value = if text.trim().is_empty() {
                Value::Null
            } else {
                feel::eval(text, ctx).map_err(|e| {
                    fail(format!(
                        "failed to evaluate output entry '{}' of decision '{}': {}",
                        text, decision.id, e.0
                    ))
                })?
            };
            evaluated_outputs.push(EvaluatedOutput {
                output_id: output.id.clone(),
                output_name: output.name.clone(),
                output_value: value.clone(),
            });
            per_output.push(value);
        }

        matched.push(RuleMatch {
            rule_id: rule.id.clone(),
            rule_index,
            row_output: row_value(&table.outputs, &per_output),
            per_output,
            evaluated_outputs,
        });
    }

    // 3. Apply the hit policy.
    let (output, selected) = apply_hit_policy(table, &matched, &fail)?;

    evaluated.push(EvaluatedDecision {
        decision_id: decision.id.clone(),
        decision_name: decision.name.clone(),
        decision_type: DecisionType::DecisionTable,
        decision_output: output.clone(),
        evaluated_inputs,
        matched_rules: selected
            .iter()
            .map(|&i| {
                let m = &matched[i];
                MatchedRule {
                    rule_id: m.rule_id.clone(),
                    rule_index: m.rule_index + 1,
                    evaluated_outputs: m.evaluated_outputs.clone(),
                }
            })
            .collect(),
        decision_key: 0,
        decision_version: 0,
    });

    Ok(output)
}

/// A rule that matched, with its evaluated outputs.
struct RuleMatch {
    rule_id: String,
    rule_index: usize,
    /// The rule's output value as a whole (scalar for single-output tables, a
    /// context for multi-output tables).
    row_output: Value,
    /// The evaluated value per output column (for aggregation / priority).
    per_output: Vec<Value>,
    evaluated_outputs: Vec<EvaluatedOutput>,
}

/// Builds a rule's output value: a scalar for a single-output table, or a context
/// keyed by output name for a multi-output table.
fn row_value(outputs: &[OutputClause], per_output: &[Value]) -> Value {
    if outputs.len() == 1 {
        per_output.first().cloned().unwrap_or(Value::Null)
    } else {
        let mut map = std::collections::BTreeMap::new();
        for (output, value) in outputs.iter().zip(per_output.iter()) {
            let key = output.name.clone().unwrap_or_else(|| output.id.clone());
            map.insert(key, value.clone());
        }
        Value::Map(map)
    }
}

/// Applies the hit policy, returning the decision-table output and the indices
/// (into `matched`) of the rules the policy selects, in result order.
fn apply_hit_policy(
    table: &DecisionTable,
    matched: &[RuleMatch],
    fail: &impl Fn(String) -> EvaluationFailure,
) -> Result<(Value, Vec<usize>), EvaluationFailure> {
    let all: Vec<usize> = (0..matched.len()).collect();
    match table.hit_policy {
        HitPolicy::Unique => {
            if matched.len() > 1 {
                return Err(fail(format!(
                    "hit policy UNIQUE requires at most one matching rule, but {} rules matched",
                    matched.len()
                )));
            }
            let output = matched
                .first()
                .map(|m| m.row_output.clone())
                .unwrap_or(Value::Null);
            Ok((output, all))
        }
        HitPolicy::First => {
            let output = matched
                .first()
                .map(|m| m.row_output.clone())
                .unwrap_or(Value::Null);
            let selected = if matched.is_empty() { vec![] } else { vec![0] };
            Ok((output, selected))
        }
        HitPolicy::Any => {
            if let Some(first) = matched.first() {
                for m in &matched[1..] {
                    if m.row_output != first.row_output {
                        return Err(fail(
                            "hit policy ANY requires all matching rules to produce the same output, \
                             but outputs differ"
                                .to_string(),
                        ));
                    }
                }
                Ok((first.row_output.clone(), vec![0]))
            } else {
                Ok((Value::Null, vec![]))
            }
        }
        HitPolicy::Priority => {
            let priorities = output_priorities(&table.outputs);
            let best = matched.iter().enumerate().min_by(|(_, a), (_, b)| {
                priority_key(&a.per_output, &priorities)
                    .cmp(&priority_key(&b.per_output, &priorities))
            });
            match best {
                Some((i, m)) => Ok((m.row_output.clone(), vec![i])),
                None => Ok((Value::Null, vec![])),
            }
        }
        HitPolicy::Collect => match table.aggregation {
            None => Ok((
                Value::List(matched.iter().map(|m| m.row_output.clone()).collect()),
                all,
            )),
            Some(agg) => Ok((aggregate(agg, matched, fail)?, all)),
        },
        HitPolicy::RuleOrder => Ok((
            Value::List(matched.iter().map(|m| m.row_output.clone()).collect()),
            all,
        )),
        HitPolicy::OutputOrder => {
            let priorities = output_priorities(&table.outputs);
            let mut order: Vec<usize> = all;
            order.sort_by(|&a, &b| {
                priority_key(&matched[a].per_output, &priorities)
                    .cmp(&priority_key(&matched[b].per_output, &priorities))
            });
            let list = Value::List(
                order
                    .iter()
                    .map(|&i| matched[i].row_output.clone())
                    .collect(),
            );
            Ok((list, order))
        }
    }
}

/// Applies a COLLECT aggregator to the matched rules' first output column.
fn aggregate(
    agg: Aggregation,
    matched: &[RuleMatch],
    fail: &impl Fn(String) -> EvaluationFailure,
) -> Result<Value, EvaluationFailure> {
    if agg == Aggregation::Count {
        return Ok(Value::Int(matched.len() as i64));
    }
    if matched.is_empty() {
        // SUM/MIN/MAX over no rows is null (Camunda semantics).
        return Ok(Value::Null);
    }
    let mut nums = Vec::with_capacity(matched.len());
    for m in matched {
        let v = m.per_output.first().unwrap_or(&Value::Null);
        match as_f64(v) {
            Some(n) => nums.push(n),
            None => {
                return Err(fail(format!(
                    "COLLECT aggregation requires numeric outputs, got {v:?}"
                )))
            }
        }
    }
    let result = match agg {
        Aggregation::Sum => nums.iter().sum(),
        Aggregation::Min => nums.iter().cloned().fold(f64::INFINITY, f64::min),
        Aggregation::Max => nums.iter().cloned().fold(f64::NEG_INFINITY, f64::max),
        Aggregation::Count => unreachable!(),
    };
    Ok(number_value(result))
}

/// The parsed `<outputValues>` of each output column, as [`Value`]s, defining
/// priority order (earlier = higher priority).
fn output_priorities(outputs: &[OutputClause]) -> Vec<Vec<Value>> {
    outputs
        .iter()
        .map(|o| {
            o.output_values
                .iter()
                .map(|t| feel::eval(t, &HashMap::new()).unwrap_or(Value::Null))
                .collect()
        })
        .collect()
}

/// A comparable priority key for a rule's outputs: the index of each output value
/// within its column's priority list (lower = higher priority; unknown = last).
fn priority_key(per_output: &[Value], priorities: &[Vec<Value>]) -> Vec<usize> {
    per_output
        .iter()
        .enumerate()
        .map(|(col, value)| {
            priorities
                .get(col)
                .and_then(|list| list.iter().position(|v| values_equal(v, value)))
                .unwrap_or(usize::MAX)
        })
        .collect()
}

/// Evaluates whether a decision-table input entry (a DMN unary test) matches the
/// input value.
fn unary_test_matches(
    entry: &str,
    input: &Value,
    ctx: &HashMap<String, Value>,
) -> Result<bool, String> {
    let t = entry.trim();
    // An empty entry or `-` is irrelevant: it always matches.
    if t.is_empty() || t == "-" {
        return Ok(true);
    }

    // `not( … )` negates a positive unary-test list.
    if let Some(inner) = strip_not(t) {
        return Ok(!positive_list_matches(inner, input, ctx)?);
    }

    positive_list_matches(t, input, ctx)
}

/// Matches a comma-separated positive unary-test list (the input satisfies it if
/// it satisfies *any* of the tests).
fn positive_list_matches(
    list: &str,
    input: &Value,
    ctx: &HashMap<String, Value>,
) -> Result<bool, String> {
    for test in split_top_level_commas(list) {
        if subtest_matches(&test, input, ctx)? {
            return Ok(true);
        }
    }
    Ok(false)
}

fn subtest_matches(
    test: &str,
    input: &Value,
    ctx: &HashMap<String, Value>,
) -> Result<bool, String> {
    let t = test.trim();
    if t.is_empty() {
        return Ok(true);
    }

    // A test referencing the input marker `?` is a boolean expression.
    if contains_input_marker(t) {
        let expr = substitute_input_marker(t);
        return eval_bool_with_input(&expr, input, ctx);
    }

    // Comparison endpoints: `< e`, `<= e`, `> e`, `>= e`.
    for op in ["<=", ">=", "<", ">"] {
        if let Some(rest) = t.strip_prefix(op) {
            let expr = format!("{INPUT_VAR} {op} ({})", rest.trim());
            return eval_bool_with_input(&expr, input, ctx);
        }
    }

    // Intervals: `[1..10]`, `(1..10)`, `]1..10[`, `[1..10)`, …
    if let Some((lo, lo_inc, hi, hi_inc)) = parse_interval(t) {
        let lo_op = if lo_inc { ">=" } else { ">" };
        let hi_op = if hi_inc { "<=" } else { "<" };
        let expr = format!("{INPUT_VAR} {lo_op} ({lo}) and {INPUT_VAR} {hi_op} ({hi})");
        return eval_bool_with_input(&expr, input, ctx);
    }

    // Otherwise it is a value expression: the input matches if it equals the value.
    let value = feel::eval(t, ctx).map_err(|e| e.0)?;
    Ok(values_equal(input, &value))
}

/// Evaluates a boolean FEEL expression with the input value bound to
/// [`INPUT_VAR`].
fn eval_bool_with_input(
    expr: &str,
    input: &Value,
    ctx: &HashMap<String, Value>,
) -> Result<bool, String> {
    let mut c = ctx.clone();
    c.insert(INPUT_VAR.to_string(), input.clone());
    feel::eval_bool(expr, &c).map_err(|e| e.0)
}

/// Strips a `not( … )` wrapper, returning its inner unary-test list.
fn strip_not(t: &str) -> Option<&str> {
    let rest = t.strip_prefix("not")?.trim_start();
    let inner = rest.strip_prefix('(')?.strip_suffix(')')?;
    Some(inner)
}

/// Parses an interval like `[1..10]` into `(low, low_inclusive, high,
/// high_inclusive)`. Returns `None` if `t` is not an interval.
fn parse_interval(t: &str) -> Option<(String, bool, String, bool)> {
    let bytes = t.as_bytes();
    let first = *bytes.first()?;
    let last = *bytes.last()?;
    let lo_inc = match first {
        b'[' => true,
        b'(' | b']' => false,
        _ => return None,
    };
    let hi_inc = match last {
        b']' => true,
        b')' | b'[' => false,
        _ => return None,
    };
    let inner = &t[1..t.len() - 1];
    let sep = inner.find("..")?;
    let lo = inner[..sep].trim().to_string();
    let hi = inner[sep + 2..].trim().to_string();
    if lo.is_empty() || hi.is_empty() {
        return None;
    }
    Some((lo, lo_inc, hi, hi_inc))
}

/// Whether `t` contains the DMN input marker `?` outside of a string literal.
fn contains_input_marker(t: &str) -> bool {
    let mut in_str = false;
    for c in t.chars() {
        match c {
            '"' => in_str = !in_str,
            '?' if !in_str => return true,
            _ => {}
        }
    }
    false
}

/// Replaces each unquoted `?` (the DMN input marker) with [`INPUT_VAR`].
fn substitute_input_marker(t: &str) -> String {
    let mut out = String::with_capacity(t.len());
    let mut in_str = false;
    for c in t.chars() {
        match c {
            '"' => {
                in_str = !in_str;
                out.push(c);
            }
            '?' if !in_str => out.push_str(INPUT_VAR),
            _ => out.push(c),
        }
    }
    out
}

/// Numeric-aware value equality (so `1` and `1.0` compare equal).
fn values_equal(a: &Value, b: &Value) -> bool {
    match (as_f64(a), as_f64(b)) {
        (Some(x), Some(y)) => x == y,
        _ => a == b,
    }
}

fn as_f64(v: &Value) -> Option<f64> {
    match v {
        Value::Int(i) => Some(*i as f64),
        Value::Double(d) => Some(*d),
        _ => None,
    }
}

/// Wraps an aggregation result as an [`Value::Int`] when it is whole, else a
/// [`Value::Double`].
fn number_value(n: f64) -> Value {
    if n.is_finite() && n.fract() == 0.0 && n.abs() < i64::MAX as f64 {
        Value::Int(n as i64)
    } else {
        Value::Double(n)
    }
}
