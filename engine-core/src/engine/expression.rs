//! The single expression-evaluation entry point for engine behaviours
//! (#1390, #1391 step 4).
//!
//! Modelled on Zeebe's `ExpressionProcessor`, whose every evaluation returns
//! `Either<Failure, T>`: an expression either yields a value of the expected
//! type, or a [`Failure`] carrying the Zeebe `ErrorType` the caller turns into
//! an incident. There is **no** silent fallback (literal text, default, `None`)
//! on an evaluation failure here — that was the defect class behind #1390,
//! where a failed job-type expression created a job whose type was the
//! expression text instead of raising `EXTRACT_VALUE_ERROR`.
//!
//! Callers that genuinely mirror a Zeebe fallback (e.g. Zeebe treats a `null`
//! assignee as "unassigned") must do so explicitly on the `Ok` value, never by
//! discarding the `Err`. The `feel_fallback_lint` test
//! (`engine-core/tests/feel_fallback_lint.rs`) rejects `.ok()` / `unwrap_or*`
//! applied to evaluation results anywhere under `src/engine`, except at
//! allow-listed sites citing the matching Zeebe behaviour.

use std::collections::HashMap;

use crate::model::Value;
use crate::state;

/// The Zeebe incident `ErrorType` of an expression [`Failure`]. Only the error
/// types an expression evaluation can raise are modelled.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ErrorType {
    /// `EXTRACT_VALUE_ERROR` — the expression failed to evaluate or yielded a
    /// value of the wrong type/shape (Zeebe `ExpressionProcessor` default).
    ExtractValueError,
}

impl ErrorType {
    /// The Zeebe enum constant name.
    #[cfg(test)]
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            ErrorType::ExtractValueError => "EXTRACT_VALUE_ERROR",
        }
    }

    /// The engine incident kind this error type is raised as.
    pub(crate) fn incident_kind(self) -> state::IncidentKind {
        match self {
            ErrorType::ExtractValueError => state::IncidentKind::ExtractValueError,
        }
    }
}

/// A failed expression evaluation (Zeebe `Failure`): the incident error type
/// plus a human-readable message (the incident's `errorMessage`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Failure {
    pub(crate) error_type: ErrorType,
    pub(crate) message: String,
    /// Set when the failure came from arming a TIMER boundary event: the
    /// boundary's element id. The engine uses it to give the incident a
    /// boundary-scoped retry (re-arm only that timer) instead of a whole-body
    /// re-drive that would duplicate the host's already-live job and other
    /// subscriptions (#1398 review).
    pub(crate) boundary_timer: Option<String>,
}

impl Failure {
    pub(crate) fn extract_value(message: impl Into<String>) -> Self {
        Failure {
            error_type: ErrorType::ExtractValueError,
            message: message.into(),
            boundary_timer: None,
        }
    }

    /// This failure, marked as raised while arming the timer of boundary event
    /// `boundary_element_id` (see [`Self::boundary_timer`]).
    pub(crate) fn with_boundary_timer(mut self, boundary_element_id: &str) -> Self {
        self.boundary_timer = Some(boundary_element_id.to_string());
        self
    }
}

pub(crate) use super::MAX_JOB_TYPE_LENGTH;

/// A successfully evaluated expression: the engine [`Value`] plus the Zeebe
/// `ResultType` name of the *native* FEEL result. The kind is authoritative
/// for type checks: temporal FEEL results render to `Value::Str` at the API
/// boundary but are `DATE`/`TIME`/`DATE_TIME`/`DURATION`/`PERIOD`, never
/// `STRING` (#1399 review).
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Evaluated {
    pub(crate) value: Value,
    pub(crate) kind: &'static str,
    /// Native kinds of the items when the result is an `ARRAY` (empty
    /// otherwise / for static values).
    pub(crate) item_kinds: Vec<&'static str>,
}

/// Whether `raw` is a FEEL expression (leading `=`) rather than a static value.
pub(crate) fn is_expression(raw: &str) -> bool {
    raw.trim_start().starts_with('=')
}

/// The single evaluation entry point. A FEEL expression (leading `=`) is
/// evaluated against `vars`; a static value is returned as a string (Zeebe
/// `StaticExpression`). A FEEL evaluation error is an `EXTRACT_VALUE_ERROR`
/// [`Failure`] (Zeebe `ExpressionProcessor.evaluateExpressionAsEither`).
pub(crate) fn evaluate_or_failure(
    raw: &str,
    vars: &HashMap<String, Value>,
) -> Result<Evaluated, Failure> {
    if !is_expression(raw) {
        return Ok(Evaluated {
            value: Value::Str(raw.to_string()),
            kind: "STRING",
            item_kinds: Vec::new(),
        });
    }
    crate::feel::eval_typed(raw.trim(), vars)
        .map(|(value, kind, item_kinds)| Evaluated {
            value,
            kind,
            item_kinds,
        })
        .map_err(|e| {
            Failure::extract_value(format!(
                "Expected to evaluate expression '{}', but failed: {}",
                raw.trim(),
                e.0
            ))
        })
}

fn type_failure(raw: &str, expected: &str, actual_kind: &str) -> Failure {
    Failure::extract_value(format!(
        "Expected result of the expression '{}' to be '{}', but was '{}'.",
        raw.trim(),
        expected,
        actual_kind
    ))
}

/// Zeebe `evaluateStringExpression`: the result must be a `STRING` (a `null`
/// result is a failure).
pub(crate) fn evaluate_string(raw: &str, vars: &HashMap<String, Value>) -> Result<String, Failure> {
    match evaluate_or_failure(raw, vars)? {
        Evaluated {
            value: Value::Str(s),
            kind: "STRING",
            ..
        } => Ok(s),
        other => Err(type_failure(raw, "STRING", other.kind)),
    }
}

/// Zeebe `BpmnJobBehavior.evalTypeExp`: a `STRING` that is non-empty and at
/// most `max_len` characters long.
pub(crate) fn evaluate_non_empty_string(
    raw: &str,
    vars: &HashMap<String, Value>,
    max_len: usize,
) -> Result<String, Failure> {
    let s = evaluate_string(raw, vars)?;
    if s.is_empty() {
        return Err(Failure::extract_value(format!(
            "Expected result of the expression '{}' to be a not-empty string, but was an empty string.",
            raw.trim()
        )));
    }
    let len = s.chars().count();
    if len > max_len {
        return Err(Failure::extract_value(format!(
            "Expected result of the expression '{}' to be a string with a maximum length of {}, but was a string with a length of {}.",
            raw.trim(),
            max_len,
            len
        )));
    }
    Ok(s)
}

/// Zeebe `evaluateLongExpression`: the result must be a `NUMBER` (truncated
/// to an integer, as Java's `Number.longValue`). A static value must parse
/// as a number.
pub(crate) fn evaluate_long(raw: &str, vars: &HashMap<String, Value>) -> Result<i64, Failure> {
    if !is_expression(raw) {
        let t = raw.trim();
        return t
            .parse::<i64>()
            .ok()
            .or_else(|| {
                t.parse::<f64>()
                    .ok()
                    .filter(|d| d.is_finite())
                    .map(|d| d as i64)
            })
            .ok_or(())
            .map_err(|_| type_failure(raw, "NUMBER", "STRING"));
    }
    match evaluate_or_failure(raw, vars)? {
        Evaluated {
            value: Value::Int(i),
            ..
        } => Ok(i),
        Evaluated {
            value: Value::Double(d),
            ..
        } if d.is_finite() => Ok(d as i64),
        other => Err(type_failure(raw, "NUMBER", other.kind)),
    }
}

/// Evaluates `expr` as FEEL **unconditionally** (a leading `=` is optional):
/// for model attributes that are always expressions — a multi-instance
/// `inputCollection` / `outputElement` / `completionCondition`, an ad-hoc
/// `activeElementsCollection` / `completionCondition` — which the parser may
/// store without the `=` marker (Zeebe parses them with
/// `ExpressionLanguage.parseExpression`, never as static values).
pub(crate) fn evaluate_feel(
    expr: &str,
    vars: &HashMap<String, Value>,
) -> Result<Evaluated, Failure> {
    let trimmed = expr.trim();
    if is_expression(trimmed) {
        evaluate_or_failure(trimmed, vars)
    } else {
        evaluate_or_failure(&format!("={trimmed}"), vars)
    }
}

/// Zeebe `evaluateBooleanExpression`: the result must be a `BOOLEAN` (a `null`
/// result is a failure — a completion condition is never "silently false").
pub(crate) fn evaluate_bool(expr: &str, vars: &HashMap<String, Value>) -> Result<bool, Failure> {
    match evaluate_feel(expr, vars)? {
        Evaluated {
            value: Value::Bool(b),
            ..
        } => Ok(b),
        other => Err(type_failure(expr, "BOOLEAN", other.kind)),
    }
}

/// Zeebe `evaluateArrayExpression`: the result must be an `ARRAY`.
pub(crate) fn evaluate_array(
    expr: &str,
    vars: &HashMap<String, Value>,
) -> Result<Vec<Value>, Failure> {
    match evaluate_feel(expr, vars)? {
        Evaluated {
            value: Value::List(items),
            ..
        } => Ok(items),
        other => Err(type_failure(expr, "ARRAY", other.kind)),
    }
}

/// Zeebe `evaluateArrayOfStringsExpression`: an `ARRAY` whose items are all
/// `STRING`s.
pub(crate) fn evaluate_array_of_strings(
    expr: &str,
    vars: &HashMap<String, Value>,
) -> Result<Vec<String>, Failure> {
    let evaluated = evaluate_feel(expr, vars)?;
    let Evaluated {
        value: Value::List(items),
        item_kinds,
        ..
    } = evaluated
    else {
        return Err(type_failure(expr, "ARRAY", evaluated.kind));
    };
    // Check the *native* item kinds: a temporal item renders to `Value::Str`
    // but is not a `STRING` (#1421 review).
    items
        .into_iter()
        .zip(item_kinds)
        .map(|(v, kind)| match (v, kind) {
            (Value::Str(s), "STRING") => Ok(s),
            _ => Err(Failure::extract_value(format!(
                "Expected result of the expression '{}' to be 'ARRAY' containing 'STRING' items, but was 'ARRAY' containing at least one non-'STRING' item.",
                expr.trim()
            ))),
        })
        .collect()
}

/// Zeebe `evaluateMessageCorrelationKeyExpression`: a `STRING` or a `NUMBER`
/// (rendered as its integer / decimal text); anything else is a failure.
pub(crate) fn evaluate_correlation_key(
    raw: &str,
    vars: &HashMap<String, Value>,
) -> Result<String, Failure> {
    // A correlation key is always FEEL (a bare `orderId` is a variable
    // reference), with or without the leading `=`.
    let evaluated = evaluate_feel(raw, vars)?;
    match (&evaluated.value, evaluated.kind) {
        // Only a native `STRING`: temporal results also render to
        // `Value::Str` but are not valid keys (#1421 review).
        (Value::Str(s), "STRING") => Ok(s.clone()),
        (Value::Int(i), _) => Ok(i.to_string()),
        // `f64` Display already omits the fraction for integral doubles and
        // never narrows (`1e20` stays `100000000000000000000`, no i64
        // saturation that would collapse distinct keys).
        (Value::Double(d), _) if d.is_finite() => Ok(d.to_string()),
        _ => Err(Failure::extract_value(format!(
            "Failed to extract the correlation key for '{}': The value must be either a string or a number, but was '{}'.",
            raw.trim(),
            evaluated.kind
        ))),
    }
}

/// Zeebe `evaluateDateTimeExpression(.., isNullable = true)` (user-task due /
/// follow-up dates): `null` is "no date"; a `DATE_TIME` or an ISO-8601
/// date-time `STRING` is the date; anything else is a failure.
pub(crate) fn evaluate_nullable_date_time(
    raw: &str,
    vars: &HashMap<String, Value>,
) -> Result<Option<String>, Failure> {
    if !is_expression(raw) {
        return Ok(Some(raw.to_string()).filter(|s| !s.trim().is_empty()));
    }
    let evaluated = evaluate_or_failure(raw, vars)?;
    match (evaluated.value, evaluated.kind) {
        // Only a NATIVE FEEL `null` is "no date": a `Function`/`Range` result
        // also converts to `Value::Null` at the API boundary but keeps the
        // kind `UNKNOWN` — accepting it here would silently drop a wrong-typed
        // expression instead of raising the incident (#1398 review).
        (Value::Null, "NULL") => Ok(None),
        (Value::Str(s), "DATE_TIME") => Ok(Some(s)),
        (Value::Str(s), "STRING") => {
            if crate::feel::temporal::DateTime::parse(s.trim()).is_some() {
                Ok(Some(s))
            } else {
                Err(Failure::extract_value(format!(
                    "Expected result of the expression '{}' to be a valid DateTime String, but was '{}'.",
                    raw.trim(),
                    s
                )))
            }
        }
        (_, kind) => Err(Failure::extract_value(format!(
            "Expected result of the expression '{}' to be one of '[DATE_TIME, STRING, NULL]', but was '{}'.",
            raw.trim(),
            kind
        ))),
    }
}

/// Zeebe `BpmnUserTaskBehavior.evaluatePriorityExpression`: an integral
/// `NUMBER` in `[0,100]`.
pub(crate) fn evaluate_user_task_priority(
    raw: &str,
    vars: &HashMap<String, Value>,
) -> Result<i32, Failure> {
    let n = evaluate_long(raw, vars)?;
    if !(0..=100).contains(&n) {
        return Err(Failure::extract_value(format!(
            "Expected priority to be in the range [0,100] but was '{n}'."
        )));
    }
    Ok(n as i32)
}

/// Zeebe `BpmnJobBehavior.evalPriorityExp`: an integral `NUMBER` (a decimal
/// with no fraction, e.g. `1.0`, is accepted), clamped into `i32`.
pub(crate) fn evaluate_job_priority(
    raw: &str,
    vars: &HashMap<String, Value>,
) -> Result<i32, Failure> {
    let integral = |d: f64| d.is_finite() && d.fract() == 0.0;
    let wrong = |kind: &str| {
        Failure::extract_value(format!(
            "Expected result of the expression '{}' for the job priority to be 'NUMBER' with an integer value, but was '{}'.",
            raw.trim(),
            kind
        ))
    };
    let n = if is_expression(raw) {
        match evaluate_or_failure(raw, vars)? {
            Evaluated {
                value: Value::Int(i),
                ..
            } => i,
            Evaluated {
                value: Value::Double(d),
                ..
            } if integral(d) => d as i64,
            other => return Err(wrong(other.kind)),
        }
    } else {
        raw.trim().parse::<i64>().map_err(|_| wrong("STRING"))?
    };
    Ok(n.clamp(i64::from(i32::MIN), i64::from(i32::MAX)) as i32)
}

/// A timer definition's text (Zeebe `CatchEventBehavior` timer evaluation): a
/// static value is used verbatim; an expression must yield a temporal or a
/// `STRING` (rendered as text); then `parse` must accept it. Either failure is
/// an `EXTRACT_VALUE_ERROR`.
pub(crate) fn evaluate_timer<T>(
    raw: &str,
    vars: &HashMap<String, Value>,
    what: &str,
    parse: impl FnOnce(&str) -> Option<T>,
) -> Result<T, Failure> {
    let text = if is_expression(raw) {
        match evaluate_or_failure(raw, vars)? {
            Evaluated {
                value: Value::Str(s),
                ..
            } => s,
            other => return Err(type_failure(raw, what, other.kind)),
        }
    } else {
        raw.to_string()
    };
    parse(text.trim()).ok_or_else(|| {
        Failure::extract_value(format!(
            "Expected result of the expression '{}' to be a valid {}, but was '{}'.",
            raw.trim(),
            what,
            text.trim()
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn evaluate_long_rejects_non_finite_static_values() {
        let v = HashMap::new();
        for lit in ["NaN", "inf", "-inf", "infinity"] {
            assert!(evaluate_long(lit, &v).is_err(), "{lit} must be rejected");
        }
        assert_eq!(evaluate_long("3.7", &v).unwrap(), 3);
    }

    fn vars(pairs: &[(&str, Value)]) -> HashMap<String, Value> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect()
    }

    #[test]
    fn correlation_key_rejects_temporals_and_never_narrows_doubles() {
        let v = vars(&[("big", Value::Double(1e20)), ("three", Value::Double(3.0))]);
        assert_eq!(
            evaluate_correlation_key("=big", &v).unwrap(),
            "100000000000000000000"
        );
        assert_ne!(
            evaluate_correlation_key("=big", &v).unwrap(),
            evaluate_correlation_key("=big * 2", &v).unwrap()
        );
        assert_eq!(evaluate_correlation_key("=three", &v).unwrap(), "3");
        for temporal in [
            r#"=date("2024-01-15")"#,
            r#"=time("10:00:00")"#,
            r#"=date and time("2024-01-15T10:00:00")"#,
            r#"=duration("PT1H")"#,
        ] {
            assert!(
                evaluate_correlation_key(temporal, &v).is_err(),
                "{temporal} must not be a correlation key"
            );
        }
    }

    #[test]
    fn array_of_strings_rejects_temporal_items() {
        let v = HashMap::new();
        assert_eq!(
            evaluate_array_of_strings(r#"=["a", "b"]"#, &v).unwrap(),
            vec!["a".to_string(), "b".to_string()]
        );
        for list in [
            r#"=[date("2024-01-15")]"#,
            r#"=["a", time("10:00:00")]"#,
            r#"=[duration("P1D")]"#,
            "=[1]",
        ] {
            let f = evaluate_array_of_strings(list, &v).unwrap_err();
            assert!(f.message.contains("non-'STRING' item"), "{list}: {f:?}");
        }
    }

    #[test]
    fn static_values_pass_through() {
        let v = HashMap::new();
        assert_eq!(evaluate_string("payment", &v).unwrap(), "payment");
        assert_eq!(evaluate_long(" 5 ", &v).unwrap(), 5);
        assert!(evaluate_long("five", &v).is_err());
    }

    #[test]
    fn string_checks() {
        let v = vars(&[("t", Value::Str("job".into())), ("n", Value::Int(42))]);
        assert_eq!(evaluate_string("=t", &v).unwrap(), "job");
        let f = evaluate_string("=n", &v).unwrap_err();
        assert_eq!(f.error_type.as_str(), "EXTRACT_VALUE_ERROR");
        assert!(
            f.message.contains("to be 'STRING', but was 'NUMBER'"),
            "{}",
            f.message
        );
        assert!(
            evaluate_string("=missing", &v).is_err(),
            "null is not a STRING"
        );
        assert!(evaluate_string("=\"a\" +", &v).is_err(), "parse error");
    }

    #[test]
    fn temporal_results_are_not_strings() {
        // #1399 review: FEEL temporals render to `Value::Str` at the API
        // boundary, but their native type is not STRING.
        let v = HashMap::new();
        for (expr, kind) in [
            ("=date(\"2024-01-15\")", "DATE"),
            ("=time(\"10:00:00\")", "TIME"),
            ("=date and time(\"2024-01-15T10:00:00\")", "DATE_TIME"),
            ("=duration(\"PT1H\")", "DURATION"),
            ("=duration(\"P1Y\")", "PERIOD"),
        ] {
            let f = evaluate_string(expr, &v).unwrap_err();
            assert!(
                f.message
                    .contains(&format!("to be 'STRING', but was '{kind}'")),
                "{expr}: {}",
                f.message
            );
            assert!(evaluate_long(expr, &v).is_err(), "{expr} is not a NUMBER");
        }
    }

    #[test]
    fn non_empty_and_max_length() {
        let v = vars(&[
            ("e", Value::Str(String::new())),
            ("l", Value::Str("abcd".into())),
        ]);
        assert!(evaluate_non_empty_string("=e", &v, 10)
            .unwrap_err()
            .message
            .contains("not-empty string"));
        assert!(evaluate_non_empty_string("=l", &v, 3)
            .unwrap_err()
            .message
            .contains("maximum length of 3"));
        assert_eq!(evaluate_non_empty_string("=l", &v, 4).unwrap(), "abcd");
    }

    #[test]
    fn number_checks() {
        let v = vars(&[("d", Value::Double(2.5)), ("s", Value::Str("x".into()))]);
        assert_eq!(evaluate_long("=d", &v).unwrap(), 2);
        assert_eq!(evaluate_long("=1 + 2", &v).unwrap(), 3);
        assert!(evaluate_long("=s", &v)
            .unwrap_err()
            .message
            .contains("to be 'NUMBER', but was 'STRING'"));
        assert!(
            evaluate_long("=missing", &v).is_err(),
            "null is not a NUMBER"
        );
    }

    #[test]
    fn evaluation_error_is_extract_value_failure() {
        let f = evaluate_or_failure("=1 +", &HashMap::new()).unwrap_err();
        assert_eq!(f.error_type, ErrorType::ExtractValueError);
        assert!(
            f.message
                .starts_with("Expected to evaluate expression '=1 +'"),
            "{}",
            f.message
        );
        assert_eq!(
            f.error_type.incident_kind(),
            state::IncidentKind::ExtractValueError
        );
    }
}
