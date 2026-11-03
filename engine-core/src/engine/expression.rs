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
            ErrorType::ExtractValueError => state::IncidentKind::ExpressionEvaluation,
        }
    }
}

/// A failed expression evaluation (Zeebe `Failure`): the incident error type
/// plus a human-readable message (the incident's `errorMessage`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Failure {
    pub(crate) error_type: ErrorType,
    pub(crate) message: String,
}

impl Failure {
    pub(crate) fn extract_value(message: impl Into<String>) -> Self {
        Failure {
            error_type: ErrorType::ExtractValueError,
            message: message.into(),
        }
    }
}

/// Zeebe's default `maxWorkerTypeLength` (`EngineConfiguration
/// .DEFAULT_MAX_WORKER_TYPE_LENGTH = 32 * 1024`), enforced on an evaluated job
/// type by `BpmnJobBehavior.evalTypeExp`.
pub(crate) const MAX_JOB_TYPE_LENGTH: usize = 32 * 1024;

/// A successfully evaluated expression: the engine [`Value`] plus the Zeebe
/// `ResultType` name of the *native* FEEL result. The kind is authoritative
/// for type checks: temporal FEEL results render to `Value::Str` at the API
/// boundary but are `DATE`/`TIME`/`DATE_TIME`/`DURATION`/`PERIOD`, never
/// `STRING` (#1399 review).
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Evaluated {
    pub(crate) value: Value,
    pub(crate) kind: &'static str,
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
        });
    }
    crate::feel::eval_typed(raw.trim(), vars)
        .map(|(value, kind)| Evaluated { value, kind })
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
            state::IncidentKind::ExpressionEvaluation
        );
    }
}
