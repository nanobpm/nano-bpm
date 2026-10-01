//! # Native DMN decision engine
//!
//! Parses and evaluates [DMN 1.3](https://www.omg.org/spec/DMN) decisions
//! natively in Rust, on top of Nano's [`crate::feel`] engine — no Scala, no JVM,
//! zero external dependencies, so it compiles unchanged for servers and
//! `wasm32`.
//!
//! A DMN resource is parsed into a [`DecisionRequirementsGraph`] (DRG) of one or
//! more [`Decision`]s wired by their information requirements. A decision is then
//! evaluated by id against a variable context, evaluating any required decisions
//! first:
//!
//! ```
//! use std::collections::HashMap;
//! use nanobpmn_engine_core::Value;
//! use nanobpmn_engine_core::dmn;
//!
//! let xml = r#"<definitions xmlns="https://www.omg.org/spec/DMN/20191111/MODEL/" id="d" name="d">
//!   <decision id="greeting" name="Greeting">
//!     <decisionTable>
//!       <input id="i1"><inputExpression id="e1" typeRef="string"><text>lang</text></inputExpression></input>
//!       <output id="o1" typeRef="string" />
//!       <rule id="r1"><inputEntry id="ie1"><text>"en"</text></inputEntry>
//!         <outputEntry id="oe1"><text>"hello"</text></outputEntry></rule>
//!       <rule id="r2"><inputEntry id="ie2"><text>"de"</text></inputEntry>
//!         <outputEntry id="oe2"><text>"hallo"</text></outputEntry></rule>
//!     </decisionTable>
//!   </decision>
//! </definitions>"#;
//!
//! let drg = dmn::parse_dmn(xml).unwrap();
//! let ctx = HashMap::from([("lang".to_string(), Value::Str("de".into()))]);
//! let result = dmn::evaluate(&drg, "greeting", &ctx);
//! assert!(!result.is_failure());
//! assert_eq!(result.decision_output, Value::Str("hallo".into()));
//! ```
//!
//! The engine mirrors Zeebe's DMN decision-evaluation surface (hit policies,
//! required decisions, evaluated inputs/matched rules) so its
//! [`DecisionEvaluationResult`] can drive `businessRuleTask` execution, a
//! standalone `EvaluateDecision` API and decision-evaluation exporter records.
//!
//! Signed for **Sebastian Menski**, creator of Camunda's `engine-dmn`.

mod eval;
pub mod model;
mod parser;

#[cfg(test)]
mod tests;

pub use eval::evaluate;
pub use model::decision_evaluation_instance_key;
pub use model::{
    Aggregation, Decision, DecisionEvaluationResult, DecisionLogic, DecisionRequirementsGraph,
    DecisionRule, DecisionTable, DecisionType, EvaluatedDecision, EvaluatedInput, EvaluatedOutput,
    EvaluationFailure, HitPolicy, InputClause, MatchedRule, OutputClause,
};
pub use parser::{parse_dmn, DmnParseError};
