//! The Decision Requirements Graph (DRG) model: the in-memory representation of
//! a parsed DMN resource.
//!
//! This mirrors the subset of DMN 1.3 that Nano evaluates natively — decision
//! tables (with every standard hit policy) and literal expressions, wired
//! together by information requirements into a decision requirements graph.

use crate::model::Value;

/// A parsed DMN resource: one or more [`Decision`]s and their requirements.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct DecisionRequirementsGraph {
    /// The `id` of the `<definitions>` element.
    pub id: String,
    /// The `name` of the `<definitions>` element.
    pub name: String,
    /// The `namespace` of the `<definitions>` element.
    pub namespace: String,
    /// Every decision declared in the resource, in document order.
    pub decisions: Vec<Decision>,
    /// The raw DMN XML the graph was parsed from, retained so the read model can
    /// serve it back via `getDecisionDefinitionXml`/`getDecisionRequirementsXml`.
    /// Defaulted to empty so journals/events written before this field existed
    /// (and hand-built test graphs) still deserialize.
    #[cfg_attr(feature = "serde", serde(default))]
    pub xml: String,
}

impl DecisionRequirementsGraph {
    /// Finds a decision by its `id`.
    pub fn decision(&self, id: &str) -> Option<&Decision> {
        self.decisions.iter().find(|d| d.id == id)
    }
}

/// A single DMN decision and its decision logic.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Decision {
    pub id: String,
    pub name: String,
    /// The name of the decision's output variable (`<decision><variable name=…>`).
    /// When absent, the decision's result is bound under its `id`.
    pub variable_name: Option<String>,
    /// Ids of decisions this decision requires (`informationRequirement` /
    /// `requiredDecision href="#id"`). Evaluated before this decision.
    pub required_decisions: Vec<String>,
    pub logic: DecisionLogic,
}

impl Decision {
    /// The name under which this decision's result is bound in the evaluation
    /// context for decisions that require it.
    pub fn result_name(&self) -> &str {
        self.variable_name.as_deref().unwrap_or(&self.id)
    }

    /// The decision's type, derived from its logic (an unsupported logic
    /// element reports the type its local name names).
    pub fn decision_type(&self) -> DecisionType {
        match &self.logic {
            DecisionLogic::DecisionTable(_) => DecisionType::DecisionTable,
            DecisionLogic::LiteralExpression(_) => DecisionType::LiteralExpression,
            DecisionLogic::Unsupported(kind) => match kind.as_str() {
                "context" => DecisionType::Context,
                "invocation" => DecisionType::Invocation,
                "list" => DecisionType::List,
                "relation" => DecisionType::Relation,
                _ => DecisionType::Unknown,
            },
        }
    }
}

/// The decision logic of a [`Decision`].
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum DecisionLogic {
    DecisionTable(DecisionTable),
    /// A FEEL literal expression.
    LiteralExpression(String),
    /// A decision type Nano does not yet evaluate natively (context, invocation,
    /// list, relation). Carries the element's local name for diagnostics.
    Unsupported(String),
}

/// A DMN decision table.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct DecisionTable {
    pub hit_policy: HitPolicy,
    pub aggregation: Option<Aggregation>,
    pub inputs: Vec<InputClause>,
    pub outputs: Vec<OutputClause>,
    pub rules: Vec<DecisionRule>,
}

/// An input column of a decision table.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct InputClause {
    pub id: String,
    pub label: Option<String>,
    /// The FEEL input expression, evaluated once per decision-table evaluation.
    pub expression: String,
    pub type_ref: Option<String>,
}

/// An output column of a decision table.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct OutputClause {
    pub id: String,
    pub label: Option<String>,
    /// The output name. Used as the key when a table has multiple outputs.
    pub name: Option<String>,
    pub type_ref: Option<String>,
    /// The `<outputValues>` list (comma-separated FEEL values), which defines the
    /// priority order for the `PRIORITY` and `OUTPUT ORDER` hit policies.
    pub output_values: Vec<String>,
}

/// A single rule (row) of a decision table.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct DecisionRule {
    pub id: String,
    /// One unary-test entry per input column, in column order.
    pub input_entries: Vec<String>,
    /// One FEEL output entry per output column, in column order.
    pub output_entries: Vec<String>,
}

/// A DMN decision-table hit policy.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum HitPolicy {
    /// `U` — at most one rule may match (the default).
    Unique,
    /// `F` — the first matching rule (in rule order) wins.
    First,
    /// `P` — the matching rule with the highest output priority wins.
    Priority,
    /// `A` — any number may match but all must produce the same output.
    Any,
    /// `C` — all matching rules, as a list (optionally aggregated).
    Collect,
    /// `R` — all matching rules, as a list in rule order.
    RuleOrder,
    /// `O` — all matching rules, as a list in output-priority order.
    OutputOrder,
}

impl HitPolicy {
    /// Parses the DMN `hitPolicy` attribute. Absent/unknown maps to `UNIQUE`.
    pub fn parse(s: &str) -> HitPolicy {
        match s.trim().to_ascii_uppercase().as_str() {
            "FIRST" => HitPolicy::First,
            "PRIORITY" => HitPolicy::Priority,
            "ANY" => HitPolicy::Any,
            "COLLECT" => HitPolicy::Collect,
            "RULE ORDER" | "RULE_ORDER" | "RULEORDER" => HitPolicy::RuleOrder,
            "OUTPUT ORDER" | "OUTPUT_ORDER" | "OUTPUTORDER" => HitPolicy::OutputOrder,
            _ => HitPolicy::Unique,
        }
    }
}

/// The aggregator of a `COLLECT` hit policy.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Aggregation {
    Sum,
    Min,
    Max,
    Count,
}

impl Aggregation {
    /// Parses the DMN `aggregation` attribute of a `COLLECT` decision table.
    pub fn parse(s: &str) -> Option<Aggregation> {
        match s.trim().to_ascii_uppercase().as_str() {
            "SUM" => Some(Aggregation::Sum),
            "MIN" => Some(Aggregation::Min),
            "MAX" => Some(Aggregation::Max),
            "COUNT" => Some(Aggregation::Count),
            _ => None,
        }
    }
}

/// The type of decision logic, mirroring Zeebe's `DecisionType`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum DecisionType {
    DecisionTable,
    LiteralExpression,
    Context,
    Invocation,
    List,
    Relation,
    Unknown,
}

/// The result of evaluating a decision in a [`DecisionRequirementsGraph`].
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct DecisionEvaluationResult {
    /// The id of the root decision that was requested.
    pub decision_id: String,
    /// The output of the root decision, or [`Value::Null`] on failure.
    pub decision_output: Value,
    /// Details of every decision evaluated, in evaluation order (required
    /// decisions first, root decision last). On failure it contains the
    /// successfully evaluated decisions plus the one that failed.
    pub evaluated_decisions: Vec<EvaluatedDecision>,
    /// The failure, if evaluation did not succeed.
    pub failure: Option<EvaluationFailure>,
}

impl DecisionEvaluationResult {
    /// Whether evaluation failed.
    pub fn is_failure(&self) -> bool {
        self.failure.is_some()
    }
}

/// A decision-evaluation failure.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct EvaluationFailure {
    pub message: String,
    pub failed_decision_id: String,
}

/// Details of one evaluated decision (for audit / exporter records).
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct EvaluatedDecision {
    pub decision_id: String,
    pub decision_name: String,
    pub decision_type: DecisionType,
    /// The output of this decision, or [`Value::Null`] if it was not evaluated
    /// successfully.
    pub decision_output: Value,
    /// The evaluated inputs (decision tables only; empty otherwise).
    pub evaluated_inputs: Vec<EvaluatedInput>,
    /// The matched rules (decision tables only; empty otherwise).
    pub matched_rules: Vec<MatchedRule>,
    /// Key of the exact deployed definition evaluated (Zeebe's per-evaluated-
    /// decision `decisionKey`), resolved by the engine within the evaluated DRG
    /// version. `0` straight out of [`crate::dmn::evaluate`] (which knows no
    /// deployments) and in journals written before it was recorded (#1292).
    #[cfg_attr(feature = "serde", serde(default))]
    pub decision_key: crate::Key,
    /// Version of that definition (Zeebe's `decisionVersion`); `0` when unknown,
    /// as for [`EvaluatedDecision::decision_key`].
    #[cfg_attr(feature = "serde", serde(default))]
    pub decision_version: i32,
}

/// The id of the `n`-th (1-based, evaluation order) decision evaluated by the
/// decision evaluation `decision_evaluation_key` — Zeebe's
/// `decisionEvaluationInstanceKey` (`DecisionEvaluationInstanceKeyGenerator`:
/// `"{decisionEvaluationKey}-{n}"`). The one canonical spelling of a
/// decision-instance id, shared by the read model, the REST gateway and wasm.
pub fn decision_evaluation_instance_key(decision_evaluation_key: crate::Key, n: usize) -> String {
    format!("{decision_evaluation_key}-{n}")
}

/// An evaluated decision-table input.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct EvaluatedInput {
    pub input_id: String,
    pub input_name: Option<String>,
    pub input_value: Value,
}

/// A matched decision-table rule and its evaluated outputs.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct MatchedRule {
    pub rule_id: String,
    /// The 1-based index of the rule in the decision table (rule order).
    pub rule_index: usize,
    pub evaluated_outputs: Vec<EvaluatedOutput>,
}

/// An evaluated decision-table output.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct EvaluatedOutput {
    pub output_id: String,
    pub output_name: Option<String>,
    pub output_value: Value,
}
