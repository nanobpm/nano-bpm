use std::collections::HashMap;

use crate::dmn::model::{DecisionLogic, DecisionType, HitPolicy};
use crate::dmn::{evaluate, parse_dmn};
use crate::model::Value;

fn ctx(pairs: &[(&str, Value)]) -> HashMap<String, Value> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.clone()))
        .collect()
}

/// A decision requirements graph with a dependent decision, ported from Zeebe's
/// `drg-force-user.dmn` fixture.
const FORCE_USER: &str = r##"<?xml version="1.0" encoding="UTF-8"?>
<definitions xmlns="https://www.omg.org/spec/DMN/20191111/MODEL/" id="force_users" name="force_users" namespace="http://camunda.org/schema/1.0/dmn">
  <decision id="jedi_or_sith" name="Jedi or Sith">
    <decisionTable id="DecisionTable_14n3bxx">
      <input id="Input_1" label="Lightsaber color">
        <inputExpression id="InputExpression_1" typeRef="string">
          <text>lightsaberColor</text>
        </inputExpression>
      </input>
      <output id="Output_1" label="Jedi or Sith" name="jedi_or_sith" typeRef="string">
        <outputValues id="UnaryTests_0hj346a">
          <text>"Jedi","Sith"</text>
        </outputValues>
      </output>
      <rule id="r1"><inputEntry id="ie1"><text>"blue"</text></inputEntry><outputEntry id="oe1"><text>"Jedi"</text></outputEntry></rule>
      <rule id="r2"><inputEntry id="ie2"><text>"green"</text></inputEntry><outputEntry id="oe2"><text>"Jedi"</text></outputEntry></rule>
      <rule id="r3"><inputEntry id="ie3"><text>"red"</text></inputEntry><outputEntry id="oe3"><text>"Sith"</text></outputEntry></rule>
    </decisionTable>
  </decision>
  <decision id="force_user" name="Which force user?">
    <informationRequirement id="ir1">
      <requiredDecision href="#jedi_or_sith" />
    </informationRequirement>
    <decisionTable id="DecisionTable_07g94t1" hitPolicy="FIRST">
      <input id="InputClause_0qnqj25" label="Jedi or Sith">
        <inputExpression id="LiteralExpression_00lcyt5" typeRef="string"><text>jedi_or_sith</text></inputExpression>
      </input>
      <input id="InputClause_0k64hys" label="Body height">
        <inputExpression id="LiteralExpression_0ib6fnk" typeRef="number"><text>height</text></inputExpression>
      </input>
      <output id="OutputClause_0hhe1yo" label="Force user" name="force_user" typeRef="string" />
      <rule id="fr1"><inputEntry id="fie1"><text>"Jedi"</text></inputEntry><inputEntry id="fie1b"><text>&gt; 190</text></inputEntry><outputEntry id="foe1"><text>"Mace Windu"</text></outputEntry></rule>
      <rule id="fr2"><inputEntry id="fie2"><text>"Jedi"</text></inputEntry><inputEntry id="fie2b"><text>&gt; 180</text></inputEntry><outputEntry id="foe2"><text>"Obi-Wan Kenobi"</text></outputEntry></rule>
      <rule id="fr3"><inputEntry id="fie3"><text>"Jedi"</text></inputEntry><inputEntry id="fie3b"><text>&lt; 70</text></inputEntry><outputEntry id="foe3"><text>"Yoda"</text></outputEntry></rule>
      <rule id="fr4"><inputEntry id="fie4"><text>"Sith"</text></inputEntry><inputEntry id="fie4b"><text>&gt; 200</text></inputEntry><outputEntry id="foe4"><text>"Darth Vader"</text></outputEntry></rule>
      <rule id="fr5"><inputEntry id="fie5"><text>"Sith"</text></inputEntry><inputEntry id="fie5b"><text>&gt; 170</text></inputEntry><outputEntry id="foe5"><text>"Darth Sidius"</text></outputEntry></rule>
      <rule id="fr6"><inputEntry id="fie6"><text></text></inputEntry><inputEntry id="fie6b"><text></text></inputEntry><outputEntry id="foe6"><text>"unknown"</text></outputEntry></rule>
    </decisionTable>
  </decision>
</definitions>"##;

#[test]
fn parses_drg_structure() {
    let drg = parse_dmn(FORCE_USER).unwrap();
    assert_eq!(drg.id, "force_users");
    assert_eq!(drg.decisions.len(), 2);
    let force_user = drg.decision("force_user").unwrap();
    assert_eq!(
        force_user.required_decisions,
        vec!["jedi_or_sith".to_string()]
    );
    match &force_user.logic {
        DecisionLogic::DecisionTable(t) => {
            assert_eq!(t.hit_policy, HitPolicy::First);
            assert_eq!(t.inputs.len(), 2);
            assert_eq!(t.rules.len(), 6);
        }
        _ => panic!("expected a decision table"),
    }
}

#[test]
fn evaluates_single_decision() {
    let drg = parse_dmn(FORCE_USER).unwrap();
    let result = evaluate(
        &drg,
        "jedi_or_sith",
        &ctx(&[("lightsaberColor", Value::Str("red".into()))]),
    );
    assert!(!result.is_failure(), "{:?}", result.failure);
    assert_eq!(result.decision_output, Value::Str("Sith".into()));
}

#[test]
fn evaluates_required_decision_chain() {
    let drg = parse_dmn(FORCE_USER).unwrap();
    let result = evaluate(
        &drg,
        "force_user",
        &ctx(&[
            ("lightsaberColor", Value::Str("blue".into())),
            ("height", Value::Int(195)),
        ]),
    );
    assert!(!result.is_failure(), "{:?}", result.failure);
    // blue -> Jedi, height 195 > 190 -> first rule -> Mace Windu.
    assert_eq!(result.decision_output, Value::Str("Mace Windu".into()));
    // The required decision is evaluated first, the root last.
    assert_eq!(result.evaluated_decisions.len(), 2);
    assert_eq!(result.evaluated_decisions[0].decision_id, "jedi_or_sith");
    assert_eq!(result.evaluated_decisions[1].decision_id, "force_user");
    // Matched rule + evaluated inputs recorded for audit parity.
    let root = &result.evaluated_decisions[1];
    assert_eq!(root.decision_type, DecisionType::DecisionTable);
    assert_eq!(root.matched_rules.len(), 1);
    assert_eq!(root.matched_rules[0].rule_id, "fr1");
    assert_eq!(
        root.evaluated_inputs[0].input_value,
        Value::Str("Jedi".into())
    );
}

#[test]
fn first_hit_policy_takes_earliest_rule() {
    let drg = parse_dmn(FORCE_USER).unwrap();
    // Jedi at 185 matches both `> 180` (Obi-Wan) rule; FIRST wins that.
    let result = evaluate(
        &drg,
        "force_user",
        &ctx(&[
            ("lightsaberColor", Value::Str("green".into())),
            ("height", Value::Int(185)),
        ]),
    );
    assert_eq!(result.decision_output, Value::Str("Obi-Wan Kenobi".into()));
}

#[test]
fn empty_input_entry_matches_anything() {
    let drg = parse_dmn(FORCE_USER).unwrap();
    // A Jedi of height 100 matches no specific rule, falling to the catch-all.
    let result = evaluate(
        &drg,
        "force_user",
        &ctx(&[
            ("lightsaberColor", Value::Str("blue".into())),
            ("height", Value::Int(100)),
        ]),
    );
    assert_eq!(result.decision_output, Value::Str("unknown".into()));
}

fn single_input_table(hit_policy: &str, rules: &str, output_values: &str) -> String {
    format!(
        r#"<definitions xmlns="https://www.omg.org/spec/DMN/20191111/MODEL/" id="d" name="d">
  <decision id="dec" name="dec">
    <decisionTable hitPolicy="{hit_policy}">
      <input id="i1"><inputExpression id="e1" typeRef="number"><text>score</text></inputExpression></input>
      <output id="o1" name="grade" typeRef="string">{output_values}</output>
      {rules}
    </decisionTable>
  </decision>
</definitions>"#
    )
}

fn rule(input: &str, output: &str) -> String {
    format!("<rule id=\"r\"><inputEntry id=\"ie\"><text>{input}</text></inputEntry><outputEntry id=\"oe\"><text>{output}</text></outputEntry></rule>")
}

#[test]
fn unique_hit_policy_rejects_overlap() {
    let rules = format!("{}{}", rule("&gt; 5", "\"a\""), rule("&gt; 1", "\"b\""));
    let drg = parse_dmn(&single_input_table("UNIQUE", &rules, "")).unwrap();
    let result = evaluate(&drg, "dec", &ctx(&[("score", Value::Int(10))]));
    assert!(result.is_failure());
}

#[test]
fn collect_hit_policy_returns_list() {
    let rules = format!("{}{}", rule("&gt; 5", "\"a\""), rule("&gt; 1", "\"b\""));
    let drg = parse_dmn(&single_input_table("COLLECT", &rules, "")).unwrap();
    let result = evaluate(&drg, "dec", &ctx(&[("score", Value::Int(10))]));
    assert_eq!(
        result.decision_output,
        Value::List(vec![Value::Str("a".into()), Value::Str("b".into())])
    );
}

#[test]
fn collect_sum_aggregates() {
    let numeric = r#"<definitions xmlns="https://www.omg.org/spec/DMN/20191111/MODEL/" id="d" name="d">
  <decision id="dec" name="dec">
    <decisionTable hitPolicy="COLLECT" aggregation="SUM">
      <input id="i1"><inputExpression id="e1" typeRef="number"><text>score</text></inputExpression></input>
      <output id="o1" name="points" typeRef="number" />
      <rule id="r1"><inputEntry id="ie1"><text>&gt; 5</text></inputEntry><outputEntry id="oe1"><text>10</text></outputEntry></rule>
      <rule id="r2"><inputEntry id="ie2"><text>&gt; 1</text></inputEntry><outputEntry id="oe2"><text>5</text></outputEntry></rule>
    </decisionTable>
  </decision>
</definitions>"#;
    let drg = parse_dmn(numeric).unwrap();
    let result = evaluate(&drg, "dec", &ctx(&[("score", Value::Int(10))]));
    assert_eq!(result.decision_output, Value::Int(15));
}

#[test]
fn priority_hit_policy_uses_output_order() {
    let rules = format!(
        "{}{}",
        rule("&gt; 5", "\"low\""),
        rule("&gt; 1", "\"high\"")
    );
    // Priority: "high" listed before "low" wins over both matches.
    let ov = "<outputValues id=\"ov\"><text>\"high\",\"low\"</text></outputValues>";
    let drg = parse_dmn(&single_input_table("PRIORITY", &rules, ov)).unwrap();
    let result = evaluate(&drg, "dec", &ctx(&[("score", Value::Int(10))]));
    assert_eq!(result.decision_output, Value::Str("high".into()));
}

#[test]
fn comma_list_and_not_and_interval_tests() {
    let numeric = r#"<definitions xmlns="https://www.omg.org/spec/DMN/20191111/MODEL/" id="d" name="d">
  <decision id="dec" name="dec">
    <decisionTable hitPolicy="COLLECT">
      <input id="i1"><inputExpression id="e1" typeRef="number"><text>n</text></inputExpression></input>
      <output id="o1" name="tag" typeRef="string" />
      <rule id="r1"><inputEntry id="ie1"><text>1,2,3</text></inputEntry><outputEntry id="oe1"><text>"small-set"</text></outputEntry></rule>
      <rule id="r2"><inputEntry id="ie2"><text>[1..10]</text></inputEntry><outputEntry id="oe2"><text>"in-range"</text></outputEntry></rule>
      <rule id="r3"><inputEntry id="ie3"><text>not(4,5)</text></inputEntry><outputEntry id="oe3"><text>"not-four-or-five"</text></outputEntry></rule>
    </decisionTable>
  </decision>
</definitions>"#;
    let drg = parse_dmn(numeric).unwrap();
    let result = evaluate(&drg, "dec", &ctx(&[("n", Value::Int(2))]));
    assert_eq!(
        result.decision_output,
        Value::List(vec![
            Value::Str("small-set".into()),
            Value::Str("in-range".into()),
            Value::Str("not-four-or-five".into()),
        ])
    );
}

#[test]
fn multiple_outputs_produce_context() {
    let xml = r#"<definitions xmlns="https://www.omg.org/spec/DMN/20191111/MODEL/" id="d" name="d">
  <decision id="dec" name="dec">
    <decisionTable>
      <input id="i1"><inputExpression id="e1" typeRef="string"><text>role</text></inputExpression></input>
      <output id="o1" name="greeting" typeRef="string" />
      <output id="o2" name="level" typeRef="number" />
      <rule id="r1"><inputEntry id="ie1"><text>"admin"</text></inputEntry>
        <outputEntry id="oe1"><text>"hi"</text></outputEntry><outputEntry id="oe2"><text>9</text></outputEntry></rule>
    </decisionTable>
  </decision>
</definitions>"#;
    let drg = parse_dmn(xml).unwrap();
    let result = evaluate(&drg, "dec", &ctx(&[("role", Value::Str("admin".into()))]));
    let mut expected = std::collections::BTreeMap::new();
    expected.insert("greeting".to_string(), Value::Str("hi".into()));
    expected.insert("level".to_string(), Value::Int(9));
    assert_eq!(result.decision_output, Value::Map(expected));
}

#[test]
fn literal_expression_decision() {
    let xml = r#"<definitions xmlns="https://www.omg.org/spec/DMN/20191111/MODEL/" id="d" name="d">
  <decision id="dec" name="dec">
    <variable name="total" />
    <literalExpression><text>a + b</text></literalExpression>
  </decision>
</definitions>"#;
    let drg = parse_dmn(xml).unwrap();
    let result = evaluate(
        &drg,
        "dec",
        &ctx(&[("a", Value::Int(2)), ("b", Value::Int(3))]),
    );
    assert_eq!(result.decision_output, Value::Int(5));
}

#[test]
fn unknown_decision_is_a_failure() {
    let drg = parse_dmn(FORCE_USER).unwrap();
    let result = evaluate(&drg, "does_not_exist", &ctx(&[]));
    assert!(result.is_failure());
    assert_eq!(result.failure.unwrap().failed_decision_id, "does_not_exist");
}

#[test]
fn non_dmn_document_is_rejected() {
    assert!(parse_dmn("<foo/>").is_err());
}

/// Defect class (#1305 review): every failed evaluation's audit trail must end
/// with the decision named by `failed_decision_id`, since the projection marks
/// the last evaluated decision FAILED.
fn assert_trail_ends_with_failed(result: &crate::dmn::model::DecisionEvaluationResult) {
    let failure = result.failure.as_ref().expect("evaluation must fail");
    let last = result
        .evaluated_decisions
        .last()
        .expect("a failed evaluation of an existing decision must audit it");
    assert_eq!(last.decision_id, failure.failed_decision_id);
    assert_eq!(last.decision_output, Value::Null);
}

fn literal(id: &str, requires: &[&str]) -> String {
    let reqs: String = requires
        .iter()
        .map(|r| format!(r##"<informationRequirement><requiredDecision href="#{r}" /></informationRequirement>"##))
        .collect();
    format!(
        r#"<decision id="{id}" name="{id}"><variable name="{id}" />{reqs}<literalExpression><text>1</text></literalExpression></decision>"#
    )
}

fn drg_of(decisions: &[String]) -> String {
    format!(
        r#"<definitions xmlns="https://www.omg.org/spec/DMN/20191111/MODEL/" id="d" name="d">{}</definitions>"#,
        decisions.concat()
    )
}

#[test]
fn unresolved_requirement_after_successful_sibling_audits_the_requiring_decision() {
    let xml = drg_of(&[literal("ok", &[]), literal("top", &["ok", "missing"])]);
    let drg = parse_dmn(&xml).unwrap();
    let result = evaluate(&drg, "top", &ctx(&[]));
    assert_trail_ends_with_failed(&result);
    assert_eq!(result.failure.unwrap().failed_decision_id, "top");
}

#[test]
fn requirement_cycle_audits_the_failed_decision() {
    let xml = drg_of(&[literal("a", &["b"]), literal("b", &["a"])]);
    let drg = parse_dmn(&xml).unwrap();
    let result = evaluate(&drg, "a", &ctx(&[]));
    assert_trail_ends_with_failed(&result);
}
