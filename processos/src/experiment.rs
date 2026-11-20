//! **The Nano Alternate Reality Engine** — speculative execution of variant
//! hypotheses. Where the other tool surfaces *observe* the process, these tools let
//! the droid *act on a counterfactual*: fork the current model into a what-if variant,
//! re-run real recorded production instances through it on an in-process engine, and
//! measure what would have happened — a whole multiverse of "what-ifs?" scored against
//! the same ground truth.
//!
//! It is built directly on the deterministic replay harness:
//! - [`simulate`] replays one candidate model against the recorded dataset and returns
//!   its fidelity scorecard (boundary conservation, latency, coverage, divergences).
//! - [`compare_variants`] scores a *population* of candidates (optionally including the
//!   current model as the `baseline`) and ranks them fidelity-first — the multiverse,
//!   ranked.
//!
//! Replay needs **recorded-input** instances (Tier-2 capture, `c8 nano --capture`): the
//! creation variables plus the ordered stimulus log. A dataset without capture is not
//! replayable, and the tools say so plainly (with skip accounting) rather than
//! fabricating a result.

use std::collections::BTreeMap;

use serde_json::{json, Value};

use crate::dataset::TraceSource;
use crate::harness::{
    parse_mock_workers, rank_candidates_by_replay, CandidateModel, RecordedInstance,
};

/// Default cap on recorded instances pulled from a LIVE gateway per turn — each instance
/// is a network round-trip, so the live path stays bounded. Overridable via
/// `PROCESSOS_REPLAY_CAP`.
pub const LIVE_RECORDED_CAP: usize = 300;

/// Ceiling on instances replayed per turn for an IN-MEMORY dataset. The traces are already
/// loaded (reading them is free), but replay *executes* each instance on the engine, so this
/// bounds worst-case per-call cost. Mirrors the Insights full-analysis cap. Overridable via
/// `PROCESSOS_REPLAY_CAP`.
pub const REPLAY_CEILING: usize = 50_000;

/// How many recorded instances to distil for this source. An in-memory dataset knows its true
/// population and costs nothing to read, so replay the WHOLE population (bounded by the ceiling)
/// — the experiment then measures against the real dataset, not an arbitrary head slice. A live
/// gateway pays a request per instance, so it keeps the bounded default. Either may be overridden
/// with `PROCESSOS_REPLAY_CAP`.
pub fn recorded_cap(src: &TraceSource) -> usize {
    let env = std::env::var("PROCESSOS_REPLAY_CAP")
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .filter(|n| *n > 0);
    match src.total() {
        Some(total) => env.unwrap_or(REPLAY_CEILING).min(total.max(1)),
        None => env.unwrap_or(LIVE_RECORDED_CAP),
    }
}

/// The recorded-input dataset distilled for a chat turn, with skip accounting so the
/// model can explain *why* instances were dropped (capture off, truncated, fetch error).
#[derive(Default, Clone)]
pub struct RecordedDataset {
    pub instances: Vec<RecordedInstance>,
    /// The true recorded population this was distilled from, when the source can report it
    /// up front (an in-memory dataset). `None` for a live gateway, whose total is only
    /// discoverable by paging — there `instances.len()` is the best estimate.
    pub population: Option<usize>,
    pub matched: u32,
    pub skipped: u32,
    pub skip_reasons: BTreeMap<String, u32>,
}

/// The full recorded population this dataset was distilled from: the source total when known,
/// else the number actually loaded (a live gateway can't cheaply know there are more).
fn population_total(dataset: &RecordedDataset) -> usize {
    dataset.population.unwrap_or(dataset.instances.len())
}

/// Distil up to `cap` replayable recorded instances from a trace source. Best-effort:
/// unreplayable / unfetchable instances are counted in the skip accounting, never fatal.
pub async fn build_recorded_dataset(src: &TraceSource, cap: usize) -> RecordedDataset {
    let mut ds = RecordedDataset {
        population: src.total(),
        ..Default::default()
    };
    let summaries = match src.list_traces(cap).await {
        Ok(s) => s,
        Err(_) => return ds,
    };
    for s in summaries.iter().take(cap) {
        ds.matched += 1;
        match src.trace(&s.instance_key).await {
            Ok(t) => match RecordedInstance::from_trace(&t) {
                Ok(r) => ds.instances.push(r),
                Err(why) => {
                    ds.skipped += 1;
                    *ds.skip_reasons.entry(why.to_string()).or_insert(0) += 1;
                }
            },
            Err(_) => {
                ds.skipped += 1;
                *ds.skip_reasons
                    .entry("trace fetch failed".into())
                    .or_insert(0) += 1;
            }
        }
    }
    ds
}

/// The most common process id across the recorded instances (the one to start on replay).
fn default_process_id(ds: &RecordedDataset) -> Option<String> {
    let mut counts: BTreeMap<&str, u32> = BTreeMap::new();
    for r in &ds.instances {
        *counts.entry(r.process_id.as_str()).or_insert(0) += 1;
    }
    counts
        .into_iter()
        .max_by_key(|(_, n)| *n)
        .map(|(id, _)| id.to_string())
}

/// The honest "can't replay" payload when the dataset carries no replayable instances.
fn unavailable(ds: &RecordedDataset) -> Value {
    json!({
        "replayable": false,
        "reason": "no replayable recorded instances are available for this process",
        "matchedInstances": ds.matched,
        "skipped": ds.skipped,
        "skipReasons": ds.skip_reasons,
        "hint": "recorded-input replay needs Tier-2 capture — run the source gateway with \
                 `c8 nano --capture` (NANOBPMN_TRACE_STIMULI). Without it you can still reason \
                 about the model structurally (read_model / analyze_model), but the Alternate \
                 Reality Engine has no ground-truth inputs to re-run.",
    })
}

/// Drop the heavy per-instance `report.results` array from a serialized scorecard so the
/// model gets the aggregates without a flood of per-instance rows.
fn trim_report(report: &mut Value) {
    if let Some(obj) = report.as_object_mut() {
        obj.remove("results");
    }
}

/// When a candidate fails to deploy, the BPMN parser's reason is buried in
/// `scorecard.report.error` under a prominent `feasible:false` / "did not deploy". Lift it
/// to a top-level `deployError` on the scorecard and, for the recurring authoring mistakes
/// a model makes, attach an actionable `fixHint` — so the agent gets a crisp signal it can
/// act on instead of re-submitting the same broken XML.
fn surface_deploy_error(scorecard: &mut Value) {
    let err = scorecard
        .get("report")
        .and_then(|r| r.get("error"))
        .and_then(|e| e.as_str())
        .map(|s| s.to_string());
    let Some(err) = err else { return };
    if let Some(obj) = scorecard.as_object_mut() {
        if let Some(hint) = crate::bpmn_model::deploy_fix_hint(&err) {
            obj.insert("fixHint".into(), Value::String(hint));
        }
        obj.insert("deployError".into(), Value::String(err));
    }
}

/// When the candidate over-issues an EXISTING worker (a job type with recorded
/// history that still ran its replay FIFO dry), lift a prominent top-level
/// `structuralDivergence` hint so the agent fixes the topology (gateway/condition
/// placement, a duplicated branch) instead of being tempted to mock a real worker.
fn surface_divergence_hint(scorecard: &mut Value) {
    let divergent: Vec<String> = scorecard
        .get("divergentWorkers")
        .and_then(|w| w.as_array())
        .map(|a| {
            a.iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect()
        })
        .unwrap_or_default();
    if divergent.is_empty() {
        return;
    }
    if let Some(obj) = scorecard.as_object_mut() {
        obj.insert(
            "structuralDivergence".into(),
            Value::String(format!(
                "Existing worker(s) ({}) were issued more often than history recorded — your \
                 variant routes a branch that did not occur (a broken gateway/condition or a \
                 duplicated path). Fix the topology; do NOT add mockWorkers for these — they \
                 have real recorded history.",
                divergent.join(", ")
            )),
        );
    }
}

/// When simulate flags uncovered job types that are actually a serviceTask's id
/// (the taskDefinition-binding mistake surfacing as "uses element_id as job type"),
/// attach an actionable `jobTypeHints` array so the agent gets a crisp fix instead
/// of perceiving an engine bug. `healed` is the model actually replayed.
fn surface_job_type_hints(scorecard: &mut Value, healed: &str) {
    let uncovered: Vec<String> = scorecard
        .get("requiresNewWorkers")
        .and_then(|w| w.as_array())
        .map(|a| {
            a.iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect()
        })
        .unwrap_or_default();
    let hints = crate::bpmn_model::job_type_binding_hints(healed, &uncovered);
    if !hints.is_empty() {
        if let Some(obj) = scorecard.as_object_mut() {
            obj.insert("jobTypeHints".into(), json!(hints));
        }
    }
}

/// `simulate` — replay one candidate model against the recorded dataset.
pub fn simulate(
    base_model: Option<&str>,
    dataset: &RecordedDataset,
    args: &Value,
    annotations: Option<&crate::layout::SemanticAnnotations>,
) -> Result<Value, String> {
    let model = args["model"]
        .as_str()
        .ok_or("simulate requires a string 'model' argument (the candidate BPMN XML)")?;
    if dataset.instances.is_empty() {
        return Ok(unavailable(dataset));
    }
    let name = args["name"].as_str().unwrap_or("variant").to_string();
    let rationale = args["rationale"].as_str().map(|s| s.to_string());
    let mock_workers = parse_mock_workers(&args["mockWorkers"]);
    let pid = default_process_id(dataset);
    // Heal the common authoring mistakes at the deploy boundary so the model that gets REPLAYED is
    // the one we validated — the green light is on the same artifact (no validate-X-but-simulate-Y).
    let (healed, fixes) = crate::bpmn_model::normalize_authoring(model);
    let candidate = CandidateModel {
        name,
        rationale,
        model: healed.clone(),
        mock_workers,
    };
    let instances = sample_slice(&dataset.instances, args);
    let ranking =
        rank_candidates_by_replay(std::slice::from_ref(&candidate), instances, pid.as_deref());
    let population = population_total(dataset);
    let sampled = (ranking.dataset_size as usize) < population;
    let mut v = serde_json::to_value(&ranking).map_err(|e| format!("serialise ranking: {e}"))?;
    if let Some(c) = v["candidates"].as_array_mut().and_then(|a| a.first_mut()) {
        trim_report(&mut c["report"]);
        surface_deploy_error(c);
        surface_divergence_hint(c);
        surface_job_type_hints(c, &candidate.model);
        if !fixes.is_empty() {
            c["authoringFixes"] = json!(fixes);
        }
        if let Some(ann) = annotations {
            attach_annotation_deltas(c, base_model, &healed, ann);
        }
        return Ok(json!({
            "replayable": true,
            "datasetSize": ranking.dataset_size,
            "populationTotal": population,
            "sampled": sampled,
            "authoringFixes": fixes,
            "scorecard": c,
        }));
    }
    Ok(json!({
        "replayable": true,
        "datasetSize": ranking.dataset_size,
        "populationTotal": population,
        "sampled": sampled,
        "scorecard": Value::Null,
    }))
}

/// Compute per-instance cost/time rollups for the baseline and the (healed) variant,
/// then attach `costAnnotated` / `timeAnnotated` blocks to the scorecard. The delta
/// only makes sense within a currency, so cost deltas are keyed by currency; time
/// deltas are absolute p50/p99 sums (upper-bound, sequential — flagged as such in
/// the UI). Elements that appear in the variant XML but aren't in the sidecar
/// contribute 0 — the coverage numbers make the "unmeasured additions" honest.
fn attach_annotation_deltas(
    scorecard: &mut Value,
    base_model: Option<&str>,
    variant_model: &str,
    ann: &crate::layout::SemanticAnnotations,
) {
    let variant_ids = element_ids(variant_model);
    let variant_roll =
        crate::annotations::AnnotatedRollup::from_ids(variant_ids.iter().map(String::as_str), ann);
    let base_roll = base_model.map(|xml| {
        let ids = element_ids(xml);
        crate::annotations::AnnotatedRollup::from_ids(ids.iter().map(String::as_str), ann)
    });

    // Cost delta per currency (only report currencies that appear in either rollup).
    let mut cost_deltas: BTreeMap<String, f64> = BTreeMap::new();
    if let Some(b) = &base_roll {
        for (cur, base_v) in &b.costs {
            let var_v = variant_roll.costs.get(cur).copied().unwrap_or(0.0);
            cost_deltas.insert(cur.clone(), var_v - base_v);
        }
        for (cur, var_v) in &variant_roll.costs {
            cost_deltas
                .entry(cur.clone())
                .or_insert_with(|| var_v - b.costs.get(cur).copied().unwrap_or(0.0));
        }
    }

    let mut cost_block = json!({
        "variant": variant_roll.costs,
        "coverage": {
            "measured": variant_roll.cost_covered,
            "total": variant_roll.total_elements,
        },
    });
    if variant_roll.cost_unit_skipped > 0 {
        cost_block["unitSkipped"] = json!(variant_roll.cost_unit_skipped);
    }
    if let Some(b) = &base_roll {
        cost_block["baseline"] = json!(b.costs);
        cost_block["delta"] = json!(cost_deltas);
    }
    scorecard["costAnnotated"] = cost_block;

    let mut time_block = json!({
        "variant": {
            "p50Ms": variant_roll.p50_ms,
            "p99Ms": variant_roll.p99_ms,
        },
        "coverage": {
            "measured": variant_roll.time_covered,
            "total": variant_roll.total_elements,
        },
        "aggregation": "sequential-upper-bound",
    });
    if let Some(b) = &base_roll {
        time_block["baseline"] = json!({
            "p50Ms": b.p50_ms,
            "p99Ms": b.p99_ms,
        });
        let dp50 = match (variant_roll.p50_ms, b.p50_ms) {
            (Some(v), Some(bv)) => Some(v as i64 - bv as i64),
            _ => None,
        };
        let dp99 = match (variant_roll.p99_ms, b.p99_ms) {
            (Some(v), Some(bv)) => Some(v as i64 - bv as i64),
            _ => None,
        };
        time_block["delta"] = json!({
            "p50Ms": dp50,
            "p99Ms": dp99,
        });
    }
    scorecard["timeAnnotated"] = time_block;
}

/// Best-effort element-id list for a BPMN XML fragment. On a parse error we return
/// an empty list — the caller then reports zero coverage, which is honest.
fn element_ids(xml: &str) -> Vec<String> {
    crate::bpmn_model::inline_definition(xml, None)
        .map(|def| def.elements.keys().cloned().collect())
        .unwrap_or_default()
}

/// Resolve how many recorded instances to replay this call. `limit` (alias
/// `sampleSize`) lets the investigator run a **cheap single-instance smoke**
/// (`limit:1`) to validate a model parses & conserves, then a **small sample**
/// (e.g. `limit:25`) to surface obvious issues, before committing to the **full
/// dataset**. Execution is cheap — staging the run is just for fast iteration,
/// never a correctness concern. An absent / zero / oversized limit replays all.
fn sample_slice<'a>(instances: &'a [RecordedInstance], args: &Value) -> &'a [RecordedInstance] {
    let limit = args["limit"]
        .as_u64()
        .or_else(|| args["sampleSize"].as_u64());
    match limit {
        Some(n) if n > 0 && (n as usize) < instances.len() => &instances[..n as usize],
        _ => instances,
    }
}

/// `compare_variants` — score and rank a population of candidate models (optionally
/// including the current model as the `baseline`) against the same recorded dataset.
pub fn compare_variants(
    base_model: Option<&str>,
    dataset: &RecordedDataset,
    args: &Value,
    annotations: Option<&crate::layout::SemanticAnnotations>,
) -> Result<Value, String> {
    let raw = args["candidates"]
        .as_array()
        .ok_or("compare_variants requires a 'candidates' array of {name, model, rationale?}")?;
    if dataset.instances.is_empty() {
        return Ok(unavailable(dataset));
    }
    let include_baseline = args["includeBaseline"].as_bool().unwrap_or(true);
    // A top-level `mockWorkers` applies to every candidate (the new workers
    // available in this alternate reality); a per-candidate `mockWorkers` merges
    // over it for variants that define their own.
    let shared_mocks = parse_mock_workers(&args["mockWorkers"]);

    let mut candidates: Vec<CandidateModel> = Vec::new();
    // name -> authoring fixes applied, so we can surface them on the matching ranked output below.
    let mut fixes_by_name: std::collections::BTreeMap<String, Vec<String>> =
        std::collections::BTreeMap::new();
    // name -> the (healed) model actually replayed, for job-type-binding hints below.
    let mut healed_by_name: std::collections::BTreeMap<String, String> =
        std::collections::BTreeMap::new();
    if include_baseline {
        if let Some(base) = base_model {
            candidates.push(CandidateModel {
                name: "baseline (current model)".into(),
                rationale: Some("the model currently deployed for this process".into()),
                model: base.to_string(),
                ..Default::default()
            });
            healed_by_name.insert("baseline (current model)".into(), base.to_string());
        }
    }
    for (i, c) in raw.iter().enumerate() {
        let model = c["model"]
            .as_str()
            .ok_or_else(|| format!("candidate #{i} is missing a string 'model' (BPMN XML)"))?;
        let mut mock_workers = shared_mocks.clone();
        for (jt, out) in parse_mock_workers(&c["mockWorkers"]) {
            mock_workers.insert(jt, out);
        }
        let name = c["name"]
            .as_str()
            .unwrap_or(&format!("variant {}", i + 1))
            .to_string();
        // Heal authoring mistakes at the deploy boundary (see `simulate`); surface what changed.
        let (healed, fixes) = crate::bpmn_model::normalize_authoring(model);
        if !fixes.is_empty() {
            fixes_by_name.insert(name.clone(), fixes);
        }
        healed_by_name.insert(name.clone(), healed.clone());
        candidates.push(CandidateModel {
            name,
            rationale: c["rationale"].as_str().map(|s| s.to_string()),
            model: healed,
            mock_workers,
        });
    }
    if candidates.is_empty() {
        return Err("no candidates to compare".into());
    }

    let pid = default_process_id(dataset);
    let instances = sample_slice(&dataset.instances, args);
    let ranking = rank_candidates_by_replay(&candidates, instances, pid.as_deref());
    let population = population_total(dataset);
    let sampled = (ranking.dataset_size as usize) < population;
    let mut v = serde_json::to_value(&ranking).map_err(|e| format!("serialise ranking: {e}"))?;
    if let Some(arr) = v["candidates"].as_array_mut() {
        for c in arr.iter_mut() {
            trim_report(&mut c["report"]);
            surface_deploy_error(c);
            surface_divergence_hint(c);
            if let Some(healed) = c["name"].as_str().and_then(|n| healed_by_name.get(n)) {
                let healed = healed.clone();
                surface_job_type_hints(c, &healed);
            }
            if let Some(fixes) = c["name"].as_str().and_then(|n| fixes_by_name.get(n)) {
                c["authoringFixes"] = json!(fixes);
            }
            if let Some(ann) = annotations {
                if let Some(healed) = c["name"].as_str().and_then(|n| healed_by_name.get(n)) {
                    let healed = healed.clone();
                    attach_annotation_deltas(c, base_model, &healed, ann);
                }
            }
        }
    }
    v["populationTotal"] = json!(population);
    v["sampled"] = json!(sampled);
    Ok(v)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use serde_json::json;

    use super::*;
    use crate::harness::RecordedStimulus;

    // start -> Classify(classify) -> Summarize(summarize) -> end. Process id "P".
    const TWO_TASK: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0" id="Defs">
  <bpmn:process id="P" name="P" isExecutable="true">
    <bpmn:startEvent id="Start"><bpmn:outgoing>f1</bpmn:outgoing></bpmn:startEvent>
    <bpmn:sequenceFlow id="f1" sourceRef="Start" targetRef="Classify" />
    <bpmn:serviceTask id="Classify" name="Classify">
      <bpmn:extensionElements><zeebe:taskDefinition type="classify" /></bpmn:extensionElements>
      <bpmn:incoming>f1</bpmn:incoming><bpmn:outgoing>f2</bpmn:outgoing>
    </bpmn:serviceTask>
    <bpmn:sequenceFlow id="f2" sourceRef="Classify" targetRef="Summarize" />
    <bpmn:serviceTask id="Summarize" name="Summarize">
      <bpmn:extensionElements><zeebe:taskDefinition type="summarize" /></bpmn:extensionElements>
      <bpmn:incoming>f2</bpmn:incoming><bpmn:outgoing>f3</bpmn:outgoing>
    </bpmn:serviceTask>
    <bpmn:sequenceFlow id="f3" sourceRef="Summarize" targetRef="End" />
    <bpmn:endEvent id="End"><bpmn:incoming>f3</bpmn:incoming></bpmn:endEvent>
  </bpmn:process>
</bpmn:definitions>"#;

    // Drops Summarize — a variant that fails to reproduce the recorded 'summary' key.
    const ONE_TASK: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0" id="Defs">
  <bpmn:process id="P" name="P" isExecutable="true">
    <bpmn:startEvent id="Start"><bpmn:outgoing>f1</bpmn:outgoing></bpmn:startEvent>
    <bpmn:sequenceFlow id="f1" sourceRef="Start" targetRef="Classify" />
    <bpmn:serviceTask id="Classify" name="Classify">
      <bpmn:extensionElements><zeebe:taskDefinition type="classify" /></bpmn:extensionElements>
      <bpmn:incoming>f1</bpmn:incoming><bpmn:outgoing>f2</bpmn:outgoing>
    </bpmn:serviceTask>
    <bpmn:sequenceFlow id="f2" sourceRef="Classify" targetRef="End" />
    <bpmn:endEvent id="End"><bpmn:incoming>f2</bpmn:incoming></bpmn:endEvent>
  </bpmn:process>
</bpmn:definitions>"#;

    fn dataset() -> RecordedDataset {
        let instances = (0..3)
            .map(|i| RecordedInstance {
                instance_key: format!("k{i}"),
                process_id: "P".into(),
                started_at: 1000,
                creation_variables: [("input".to_string(), json!("x"))].into_iter().collect(),
                stimuli: vec![
                    RecordedStimulus {
                        seq: 1,
                        at: 1100,
                        kind: "jobCompleted".into(),
                        reference: Some("classify".into()),
                        variables: Some(
                            [("label".to_string(), json!("A"))]
                                .into_iter()
                                .collect::<BTreeMap<_, _>>(),
                        ),
                    },
                    RecordedStimulus {
                        seq: 2,
                        at: 1300,
                        kind: "jobCompleted".into(),
                        reference: Some("summarize".into()),
                        variables: Some(
                            [("summary".to_string(), json!("S"))]
                                .into_iter()
                                .collect::<BTreeMap<_, _>>(),
                        ),
                    },
                ],
            })
            .collect();
        RecordedDataset {
            instances,
            population: None,
            matched: 3,
            skipped: 0,
            skip_reasons: BTreeMap::new(),
        }
    }

    #[test]
    fn simulate_scores_a_conserving_variant() {
        let ds = dataset();
        let v = simulate(
            None,
            &ds,
            &json!({ "model": TWO_TASK, "name": "identity" }),
            None,
        )
        .unwrap();
        assert_eq!(v["replayable"], true);
        assert_eq!(v["datasetSize"], 3);
        let sc = &v["scorecard"];
        assert_eq!(sc["fidelityTier"], "recorded-replay");
        assert_eq!(sc["report"]["conserved"], 3);
        assert!((sc["report"]["conservedRate"].as_f64().unwrap() - 1.0).abs() < 1e-9);
        // The heavy per-instance results were trimmed out.
        assert!(sc["report"].get("results").is_none());
    }

    // TWO_TASK plus an error boundary authored as the NON-EXISTENT `<bpmn:errorBoundaryEvent>` —
    // unparseable as-is; the deploy boundary must auto-heal it (the Investigation 1 failure).
    const TWO_TASK_ERR_BOUNDARY: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0" id="Defs">
  <bpmn:error id="E1" errorCode="BOOM"/>
  <bpmn:process id="P" name="P" isExecutable="true">
    <bpmn:startEvent id="Start"><bpmn:outgoing>f1</bpmn:outgoing></bpmn:startEvent>
    <bpmn:sequenceFlow id="f1" sourceRef="Start" targetRef="Classify" />
    <bpmn:serviceTask id="Classify" name="Classify">
      <bpmn:extensionElements><zeebe:taskDefinition type="classify" /></bpmn:extensionElements>
      <bpmn:incoming>f1</bpmn:incoming><bpmn:outgoing>f2</bpmn:outgoing>
    </bpmn:serviceTask>
    <bpmn:sequenceFlow id="f2" sourceRef="Classify" targetRef="Summarize" />
    <bpmn:serviceTask id="Summarize" name="Summarize">
      <bpmn:extensionElements><zeebe:taskDefinition type="summarize" /></bpmn:extensionElements>
      <bpmn:incoming>f2</bpmn:incoming><bpmn:outgoing>f3</bpmn:outgoing>
    </bpmn:serviceTask>
    <bpmn:sequenceFlow id="f3" sourceRef="Summarize" targetRef="End" />
    <bpmn:endEvent id="End"><bpmn:incoming>f3</bpmn:incoming></bpmn:endEvent>
    <bpmn:errorBoundaryEvent id="BE" attachedToRef="Classify"><bpmn:errorEventDefinition errorRef="E1"/></bpmn:errorBoundaryEvent>
    <bpmn:sequenceFlow id="f4" sourceRef="BE" targetRef="ErrEnd" />
    <bpmn:endEvent id="ErrEnd" />
  </bpmn:process>
</bpmn:definitions>"#;

    #[test]
    fn simulate_auto_heals_a_misspelled_error_boundary_at_the_deploy_boundary() {
        let ds = dataset();
        let v = simulate(
            None,
            &ds,
            &json!({ "model": TWO_TASK_ERR_BOUNDARY, "name": "err" }),
            None,
        )
        .unwrap();
        // It deployed and scored (not infeasible) because the deploy boundary healed the XML,
        // and the fix is surfaced so the model learns rather than re-submitting the same mistake.
        let sc = &v["scorecard"];
        assert_eq!(sc["feasible"], true, "should deploy after healing: {sc}");
        assert_eq!(sc["report"]["conserved"], 3);
        let fixes = v["authoringFixes"].as_array().expect("authoringFixes");
        assert_eq!(fixes.len(), 1);
        assert!(fixes[0].as_str().unwrap().contains("errorBoundaryEvent"));
    }

    #[test]
    fn simulate_reports_total_and_unsampled_by_default() {
        let ds = dataset();
        let v = simulate(None, &ds, &json!({ "model": TWO_TASK }), None).unwrap();
        assert_eq!(v["datasetSize"], 3);
        assert_eq!(v["populationTotal"], 3);
        assert_eq!(v["sampled"], false);
    }

    #[test]
    fn simulate_reports_the_true_population_even_when_fewer_were_loaded() {
        // A capped/distilled dataset: only 3 instances loaded, but the source population is 6553.
        let mut ds = dataset();
        ds.population = Some(6553);
        let v = simulate(None, &ds, &json!({ "model": TWO_TASK }), None).unwrap();
        assert_eq!(v["datasetSize"], 3, "replayed what was loaded");
        assert_eq!(
            v["populationTotal"], 6553,
            "reports the real population, not the loaded slice"
        );
        assert_eq!(v["sampled"], true, "3 of 6553 is a sample");
    }

    #[test]
    fn simulate_limit_replays_only_a_sample() {
        let ds = dataset();
        // limit:1 — the cheap single-run smoke.
        let v = simulate(None, &ds, &json!({ "model": TWO_TASK, "limit": 1 }), None).unwrap();
        assert_eq!(v["datasetSize"], 1, "only one instance replayed");
        assert_eq!(v["populationTotal"], 3, "full population still reported");
        assert_eq!(v["sampled"], true);
        assert_eq!(v["scorecard"]["report"]["instancesTotal"], 1);

        // sampleSize alias works too; an oversized limit falls back to the full set.
        let s = simulate(
            None,
            &ds,
            &json!({ "model": TWO_TASK, "sampleSize": 2 }),
            None,
        )
        .unwrap();
        assert_eq!(s["datasetSize"], 2);
        let full = simulate(None, &ds, &json!({ "model": TWO_TASK, "limit": 999 }), None).unwrap();
        assert_eq!(full["datasetSize"], 3);
        assert_eq!(full["sampled"], false);
    }

    #[test]
    fn compare_variants_honours_limit() {
        let ds = dataset();
        let v = compare_variants(
            Some(TWO_TASK),
            &ds,
            &json!({ "candidates": [ { "name": "identity", "model": TWO_TASK } ], "limit": 1 }),
            None,
        )
        .unwrap();
        assert_eq!(v["datasetSize"], 1);
        assert_eq!(v["populationTotal"], 3);
        assert_eq!(v["sampled"], true);
    }

    #[test]
    fn simulate_is_honest_when_nothing_is_replayable() {
        let empty = RecordedDataset {
            matched: 5,
            skipped: 5,
            skip_reasons: [(
                "trace has no recorded-input log (run the source node with c8 nano --capture)"
                    .to_string(),
                5,
            )]
            .into_iter()
            .collect(),
            ..Default::default()
        };
        let v = simulate(None, &empty, &json!({ "model": TWO_TASK }), None).unwrap();
        assert_eq!(v["replayable"], false);
        assert_eq!(v["skipped"], 5);
    }

    #[test]
    fn compare_variants_ranks_baseline_above_a_diverging_fork() {
        let ds = dataset();
        let v = compare_variants(
            Some(TWO_TASK),
            &ds,
            &json!({ "candidates": [ { "name": "drop-summarize", "model": ONE_TASK } ] }),
            None,
        )
        .unwrap();
        assert_eq!(v["datasetSize"], 3);
        // The conserving baseline is the best candidate; the diverging fork ranks below.
        assert_eq!(v["best"], "baseline (current model)");
        let cands = v["candidates"].as_array().unwrap();
        assert_eq!(cands.len(), 2);
        let baseline = cands
            .iter()
            .find(|c| c["name"] == "baseline (current model)")
            .unwrap();
        assert_eq!(baseline["report"]["conserved"], 3);
        let fork = cands
            .iter()
            .find(|c| c["name"] == "drop-summarize")
            .unwrap();
        assert_eq!(fork["report"]["conserved"], 0);
    }

    #[test]
    fn simulate_scores_a_new_worker_when_a_mock_is_supplied() {
        // TWO_TASK issues classify+summarize, but our dataset only recorded classify —
        // so summarize is a new worker. Supplying a mock for it makes the variant
        // scorable at the mocked-replay tier instead of requiring a generative mock.
        let ds = RecordedDataset {
            instances: vec![RecordedInstance {
                instance_key: "k0".into(),
                process_id: "P".into(),
                started_at: 1000,
                creation_variables: [("input".to_string(), json!("x"))].into_iter().collect(),
                stimuli: vec![RecordedStimulus {
                    seq: 1,
                    at: 1100,
                    kind: "jobCompleted".into(),
                    reference: Some("classify".into()),
                    variables: Some(
                        [("label".to_string(), json!("A"))]
                            .into_iter()
                            .collect::<BTreeMap<_, _>>(),
                    ),
                }],
            }],
            matched: 1,
            skipped: 0,
            skip_reasons: BTreeMap::new(),
            population: None,
        };
        let v = simulate(
            None,
            &ds,
            &json!({
                "model": TWO_TASK,
                "name": "mocked",
                "mockWorkers": { "summarize": { "summary": "M" } }
            }),
            None,
        )
        .unwrap();
        assert_eq!(v["replayable"], true);
        let sc = &v["scorecard"];
        assert_eq!(sc["fidelityTier"], "mocked-replay");
        assert_eq!(sc["mockedWorkers"], json!(["summarize"]));
        assert!(sc["requiresNewWorkers"].as_array().unwrap().is_empty());
    }

    #[test]
    fn surface_deploy_error_lifts_the_parse_error_with_a_fix_hint() {
        // The Investigation 12 symptom: the deploy reason is buried in report.error.
        let mut sc = json!({
            "feasible": false,
            "confidence": "n/a — candidate did not deploy",
            "report": {
                "valid": 0,
                "error": "failed to parse: InvalidBoundaryEvent { process_id: \"loan-approval\", reason: \"boundary event BoundaryEvent_CreditError references unknown error ''\" }"
            }
        });
        surface_deploy_error(&mut sc);
        assert!(sc["deployError"]
            .as_str()
            .unwrap()
            .contains("InvalidBoundaryEvent"));
        assert!(sc["fixHint"].as_str().unwrap().contains("errorRef"));
    }

    #[test]
    fn surface_deploy_error_is_a_noop_for_a_clean_scorecard() {
        let mut sc = json!({ "feasible": true, "report": { "conserved": 3 } });
        surface_deploy_error(&mut sc);
        assert!(sc.get("deployError").is_none());
        assert!(sc.get("fixHint").is_none());
    }

    #[test]
    fn surface_divergence_hint_lifts_existing_over_issued_workers() {
        let mut sc = json!({ "divergentWorkers": ["credit-check", "reject-application"] });
        surface_divergence_hint(&mut sc);
        let hint = sc
            .get("structuralDivergence")
            .and_then(|v| v.as_str())
            .unwrap();
        assert!(hint.contains("credit-check"));
        assert!(hint.contains("do NOT add mockWorkers"));
    }

    #[test]
    fn surface_divergence_hint_is_a_noop_without_divergent_workers() {
        let mut sc = json!({ "divergentWorkers": [] });
        surface_divergence_hint(&mut sc);
        assert!(sc.get("structuralDivergence").is_none());
    }

    #[test]
    fn simulate_attaches_cost_and_time_deltas_from_annotations_sidecar() {
        use crate::layout::{Cost, SemanticAnnotations, Time};
        let ds = dataset();
        let mut ann = SemanticAnnotations::default();
        // Baseline (TWO_TASK) has Classify + Summarize. Variant (ONE_TASK) drops Summarize.
        ann.costs.insert(
            "Classify".into(),
            Cost {
                value: 0.10,
                currency: Some("USD".into()),
                per: Some("invocation".into()),
            },
        );
        ann.costs.insert(
            "Summarize".into(),
            Cost {
                value: 0.20,
                currency: Some("USD".into()),
                per: Some("invocation".into()),
            },
        );
        ann.times.insert(
            "Classify".into(),
            Time {
                p50_ms: Some(100),
                p99_ms: Some(300),
                source: None,
            },
        );
        ann.times.insert(
            "Summarize".into(),
            Time {
                p50_ms: Some(500),
                p99_ms: Some(1500),
                source: None,
            },
        );

        let v = simulate(
            Some(TWO_TASK),
            &ds,
            &json!({ "model": ONE_TASK, "name": "drop-summarize" }),
            Some(&ann),
        )
        .unwrap();
        let sc = &v["scorecard"];
        let cost = &sc["costAnnotated"];
        assert!(
            !cost.is_null(),
            "expected costAnnotated block on scorecard: {sc}"
        );
        assert!((cost["variant"]["USD"].as_f64().unwrap() - 0.10).abs() < 1e-9);
        assert!((cost["baseline"]["USD"].as_f64().unwrap() - 0.30).abs() < 1e-9);
        assert!((cost["delta"]["USD"].as_f64().unwrap() - (-0.20)).abs() < 1e-9);
        let time = &sc["timeAnnotated"];
        assert_eq!(time["variant"]["p50Ms"].as_u64(), Some(100));
        assert_eq!(time["variant"]["p99Ms"].as_u64(), Some(300));
        assert_eq!(time["baseline"]["p50Ms"].as_u64(), Some(600));
        assert_eq!(time["baseline"]["p99Ms"].as_u64(), Some(1800));
        assert_eq!(time["delta"]["p50Ms"].as_i64(), Some(-500));
        assert_eq!(time["delta"]["p99Ms"].as_i64(), Some(-1500));
        assert_eq!(time["aggregation"].as_str(), Some("sequential-upper-bound"));
    }

    #[test]
    fn simulate_omits_cost_and_time_blocks_when_no_annotations_provided() {
        let ds = dataset();
        let v = simulate(
            Some(TWO_TASK),
            &ds,
            &json!({ "model": TWO_TASK, "name": "identity" }),
            None,
        )
        .unwrap();
        let sc = &v["scorecard"];
        assert!(
            sc.get("costAnnotated").is_none(),
            "no annotations => no costAnnotated block"
        );
        assert!(
            sc.get("timeAnnotated").is_none(),
            "no annotations => no timeAnnotated block"
        );
    }
}
