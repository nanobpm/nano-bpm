//! M1: candidate enumeration + evaluation + ranking.
//!
//! The MVP transform space is **worker swaps**: for each job type with latent
//! options, a candidate picks one of `{default} ∪ latent_options[job_type]`. We
//! enumerate that grid (bounded), evaluate each candidate over every input with
//! the [`super::sim::SimRunner`], and rank by the objective. The optional golden
//! variant is evaluated as a candidate so we can report whether exploration
//! recovered it (a sanity check that the rig's ranking is trustworthy before we
//! hand candidate generation to an LLM in M2).

use std::cmp::Ordering;
use std::collections::BTreeMap;

use nanobpmn_engine_core::{bpmn::parse_bpmn, ProcessDefinition};
use serde::Serialize;

use super::sim::run_instance;
use super::{mix_seed, Objective, Scenario};

/// Per-candidate aggregate over all inputs.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct VariantResult {
    pub name: String,
    /// Where this candidate came from: `baseline`, `baked`, `golden`, or `llm`.
    pub source: String,
    /// Optional free-text rationale (the LLM's reasoning for an `llm` candidate).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rationale: Option<String>,
    /// The full effective `job_type -> worker_id` assignment.
    pub assignment: BTreeMap<String, String>,
    pub runs: usize,
    pub completion_rate: f64,
    pub correctness_rate: f64,
    pub incident_rate: f64,
    pub avg_latency_ms: f64,
    pub avg_cost: f64,
    pub total_cost: f64,
    /// Whether this candidate meets the objective's feasibility gates.
    pub feasible: bool,
}

/// Metadata about an LLM hypothesis pass, attached to the report when the
/// `/api/harness/hypothesize` path is used.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LlmMeta {
    pub provider: String,
    pub model: String,
    /// Candidates the model proposed.
    pub proposed: usize,
    /// Proposals that validated and were evaluated.
    pub accepted: usize,
    /// Rejected proposals with the reason (bad worker id, unparsable model, …).
    pub rejected: Vec<RejectedCandidate>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RejectedCandidate {
    pub name: String,
    pub reason: String,
}

/// The harness output: the base dataset folded into ranked candidates.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HarnessReport {
    pub scenario: String,
    pub process_id: String,
    pub inputs: usize,
    pub objective: Objective,
    /// The default assignment, evaluated as the comparison point.
    pub baseline: VariantResult,
    /// The golden variant evaluated as a candidate, if the scenario supplied one.
    pub golden: Option<VariantResult>,
    /// All enumerated candidates, ranked best-first.
    pub variants: Vec<VariantResult>,
    /// Name of the top-ranked candidate.
    pub best: String,
    /// True when the best candidate's assignment equals the golden's.
    pub recovered_golden: bool,
    /// Number of task assignments by which the best candidate differs from golden.
    pub golden_distance: Option<usize>,
    /// LLM hypothesis metadata, present only on the hypothesize path.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub llm: Option<LlmMeta>,
    /// Human-readable observations (transform space, caps applied, recovery).
    pub notes: Vec<String>,
}

/// Parse + validate a scenario's model, returning its definitions and the
/// resolved process id. Shared by the baked and LLM-hypothesis paths.
pub(crate) fn prepare(scenario: &Scenario) -> Result<(Vec<ProcessDefinition>, String), String> {
    let defs = parse_bpmn(&scenario.test_model)
        .map_err(|e| format!("test model failed to parse: {e:?}"))?;
    if defs.is_empty() {
        return Err("test model contained no process definitions".to_string());
    }
    let process_id = match &scenario.process_id {
        Some(id) => id.clone(),
        None => defs[0].id.clone(),
    };
    if !defs.iter().any(|d| d.id == process_id) {
        return Err(format!(
            "process id '{process_id}' not found in the test model"
        ));
    }
    if scenario.inputs.is_empty() {
        return Err("scenario has no inputs to evaluate".to_string());
    }
    Ok((defs, process_id))
}

/// Run a scenario end-to-end with the deterministic baked candidate generator:
/// parse, enumerate worker-swaps, evaluate, rank. Pure CPU work; the HTTP layer
/// runs it on a blocking task. No network, no LLM.
pub fn run_scenario(scenario: &Scenario) -> Result<HarnessReport, String> {
    let (defs, process_id) = prepare(scenario)?;

    let mut notes = Vec::new();
    notes.push(
        "Baked transform space: worker swaps over the test model (same BPMN). \
         The LLM hypothesis path may also propose structural rewrites."
            .to_string(),
    );

    let baseline_assignment = effective_assignment(scenario, &BTreeMap::new());
    let baseline = evaluate(
        scenario,
        &defs,
        &process_id,
        "baseline",
        "baseline",
        None,
        &baseline_assignment,
    );

    let (mut variants, baked_notes) = baked_candidates(scenario, &defs, &process_id);
    notes.extend(baked_notes);

    let golden = scenario.golden.as_ref().map(|g| {
        let assignment = effective_assignment(scenario, &g.assignment);
        evaluate(
            scenario,
            &defs,
            &process_id,
            &g.name,
            "golden",
            None,
            &assignment,
        )
    });

    Ok(finalize(
        scenario,
        process_id,
        baseline,
        golden,
        std::mem::take(&mut variants),
        notes,
        None,
    ))
}

/// Enumerate and evaluate the baked worker-swap candidate grid. Returns the
/// evaluated variants and any notes (e.g. a cap warning).
pub(crate) fn baked_candidates(
    scenario: &Scenario,
    defs: &[ProcessDefinition],
    process_id: &str,
) -> (Vec<VariantResult>, Vec<String>) {
    let mut notes = Vec::new();

    // The swappable job types and their option sets ({default} ∪ latent options).
    let mut swap_types: Vec<String> = scenario.latent_options.keys().cloned().collect();
    swap_types.sort();
    let mut option_lists: Vec<(String, Vec<String>)> = Vec::new();
    for jt in &swap_types {
        let mut opts: Vec<String> = Vec::new();
        if let Some(def) = scenario.task_workers.get(jt) {
            opts.push(def.clone());
        }
        for o in scenario.latent_options.get(jt).into_iter().flatten() {
            if !opts.contains(o) {
                opts.push(o.clone());
            }
        }
        if opts.is_empty() {
            continue;
        }
        option_lists.push((jt.clone(), opts));
    }

    // Cartesian product of the option lists, bounded to keep the MVP snappy.
    const MAX_CANDIDATES: usize = 256;
    let mut combos: Vec<Vec<usize>> = vec![Vec::new()];
    for (_, opts) in &option_lists {
        let mut next = Vec::new();
        for prefix in &combos {
            for i in 0..opts.len() {
                let mut c = prefix.clone();
                c.push(i);
                next.push(c);
                if next.len() >= MAX_CANDIDATES {
                    break;
                }
            }
            if next.len() >= MAX_CANDIDATES {
                break;
            }
        }
        combos = next;
    }
    if combos.len() >= MAX_CANDIDATES {
        notes.push(format!(
            "candidate space capped at {MAX_CANDIDATES}; not all worker-swap combinations evaluated"
        ));
    }

    let baseline_assignment = effective_assignment(scenario, &BTreeMap::new());
    let mut variants: Vec<VariantResult> = Vec::with_capacity(combos.len());
    for combo in &combos {
        let mut overrides: BTreeMap<String, String> = BTreeMap::new();
        for (slot, &choice) in combo.iter().enumerate() {
            let (jt, opts) = &option_lists[slot];
            overrides.insert(jt.clone(), opts[choice].clone());
        }
        let assignment = effective_assignment(scenario, &overrides);
        let name = candidate_name(&option_lists, combo, &baseline_assignment, &assignment);
        let source = if name == "baseline" {
            "baseline"
        } else {
            "baked"
        };
        variants.push(evaluate(
            scenario,
            defs,
            process_id,
            &name,
            source,
            None,
            &assignment,
        ));
    }
    (variants, notes)
}

/// Rank a candidate set, compute golden recovery, and assemble the final report.
/// Shared by the baked and LLM-hypothesis paths.
pub(crate) fn finalize(
    scenario: &Scenario,
    process_id: String,
    baseline: VariantResult,
    golden: Option<VariantResult>,
    mut variants: Vec<VariantResult>,
    mut notes: Vec<String>,
    llm: Option<LlmMeta>,
) -> HarnessReport {
    let golden_assignment: Option<BTreeMap<String, String>> =
        golden.as_ref().map(|g| g.assignment.clone());

    rank(&mut variants, &scenario.objective);

    let best = variants
        .first()
        .map(|v| v.name.clone())
        .unwrap_or_else(|| baseline.name.clone());
    let best_assignment = variants.first().map(|v| v.assignment.clone());

    let (recovered_golden, golden_distance) = match (&golden_assignment, &best_assignment) {
        (Some(g), Some(b)) => {
            let dist = assignment_distance(g, b);
            (dist == 0, Some(dist))
        }
        _ => (false, None),
    };

    if let Some(g) = &golden {
        if recovered_golden {
            notes.push(format!(
                "exploration recovered the golden variant ('{}') as the top-ranked candidate.",
                g.name
            ));
        } else if let Some(d) = golden_distance {
            notes.push(format!(
                "top candidate differs from golden in {d} task assignment(s)."
            ));
        }
    }

    HarnessReport {
        scenario: scenario.name.clone(),
        process_id,
        inputs: scenario.inputs.len(),
        objective: scenario.objective.clone(),
        baseline,
        golden,
        variants,
        best,
        recovered_golden,
        golden_distance,
        llm,
        notes,
    }
}

/// Merge per-task overrides onto the scenario defaults into a full assignment.
pub(crate) fn effective_assignment(
    scenario: &Scenario,
    overrides: &BTreeMap<String, String>,
) -> BTreeMap<String, String> {
    let mut m: BTreeMap<String, String> = scenario
        .task_workers
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    for (k, v) in overrides {
        m.insert(k.clone(), v.clone());
    }
    m
}

/// Name a candidate by the tasks it swaps away from the baseline, or "baseline".
fn candidate_name(
    option_lists: &[(String, Vec<String>)],
    combo: &[usize],
    baseline: &BTreeMap<String, String>,
    assignment: &BTreeMap<String, String>,
) -> String {
    let mut swaps: Vec<String> = Vec::new();
    for (slot, &choice) in combo.iter().enumerate() {
        let (jt, opts) = &option_lists[slot];
        let worker = &opts[choice];
        if baseline.get(jt) != Some(worker) {
            swaps.push(format!("{jt}={worker}"));
        }
    }
    let _ = assignment;
    if swaps.is_empty() {
        "baseline".to_string()
    } else {
        swaps.join(", ")
    }
}

/// Evaluate one assignment over all scenario inputs into an aggregate result.
pub(crate) fn evaluate(
    scenario: &Scenario,
    defs: &[ProcessDefinition],
    process_id: &str,
    name: &str,
    source: &str,
    rationale: Option<String>,
    assignment: &BTreeMap<String, String>,
) -> VariantResult {
    let lookup: BTreeMap<String, String> = assignment
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    let n = scenario.inputs.len().max(1);
    let mut completed = 0usize;
    let mut correct = 0usize;
    let mut with_incident = 0usize;
    let mut total_latency = 0u128;
    let mut total_cost = 0.0_f64;

    for (i, input) in scenario.inputs.iter().enumerate() {
        let seed = mix_seed(scenario.seed, i);
        let run = run_instance(
            defs,
            process_id,
            &input.vars,
            &input.expected,
            &lookup,
            scenario,
            seed,
        );
        if run.completed {
            completed += 1;
        }
        if run.correct {
            correct += 1;
        }
        if run.incidents > 0 {
            with_incident += 1;
        }
        total_latency += run.e2e_latency_ms as u128;
        total_cost += run.cost;
    }

    let nf = n as f64;
    let objective = &scenario.objective;
    let completion_rate = completed as f64 / nf;
    let correctness_rate = correct as f64 / nf;
    let incident_rate = with_incident as f64 / nf;
    let feasible = correctness_rate >= objective.min_correctness
        && incident_rate <= objective.max_incident_rate;

    VariantResult {
        name: name.to_string(),
        source: source.to_string(),
        rationale,
        assignment: assignment.clone(),
        runs: n,
        completion_rate,
        correctness_rate,
        incident_rate,
        avg_latency_ms: total_latency as f64 / nf,
        avg_cost: total_cost / nf,
        total_cost,
        feasible,
    }
}

/// Rank candidates best-first: feasible ahead of infeasible, then by the chosen
/// axis, then latency, then cost. Infeasible candidates fall back to highest
/// correctness first so the report still shows the closest near-misses.
fn rank(variants: &mut [VariantResult], objective: &Objective) {
    variants.sort_by(|a, b| {
        // Feasible first.
        match (a.feasible, b.feasible) {
            (true, false) => return Ordering::Less,
            (false, true) => return Ordering::Greater,
            _ => {}
        }
        if a.feasible {
            let pa = primary_metric(a, objective);
            let pb = primary_metric(b, objective);
            cmp_f64(pa, pb)
                .then_with(|| cmp_f64(a.avg_latency_ms, b.avg_latency_ms))
                .then_with(|| cmp_f64(a.avg_cost, b.avg_cost))
                .then_with(|| a.name.cmp(&b.name))
        } else {
            // Both infeasible: prefer higher correctness, then lower incident rate.
            cmp_f64(b.correctness_rate, a.correctness_rate)
                .then_with(|| cmp_f64(a.incident_rate, b.incident_rate))
                .then_with(|| cmp_f64(primary_metric(a, objective), primary_metric(b, objective)))
                .then_with(|| a.name.cmp(&b.name))
        }
    });
}

fn primary_metric(v: &VariantResult, objective: &Objective) -> f64 {
    match objective.minimize.as_str() {
        "latency" => v.avg_latency_ms,
        "incidents" => v.incident_rate,
        _ => v.avg_cost, // "cost" and any unknown axis default to cost
    }
}

fn cmp_f64(a: f64, b: f64) -> Ordering {
    a.partial_cmp(&b).unwrap_or(Ordering::Equal)
}

/// Number of task assignments by which two full assignments differ.
fn assignment_distance(a: &BTreeMap<String, String>, b: &BTreeMap<String, String>) -> usize {
    let mut keys: std::collections::BTreeSet<&String> = a.keys().collect();
    keys.extend(b.keys());
    keys.into_iter().filter(|k| a.get(*k) != b.get(*k)).count()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::harness::example_scenario;

    #[test]
    fn ranks_golden_best_on_the_example() {
        let scenario = example_scenario();
        let report = run_scenario(&scenario).expect("example scenario runs");
        // The example is built so the golden (cheap classify, premium summarize)
        // is the cheapest fully-correct, incident-free candidate.
        assert!(
            report.recovered_golden,
            "expected golden recovery; notes: {:?}",
            report.notes
        );
        assert_eq!(report.golden_distance, Some(0));
        let best = report.variants.first().unwrap();
        assert!(best.feasible);
        assert!((best.correctness_rate - 1.0).abs() < 1e-9);
        assert_eq!(best.incident_rate, 0.0);
        // The best feasible candidate must be at least as cheap as the baseline.
        assert!(best.avg_cost <= report.baseline.avg_cost + 1e-9);
    }

    #[test]
    fn determinism_same_scenario_same_ranking() {
        let scenario = example_scenario();
        let a = run_scenario(&scenario).unwrap();
        let b = run_scenario(&scenario).unwrap();
        let names_a: Vec<_> = a.variants.iter().map(|v| &v.name).collect();
        let names_b: Vec<_> = b.variants.iter().map(|v| &v.name).collect();
        assert_eq!(names_a, names_b);
        assert_eq!(a.best, b.best);
    }

    #[test]
    fn baseline_is_feasible_but_not_cheapest() {
        let scenario = example_scenario();
        let report = run_scenario(&scenario).unwrap();
        assert!(report.baseline.feasible);
        // Swapping classify to the cheap worker is a strict cost win.
        assert!(report.best != "baseline");
        let best = report.variants.first().unwrap();
        assert!(best.avg_cost < report.baseline.avg_cost);
    }
}
