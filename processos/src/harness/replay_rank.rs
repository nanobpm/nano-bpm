//! Replay-ranked candidate evaluation (the gradient-driven loop core, §7.9).
//!
//! Where [`super::replay::replay_dataset`] scores **one** candidate against a
//! recorded dataset, this module scores a whole **population** of candidates
//! against the *same* dataset and ranks them by the fidelity gradient — the
//! "engine as fitness function" fan-out the hypothesis loop ranks with.
//!
//! It is the Level-2 analogue of [`super::rank`] (which ranks worker-swap
//! candidates on the distributional SimRunner). Replay discriminates
//! **structural** candidates: it re-drives each candidate model against the
//! recorded job outputs (matched by job *type*) and the recorded creation
//! inputs, so a redesign that still reproduces production's observed boundary
//! ranks above one that diverges or needs workers history never exercised.
//!
//! Ranking is **fidelity-first, never a single verdict**: feasible candidates
//! (those that deploy + run on the dataset) sort ahead of invalid ones; among
//! feasible, higher `conservedRate` wins, then lower replayed latency, then
//! fewer required new workers. Every candidate keeps its full [`ReplayReport`]
//! scorecard for drill-down, so the operator decides — the harness only ranks.

use std::collections::BTreeMap;

use nanobpmn_engine_core::bpmn::parse_bpmn;
use nanobpmn_engine_core::ProcessDefinition;
use serde::Serialize;

use super::replay::{
    replay_dataset, replay_dataset_with_mocks, MockWorkers, RecordedInstance, ReplayReport,
};

/// A candidate model to score, as supplied by the operator or the LLM.
#[derive(Clone, Debug, Default)]
pub struct CandidateModel {
    /// Display name for the candidate (e.g. an LLM rationale label).
    pub name: String,
    /// Why this candidate was proposed (carried through to the scorecard).
    pub rationale: Option<String>,
    /// BPMN XML of the candidate model.
    pub model: String,
    /// Operator/LLM-supplied generative mocks for any **new workers** this
    /// candidate introduces (job type → assumed output delta). Empty for a
    /// candidate that only reuses existing, recorded job types.
    pub mock_workers: MockWorkers,
}

/// The evaluation fidelity tier a candidate was scored at (§7.7 fidelity ladder).
///
/// Surfaced per candidate so the rank step **never collapses into one number** — a
/// result is only as trustworthy as the tier that produced it, and the tier doubles
/// as the deploy-cost signal (replay-evaluable ⇔ no new workers).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum FidelityTier {
    /// The candidate could not be deployed/replayed at all (parse or feasibility
    /// failure) — no fidelity; see `report.error`.
    Infeasible,
    /// **Level 2 — recorded-input replay.** Every job the candidate issued had a
    /// recorded output *of that job type*, so routing and the boundary used real
    /// production values. The result is measured (±replay residual), high trust.
    RecordedReplay,
    /// **Level 3 — generative mock applied.** The candidate introduces new job
    /// types, but the operator/LLM supplied a mock worker for every one, so it was
    /// scored end-to-end on those *assumed* outputs. Evaluable, but only as trust-
    /// worthy as the mock — distinct from a measured Level-2 result.
    MockedReplay,
    /// **Level 3 — generative mock required (not yet evaluable).** The candidate
    /// issues job types history never exercised *and no mock was supplied* for
    /// them, so faithful replay is impossible. Surfaced, not hidden: these
    /// candidates also require new workers to *deploy*.
    RequiresGenerativeMock,
}

impl FidelityTier {
    /// Sort precedence: replay-evaluable (most trustworthy) first, then mock-applied,
    /// then mock-required, then infeasible. Keeps a high-fidelity result ahead of a
    /// speculative one even when a flukey conserved rate would otherwise reorder them.
    fn rank(self) -> u8 {
        match self {
            FidelityTier::RecordedReplay => 0,
            FidelityTier::MockedReplay => 1,
            FidelityTier::RequiresGenerativeMock => 2,
            FidelityTier::Infeasible => 3,
        }
    }
}

/// Classify a scored candidate onto the fidelity ladder from its replay scorecard.
fn classify_tier(feasible: bool, report: &ReplayReport) -> FidelityTier {
    if !feasible || report.error.is_some() {
        FidelityTier::Infeasible
    } else if !report.uncovered_job_types.is_empty() {
        FidelityTier::RequiresGenerativeMock
    } else if !report.mocked_job_types.is_empty() {
        FidelityTier::MockedReplay
    } else {
        FidelityTier::RecordedReplay
    }
}

/// A short, honest confidence band for the candidate's tier — never overstated.
fn confidence_band(tier: FidelityTier, report: &ReplayReport) -> String {
    let base = match tier {
        FidelityTier::Infeasible => "n/a — candidate did not deploy".to_string(),
        FidelityTier::RecordedReplay => format!(
            "Level 2 · measured on {} recorded instance(s), {} boundary-conserved",
            report.instances_total, report.conserved
        ),
        FidelityTier::MockedReplay => format!(
            "Level 3 · scored with operator-supplied mock(s) for {} new worker(s) ({}) — \
             assumption-based, not measured",
            report.mocked_job_types.len(),
            report.mocked_job_types.join(", ")
        ),
        FidelityTier::RequiresGenerativeMock => format!(
            "Level 3 required · {} new job type(s) need a generative mock to score ({}) — \
             supply mockWorkers to evaluate",
            report.uncovered_job_types.len(),
            report.uncovered_job_types.join(", ")
        ),
    };
    // A divergence note when the candidate over-issues EXISTING workers (a broken
    // structure, not a missing worker) — steer toward fixing topology, not mocking.
    if report.divergent_job_types.is_empty() {
        base
    } else {
        format!(
            "{base} · structural divergence: existing worker(s) ({}) were issued more often \
             than history recorded — check gateway/condition placement, not mockWorkers",
            report.divergent_job_types.join(", ")
        )
    }
}

/// One scored candidate in the ranking.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RankedCandidate {
    /// Display name.
    pub name: String,
    /// Why it was proposed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rationale: Option<String>,
    /// Process id the candidate was replayed under.
    pub process_id: String,
    /// Whether the candidate parsed + replayed at all (an unparsable candidate is
    /// `false` with the parse error in `report.error`).
    pub feasible: bool,
    /// Which rung of the §7.7 fidelity ladder this result was produced at.
    pub fidelity_tier: FidelityTier,
    /// Job types the candidate issues that history never recorded — the candidate
    /// needs a new worker per type to *deploy*, and a generative mock to *evaluate*.
    /// Empty ⇔ replay-evaluable (Level 2). First-class, not buried in the scorecard.
    pub requires_new_workers: Vec<String>,
    /// New job types that WERE scored, served from an operator/LLM-supplied mock
    /// (the new workers this candidate introduced and provided a mock for).
    pub mocked_workers: Vec<String>,
    /// **Existing** workers (with recorded history) the candidate issued more often
    /// than history did — a structural-divergence signal pointing at a broken
    /// gateway/condition or duplicated branch. Distinct from `requires_new_workers`:
    /// these must NOT be mocked; the topology should be fixed instead.
    pub divergent_workers: Vec<String>,
    /// A short confidence band qualifying the metrics by their fidelity tier.
    pub confidence: String,
    /// The full Level-2 scorecard for this candidate.
    pub report: ReplayReport,
}

/// The ranked population, plus the dataset it was scored against.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ReplayRanking {
    /// How many recorded instances every candidate was replayed over.
    pub dataset_size: u32,
    /// Candidates, best fidelity first.
    pub candidates: Vec<RankedCandidate>,
    /// Name of the top-ranked feasible candidate, if any.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub best: Option<String>,
}

/// Score + rank a population of candidate models against one recorded dataset.
///
/// Pure and deterministic: the dataset is fetched by the caller (I/O stays out
/// of here). An unparsable or process-less candidate is retained as an
/// *infeasible* entry (so the operator sees *why* it failed) and sorts last.
pub fn rank_candidates_by_replay(
    candidates: &[CandidateModel],
    dataset: &[RecordedInstance],
    default_process_id: Option<&str>,
) -> ReplayRanking {
    let mut ranked: Vec<RankedCandidate> = candidates
        .iter()
        .map(|c| score_candidate(c, dataset, default_process_id))
        .collect();

    ranked.sort_by_key(candidate_order);

    let best = ranked
        .iter()
        .find(|c| c.feasible && c.report.error.is_none())
        .map(|c| c.name.clone());

    ReplayRanking {
        dataset_size: dataset.len() as u32,
        candidates: ranked,
        best,
    }
}

/// Score a single candidate, distilling a parse failure into an infeasible entry
/// rather than dropping it.
fn score_candidate(
    c: &CandidateModel,
    dataset: &[RecordedInstance],
    default_process_id: Option<&str>,
) -> RankedCandidate {
    match parse_bpmn(&c.model) {
        Ok(defs) if !defs.is_empty() => {
            let process_id = default_process_id
                .map(|s| s.to_string())
                .unwrap_or_else(|| defs[0].id.clone());
            // Inline call activities before replay: the engine has no first-class call-activity
            // executor (it expands them into sub-process scopes), so a multi-stage orchestrator —
            // or a variant the droid authored by editing a single called phase — only runs once
            // its phases are spliced in. With no call activities this is a faithful identity.
            let library: BTreeMap<String, ProcessDefinition> =
                defs.iter().map(|d| (d.id.clone(), d.clone())).collect();
            let run_defs: Vec<ProcessDefinition> = library
                .get(&process_id)
                .and_then(|root| root.inline_call_activities(&library).ok())
                .map(|inlined| vec![inlined])
                .unwrap_or_else(|| defs.clone());
            let report =
                replay_dataset_with_mocks(&run_defs, &process_id, dataset, &c.mock_workers);
            let tier = classify_tier(true, &report);
            RankedCandidate {
                name: c.name.clone(),
                rationale: c.rationale.clone(),
                process_id,
                feasible: true,
                fidelity_tier: tier,
                requires_new_workers: report.uncovered_job_types.clone(),
                mocked_workers: report.mocked_job_types.clone(),
                divergent_workers: report.divergent_job_types.clone(),
                confidence: confidence_band(tier, &report),
                report,
            }
        }
        Ok(_) => infeasible(
            c,
            default_process_id,
            "candidate contained no process definitions",
        ),
        Err(e) => infeasible(c, default_process_id, &format!("failed to parse: {e:?}")),
    }
}

/// Build an infeasible `RankedCandidate` carrying the reason in `report.error`.
/// Scored over an empty dataset (the report is just a carrier for the parse
/// error), keeping every aggregate counter at zero.
fn infeasible(
    c: &CandidateModel,
    default_process_id: Option<&str>,
    reason: &str,
) -> RankedCandidate {
    let no_defs: [ProcessDefinition; 0] = [];
    let process_id = default_process_id.unwrap_or("").to_string();
    let mut report = replay_dataset(&no_defs, &process_id, &[]);
    report.error = Some(reason.to_string());
    let tier = classify_tier(false, &report);
    RankedCandidate {
        name: c.name.clone(),
        rationale: c.rationale.clone(),
        process_id,
        feasible: false,
        fidelity_tier: tier,
        requires_new_workers: report.uncovered_job_types.clone(),
        mocked_workers: report.mocked_job_types.clone(),
        divergent_workers: report.divergent_job_types.clone(),
        confidence: confidence_band(tier, &report),
        report,
    }
}

/// Sort key: feasible-and-valid first, then by fidelity *tier* (a measured Level-2
/// result outranks a speculative mock-required one), then by descending conserved
/// fidelity, then ascending replayed latency, then fewer required new workers, then
/// name.
///
/// Returned as a tuple of orderable scalars (latency/fidelity scaled to integers
/// for a total order).
fn candidate_order(c: &RankedCandidate) -> (u8, u8, i64, u64, usize, String) {
    let broken = if c.feasible && c.report.error.is_none() {
        0
    } else {
        1
    };
    let tier = c.fidelity_tier.rank();
    // Higher conservedRate first ⇒ negate (scaled to permille for integer order).
    let fidelity = -((c.report.conserved_rate * 1000.0).round() as i64);
    let latency = c.report.avg_e2e_latency_ms.round() as u64;
    let new_workers = c.report.uncovered_job_types.len();
    (broken, tier, fidelity, latency, new_workers, c.name.clone())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use serde_json::json;
    use serde_json::Value as Json;

    use super::*;
    use crate::harness::replay::{RecordedInstance, RecordedStimulus};

    const TWO_TASK: &str = r#"<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0" id="D">
  <bpmn:process id="P" isExecutable="true">
    <bpmn:startEvent id="S"><bpmn:outgoing>f1</bpmn:outgoing></bpmn:startEvent>
    <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="C"/>
    <bpmn:serviceTask id="C" name="Classify">
      <bpmn:extensionElements><zeebe:taskDefinition type="classify"/></bpmn:extensionElements>
      <bpmn:incoming>f1</bpmn:incoming><bpmn:outgoing>f2</bpmn:outgoing>
    </bpmn:serviceTask>
    <bpmn:sequenceFlow id="f2" sourceRef="C" targetRef="U"/>
    <bpmn:serviceTask id="U" name="Summarize">
      <bpmn:extensionElements><zeebe:taskDefinition type="summarize"/></bpmn:extensionElements>
      <bpmn:incoming>f2</bpmn:incoming><bpmn:outgoing>f3</bpmn:outgoing>
    </bpmn:serviceTask>
    <bpmn:sequenceFlow id="f3" sourceRef="U" targetRef="E"/>
    <bpmn:endEvent id="E"><bpmn:incoming>f3</bpmn:incoming></bpmn:endEvent>
  </bpmn:process>
</bpmn:definitions>"#;

    const ONE_TASK: &str = r#"<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0" id="D">
  <bpmn:process id="P" isExecutable="true">
    <bpmn:startEvent id="S"><bpmn:outgoing>f1</bpmn:outgoing></bpmn:startEvent>
    <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="C"/>
    <bpmn:serviceTask id="C" name="Classify">
      <bpmn:extensionElements><zeebe:taskDefinition type="classify"/></bpmn:extensionElements>
      <bpmn:incoming>f1</bpmn:incoming><bpmn:outgoing>f3</bpmn:outgoing>
    </bpmn:serviceTask>
    <bpmn:sequenceFlow id="f3" sourceRef="C" targetRef="E"/>
    <bpmn:endEvent id="E"><bpmn:incoming>f3</bpmn:incoming></bpmn:endEvent>
  </bpmn:process>
</bpmn:definitions>"#;

    // Introduces a job type ("translate") history never recorded — replay cannot serve
    // it, so it is a Level-3 (generative-mock-required) candidate that also needs a new
    // worker to deploy.
    const NEW_JOB: &str = r#"<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0" id="D">
  <bpmn:process id="P" isExecutable="true">
    <bpmn:startEvent id="S"><bpmn:outgoing>f1</bpmn:outgoing></bpmn:startEvent>
    <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="C"/>
    <bpmn:serviceTask id="C" name="Classify">
      <bpmn:extensionElements><zeebe:taskDefinition type="classify"/></bpmn:extensionElements>
      <bpmn:incoming>f1</bpmn:incoming><bpmn:outgoing>f2</bpmn:outgoing>
    </bpmn:serviceTask>
    <bpmn:sequenceFlow id="f2" sourceRef="C" targetRef="T"/>
    <bpmn:serviceTask id="T" name="Translate">
      <bpmn:extensionElements><zeebe:taskDefinition type="translate"/></bpmn:extensionElements>
      <bpmn:incoming>f2</bpmn:incoming><bpmn:outgoing>f3</bpmn:outgoing>
    </bpmn:serviceTask>
    <bpmn:sequenceFlow id="f3" sourceRef="T" targetRef="E"/>
    <bpmn:endEvent id="E"><bpmn:incoming>f3</bpmn:incoming></bpmn:endEvent>
  </bpmn:process>
</bpmn:definitions>"#;

    fn map(pairs: &[(&str, Json)]) -> BTreeMap<String, Json> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect()
    }

    fn rec(creation: &[(&str, Json)], stimuli: Vec<RecordedStimulus>) -> RecordedInstance {
        RecordedInstance {
            instance_key: "1".into(),
            process_id: "P".into(),
            started_at: 1000,
            creation_variables: map(creation),
            stimuli,
            terminal_variables: None,
        }
    }

    fn job(seq: u32, at: u64, job_type: &str, out: Option<&[(&str, Json)]>) -> RecordedStimulus {
        RecordedStimulus {
            seq,
            at,
            kind: "jobCompleted".into(),
            reference: Some(job_type.into()),
            variables: out.map(map),
        }
    }

    fn dataset() -> Vec<RecordedInstance> {
        let mk = |k: &str| {
            rec(
                &[("input", json!("x"))],
                vec![
                    job(1, 1100, "classify", Some(&[("label", json!("A"))])),
                    job(2, 1300, "summarize", Some(&[("summary", json!(k))])),
                ],
            )
        };
        vec![mk("S1"), mk("S2"), mk("S3")]
    }

    // A multi-stage orchestrator P → CallPhase(calledElement="Phase") → End, where the
    // called phase runs classify → summarize. The engine has no call-activity executor, so
    // this only replays if score_candidate inlines the phase first.
    const CALL_ACTIVITY: &str = r#"<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0" id="D">
  <bpmn:process id="P" isExecutable="true">
    <bpmn:startEvent id="S"><bpmn:outgoing>f1</bpmn:outgoing></bpmn:startEvent>
    <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="CallPhase"/>
    <bpmn:callActivity id="CallPhase" name="Run phase" calledElement="Phase">
      <bpmn:incoming>f1</bpmn:incoming><bpmn:outgoing>f2</bpmn:outgoing>
    </bpmn:callActivity>
    <bpmn:sequenceFlow id="f2" sourceRef="CallPhase" targetRef="E"/>
    <bpmn:endEvent id="E"><bpmn:incoming>f2</bpmn:incoming></bpmn:endEvent>
  </bpmn:process>
  <bpmn:process id="Phase" isExecutable="true">
    <bpmn:startEvent id="PS"><bpmn:outgoing>p1</bpmn:outgoing></bpmn:startEvent>
    <bpmn:sequenceFlow id="p1" sourceRef="PS" targetRef="C"/>
    <bpmn:serviceTask id="C" name="Classify">
      <bpmn:extensionElements><zeebe:taskDefinition type="classify"/></bpmn:extensionElements>
      <bpmn:incoming>p1</bpmn:incoming><bpmn:outgoing>p2</bpmn:outgoing>
    </bpmn:serviceTask>
    <bpmn:sequenceFlow id="p2" sourceRef="C" targetRef="U"/>
    <bpmn:serviceTask id="U" name="Summarize">
      <bpmn:extensionElements><zeebe:taskDefinition type="summarize"/></bpmn:extensionElements>
      <bpmn:incoming>p2</bpmn:incoming><bpmn:outgoing>p3</bpmn:outgoing>
    </bpmn:serviceTask>
    <bpmn:sequenceFlow id="p3" sourceRef="U" targetRef="PE"/>
    <bpmn:endEvent id="PE"><bpmn:incoming>p3</bpmn:incoming></bpmn:endEvent>
  </bpmn:process>
</bpmn:definitions>"#;

    #[test]
    fn a_call_activity_candidate_is_inlined_before_replay_and_conserves() {
        let ds = dataset();
        let cands = vec![CandidateModel {
            name: "orchestrated".into(),
            rationale: None,
            model: CALL_ACTIVITY.into(),
            ..Default::default()
        }];
        let ranking = rank_candidates_by_replay(&cands, &ds, Some("P"));
        let c = &ranking.candidates[0];
        assert!(
            c.feasible && c.report.error.is_none(),
            "deploys: {:?}",
            c.report.error
        );
        // The inlined phase fires classify then summarize, conserving against the recorded runs.
        assert!(
            (c.report.conserved_rate - 1.0).abs() < 1e-9,
            "call-activity model conserves once inlined: {:?}",
            c.report
        );
        assert_eq!(ranking.best.as_deref(), Some("orchestrated"));
    }

    #[test]
    fn the_conserving_candidate_outranks_the_diverging_one() {
        let ds = dataset();
        let cands = vec![
            CandidateModel {
                name: "drop-summarize".into(),
                rationale: Some("fewer steps".into()),
                model: ONE_TASK.into(),
                ..Default::default()
            },
            CandidateModel {
                name: "keep-both".into(),
                rationale: None,
                model: TWO_TASK.into(),
                ..Default::default()
            },
        ];
        let ranking = rank_candidates_by_replay(&cands, &ds, Some("P"));
        assert_eq!(ranking.dataset_size, 3);
        assert_eq!(ranking.best.as_deref(), Some("keep-both"));
        assert_eq!(ranking.candidates[0].name, "keep-both");
        assert!((ranking.candidates[0].report.conserved_rate - 1.0).abs() < 1e-9);
        // The diverging candidate is ranked below and still carries its scorecard.
        assert_eq!(ranking.candidates[1].name, "drop-summarize");
        assert_eq!(ranking.candidates[1].report.conserved, 0);
    }

    #[test]
    fn an_existing_worker_run_dry_is_a_divergence_not_a_required_new_worker() {
        // A dataset where summarize is recorded in instance 1 but not instance 2.
        // TWO_TASK issues summarize for both ⇒ it runs dry in instance 2. Because
        // summarize HAS history in the dataset, the candidate must report it as a
        // structural divergence (not a required new worker), and the confidence band
        // must steer toward fixing topology rather than supplying a mock.
        let ds = vec![
            rec(
                &[("input", json!("x"))],
                vec![
                    job(1, 1100, "classify", Some(&[("label", json!("A"))])),
                    job(2, 1300, "summarize", Some(&[("summary", json!("S"))])),
                ],
            ),
            rec(
                &[("input", json!("y"))],
                vec![job(1, 1100, "classify", Some(&[("label", json!("B"))]))],
            ),
        ];
        let cands = vec![CandidateModel {
            name: "keep-both".into(),
            rationale: None,
            model: TWO_TASK.into(),
            ..Default::default()
        }];
        let ranking = rank_candidates_by_replay(&cands, &ds, Some("P"));
        let c = &ranking.candidates[0];
        assert_eq!(c.divergent_workers, vec!["summarize".to_string()]);
        assert!(
            c.requires_new_workers.is_empty(),
            "an existing worker must not be flagged as requiring a new worker"
        );
        assert_ne!(
            c.fidelity_tier,
            FidelityTier::RequiresGenerativeMock,
            "an over-issued EXISTING worker is not a generative-mock-required candidate"
        );
        assert!(
            c.confidence.contains("structural divergence"),
            "confidence must point at the topology, got: {}",
            c.confidence
        );
    }

    #[test]
    fn an_unparsable_candidate_is_infeasible_and_ranks_last() {
        let ds = dataset();
        let cands = vec![
            CandidateModel {
                name: "broken".into(),
                rationale: None,
                model: "<not-bpmn/>".into(),
                ..Default::default()
            },
            CandidateModel {
                name: "good".into(),
                rationale: None,
                model: TWO_TASK.into(),
                ..Default::default()
            },
        ];
        let ranking = rank_candidates_by_replay(&cands, &ds, Some("P"));
        assert_eq!(ranking.best.as_deref(), Some("good"));
        assert_eq!(ranking.candidates[0].name, "good");
        let broken = &ranking.candidates[1];
        assert_eq!(broken.name, "broken");
        assert!(!broken.feasible);
        assert!(broken.report.error.is_some());
    }

    #[test]
    fn empty_population_has_no_best() {
        let ranking = rank_candidates_by_replay(&[], &dataset(), Some("P"));
        assert!(ranking.best.is_none());
        assert!(ranking.candidates.is_empty());
        assert_eq!(ranking.dataset_size, 3);
    }

    #[test]
    fn fidelity_tier_is_labelled_per_candidate_and_orders_replay_above_mock() {
        let ds = dataset();
        let cands = vec![
            CandidateModel {
                name: "needs-translate-worker".into(),
                rationale: Some("add translation".into()),
                model: NEW_JOB.into(),
                ..Default::default()
            },
            CandidateModel {
                name: "keep-both".into(),
                rationale: None,
                model: TWO_TASK.into(),
                ..Default::default()
            },
            CandidateModel {
                name: "broken".into(),
                rationale: None,
                model: "<not-bpmn/>".into(),
                ..Default::default()
            },
        ];
        let ranking = rank_candidates_by_replay(&cands, &ds, Some("P"));

        // A replay-evaluable candidate ranks above one that needs a generative mock,
        // which ranks above the infeasible one.
        assert_eq!(ranking.candidates[0].name, "keep-both");
        assert_eq!(ranking.candidates[1].name, "needs-translate-worker");
        assert_eq!(ranking.candidates[2].name, "broken");

        // Level 2: fully covered by recorded job outputs, no new workers.
        let l2 = &ranking.candidates[0];
        assert_eq!(l2.fidelity_tier, FidelityTier::RecordedReplay);
        assert!(l2.requires_new_workers.is_empty());
        assert!(l2.confidence.starts_with("Level 2"));

        // Level 3: issues a job type history never recorded ⇒ mock required + new worker.
        let l3 = &ranking.candidates[1];
        assert_eq!(l3.fidelity_tier, FidelityTier::RequiresGenerativeMock);
        assert_eq!(l3.requires_new_workers, vec!["translate".to_string()]);
        assert!(l3.confidence.starts_with("Level 3"));

        // Infeasible: no tier credit.
        let bad = &ranking.candidates[2];
        assert_eq!(bad.fidelity_tier, FidelityTier::Infeasible);
        assert!(!bad.feasible);
    }

    #[test]
    fn fidelity_tier_serializes_as_kebab_case() {
        let ds = dataset();
        let cands = vec![CandidateModel {
            name: "keep-both".into(),
            rationale: None,
            model: TWO_TASK.into(),
            ..Default::default()
        }];
        let ranking = rank_candidates_by_replay(&cands, &ds, Some("P"));
        let json = serde_json::to_value(&ranking).unwrap();
        assert_eq!(
            json["candidates"][0]["fidelityTier"],
            serde_json::json!("recorded-replay")
        );
        assert!(json["candidates"][0]["requiresNewWorkers"].is_array());
        assert!(json["candidates"][0]["confidence"].is_string());
    }

    #[test]
    fn supplying_a_mock_for_a_new_worker_upgrades_it_to_mocked_replay() {
        let ds = dataset();
        let mut with_mock = CandidateModel {
            name: "translate-with-mock".into(),
            rationale: Some("add translation, mocked".into()),
            model: NEW_JOB.into(),
            ..Default::default()
        };
        with_mock.mock_workers.insert(
            "translate".to_string(),
            crate::harness::MockWorker::deterministic(
                [("translated".to_string(), json!(true))]
                    .into_iter()
                    .collect(),
            ),
        );
        let without_mock = CandidateModel {
            name: "translate-no-mock".into(),
            model: NEW_JOB.into(),
            ..Default::default()
        };
        let ranking = rank_candidates_by_replay(&[with_mock, without_mock], &ds, Some("P"));

        let mocked = ranking
            .candidates
            .iter()
            .find(|c| c.name == "translate-with-mock")
            .unwrap();
        assert_eq!(mocked.fidelity_tier, FidelityTier::MockedReplay);
        assert_eq!(mocked.mocked_workers, vec!["translate".to_string()]);
        assert!(mocked.requires_new_workers.is_empty());
        assert!(mocked.confidence.starts_with("Level 3"));

        let unmocked = ranking
            .candidates
            .iter()
            .find(|c| c.name == "translate-no-mock")
            .unwrap();
        assert_eq!(unmocked.fidelity_tier, FidelityTier::RequiresGenerativeMock);

        // A mock-applied (evaluable) candidate ranks above one that still needs a mock.
        let mocked_idx = ranking
            .candidates
            .iter()
            .position(|c| c.name == "translate-with-mock")
            .unwrap();
        let unmocked_idx = ranking
            .candidates
            .iter()
            .position(|c| c.name == "translate-no-mock")
            .unwrap();
        assert!(mocked_idx < unmocked_idx);
    }
}
