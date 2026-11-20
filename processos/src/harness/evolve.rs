//! LLM-proposed structural candidates, scored on real recorded traces (§7.9).
//!
//! This is the "droid proposes, engine proves" step of the gradient-driven loop,
//! wired end-to-end against production data. Where [`super::hypothesize`] asks the
//! model for worker-swap *assignments* scored on the distributional
//! [`super::sim`] runner, this module asks the model for full **structural
//! redesigns** (candidate BPMN models) and scores them with
//! [`super::replay_rank`] — replaying each against the *actual* recorded inputs
//! and outputs of the process, so a hallucinated "improvement" that fails to
//! reproduce the observed boundary is exposed by `conservedRate`, not trusted.
//!
//! The pieces here are pure and testable: distil the production signal a model
//! needs to reason (the job types it must drive, the boundary keys it must
//! preserve, a sample input), build the prompt, and parse the model's reply. The
//! LLM call and the trace fetch are orchestrated by the caller (the HTTP layer),
//! keeping this module free of I/O.

use std::collections::BTreeSet;

use serde::Serialize;
use serde_json::Value as Json;

use super::replay::RecordedInstance;
use super::replay_rank::CandidateModel;

/// The distilled production signal a candidate generator reasons over — derived
/// purely from the recorded dataset, never from the live engine.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DatasetSignal {
    /// The process whose recorded instances were summarised.
    pub process_id: String,
    /// How many recorded instances back this signal.
    pub instance_count: u32,
    /// Job types production actually completed (a candidate that issues only
    /// these needs no new workers; anything else is an uncovered requirement).
    pub job_types: Vec<String>,
    /// Keys observed on the recorded boundary (creation inputs + every stimulus
    /// output) — what a faithful candidate is expected to still produce.
    pub boundary_keys: Vec<String>,
    /// A representative creation-input payload, to ground the model in the shape
    /// of the data (the first instance's creation variables).
    pub sample_input: Json,
}

/// Summarise a recorded dataset into the signal a candidate generator needs.
/// Pure: `job_types` and `boundary_keys` are unions across the dataset, sorted
/// for determinism.
pub fn summarize_dataset(process_id: &str, dataset: &[RecordedInstance]) -> DatasetSignal {
    let mut job_types: BTreeSet<String> = BTreeSet::new();
    let mut boundary_keys: BTreeSet<String> = BTreeSet::new();

    for rec in dataset {
        for k in rec.creation_variables.keys() {
            boundary_keys.insert(k.clone());
        }
        for s in &rec.stimuli {
            if s.kind == "jobCompleted" {
                if let Some(r) = &s.reference {
                    job_types.insert(r.clone());
                }
            }
            if let Some(vars) = &s.variables {
                for k in vars.keys() {
                    boundary_keys.insert(k.clone());
                }
            }
        }
    }

    let sample_input = dataset
        .first()
        .map(|rec| {
            let mut m = serde_json::Map::new();
            for (k, v) in &rec.creation_variables {
                m.insert(k.clone(), v.clone());
            }
            Json::Object(m)
        })
        .unwrap_or(Json::Null);

    DatasetSignal {
        process_id: process_id.to_string(),
        instance_count: dataset.len() as u32,
        job_types: job_types.into_iter().collect(),
        boundary_keys: boundary_keys.into_iter().collect(),
        sample_input,
    }
}

/// The built-in system prompt for structural candidate generation. Seeds the
/// prompt library; callers may select or author alternatives.
pub const DEFAULT_EVOLVE_SYSTEM_PROMPT: &str = "You are a BPMN process-redesign assistant. \
Given the current executable BPMN model of a process and a summary of how it actually runs in \
production (the service-task job types it completes, the data keys it produces, and a sample \
input), you propose alternative executable BPMN models that could improve the process — fewer \
or cheaper steps, better parallelism — WITHOUT losing any of the boundary data keys the process \
is observed to produce. Each candidate must keep the same process id so it can be replayed \
against the recorded history. Prefer reusing the existing job types; introducing a new job type \
means a new worker must be built, so only do it when clearly worthwhile. Reply with ONLY a JSON \
array and no prose. Each element is an object: {\"name\": string, \"rationale\": string, \
\"model\": string} where \"model\" is the full BPMN XML of the candidate.";

/// Build the user prompt: the baseline model plus the distilled production signal.
pub fn build_evolve_prompt(
    signal: &DatasetSignal,
    baseline_model: &str,
    pilot_note: Option<&str>,
) -> String {
    let job_types = if signal.job_types.is_empty() {
        "(none recorded)".to_string()
    } else {
        signal.job_types.join(", ")
    };
    let boundary_keys = if signal.boundary_keys.is_empty() {
        "(none recorded)".to_string()
    } else {
        signal.boundary_keys.join(", ")
    };
    let sample = serde_json::to_string(&signal.sample_input).unwrap_or_else(|_| "null".to_string());

    // The pilot's free-text steer for this round (§10) — surfaced prominently so the
    // droid conditions its redesigns on the human's intent, not just the metrics.
    let guidance = match pilot_note.map(str::trim).filter(|s| !s.is_empty()) {
        Some(note) => format!("Operator guidance for this round (prioritise this): {note}\n\n"),
        None => String::new(),
    };

    format!(
        "Process id: {pid}\n\
         Recorded instances summarised: {count}\n\
         Service-task job types completed in production: {jobs}\n\
         Boundary data keys the process is observed to produce: {keys}\n\
         Sample creation input: {sample}\n\n\
         {guidance}\
         Current executable BPMN model:\n{model}\n\n\
         Propose 1-3 alternative executable BPMN models (same process id `{pid}`) that could \
         improve this process while still producing the boundary keys above. Reply with ONLY the \
         JSON array described in the system prompt.",
        pid = signal.process_id,
        count = signal.instance_count,
        jobs = job_types,
        keys = boundary_keys,
        sample = sample,
        guidance = guidance,
        model = baseline_model,
    )
}

/// One structural candidate as proposed by the model.
#[derive(serde::Deserialize)]
struct ProposedStructural {
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    rationale: Option<String>,
    model: String,
}

/// Parse the model's reply into candidate models, tolerating code fences and
/// prose around the JSON array (small local models rarely reply with bare JSON).
pub fn parse_structural_candidates(text: &str) -> Result<Vec<CandidateModel>, String> {
    let cleaned = strip_code_fences(text);

    let parsed: Option<Vec<ProposedStructural>> =
        serde_json::from_str(cleaned.trim()).ok().or_else(|| {
            // Last resort: slice to the outermost JSON array.
            match (cleaned.find('['), cleaned.rfind(']')) {
                (Some(start), Some(end)) if end > start => {
                    serde_json::from_str(&cleaned[start..=end]).ok()
                }
                _ => None,
            }
        });

    let parsed = parsed.ok_or_else(|| {
        format!(
            "could not parse structural candidates from LLM output: {}",
            truncate(text, 300)
        )
    })?;

    let candidates: Vec<CandidateModel> = parsed
        .into_iter()
        .enumerate()
        .filter(|(_, c)| !c.model.trim().is_empty())
        .map(|(i, c)| CandidateModel {
            name: c
                .name
                .filter(|s| !s.trim().is_empty())
                .unwrap_or_else(|| format!("llm-candidate-{}", i + 1)),
            rationale: c.rationale.filter(|s| !s.trim().is_empty()),
            model: c.model,
            ..Default::default()
        })
        .collect();

    if candidates.is_empty() {
        return Err("LLM produced no candidates with a non-empty model".to_string());
    }
    Ok(candidates)
}

/// Strip a leading/trailing ``` code fence (with optional language tag), if any.
fn strip_code_fences(text: &str) -> String {
    if let Some(open) = text.find("```") {
        let after = &text[open + 3..];
        let body_start = after.find('\n').map(|n| n + 1).unwrap_or(0);
        let body = &after[body_start..];
        if let Some(close) = body.find("```") {
            return body[..close].to_string();
        }
        return body.to_string();
    }
    text.to_string()
}

fn truncate(s: &str, n: usize) -> String {
    if s.len() <= n {
        s.to_string()
    } else {
        format!("{}…", &s[..n])
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use serde_json::json;

    use super::*;
    use crate::harness::replay::{RecordedInstance, RecordedStimulus};

    fn map(pairs: &[(&str, Json)]) -> BTreeMap<String, Json> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect()
    }

    fn job(seq: u32, at: u64, jt: &str, out: &[(&str, Json)]) -> RecordedStimulus {
        RecordedStimulus {
            seq,
            at,
            kind: "jobCompleted".into(),
            reference: Some(jt.into()),
            variables: Some(map(out)),
        }
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

    #[test]
    fn summarize_unions_job_types_and_boundary_keys() {
        let ds = vec![
            rec(
                &[("input", json!("x"))],
                vec![
                    job(1, 1100, "classify", &[("label", json!("A"))]),
                    job(2, 1300, "summarize", &[("summary", json!("S"))]),
                ],
            ),
            rec(
                &[("input", json!("y")), ("priority", json!(2))],
                vec![job(1, 1100, "classify", &[("label", json!("B"))])],
            ),
        ];
        let sig = summarize_dataset("P", &ds);
        assert_eq!(sig.instance_count, 2);
        assert_eq!(
            sig.job_types,
            vec!["classify".to_string(), "summarize".to_string()]
        );
        // boundary keys: creation (input, priority) + outputs (label, summary), sorted.
        assert_eq!(
            sig.boundary_keys,
            vec![
                "input".to_string(),
                "label".to_string(),
                "priority".to_string(),
                "summary".to_string(),
            ]
        );
        assert_eq!(sig.sample_input, json!({"input": "x"}));
    }

    #[test]
    fn prompt_includes_the_signal_and_model() {
        let sig = summarize_dataset(
            "P",
            &[rec(
                &[("input", json!("x"))],
                vec![job(1, 1100, "classify", &[("label", json!("A"))])],
            )],
        );
        let p = build_evolve_prompt(&sig, "<bpmn:definitions/>", None);
        assert!(p.contains("Process id: P"));
        assert!(p.contains("classify"));
        assert!(p.contains("label"));
        assert!(p.contains("<bpmn:definitions/>"));
        assert!(!p.contains("Operator guidance"));
    }

    #[test]
    fn prompt_includes_pilot_guidance_when_present() {
        let sig = summarize_dataset(
            "P",
            &[rec(
                &[("input", json!("x"))],
                vec![job(1, 1100, "classify", &[("label", json!("A"))])],
            )],
        );
        let p = build_evolve_prompt(&sig, "<bpmn:definitions/>", Some("cut the tail latency"));
        assert!(p.contains("Operator guidance for this round"));
        assert!(p.contains("cut the tail latency"));
        // A blank/whitespace note is treated as no guidance.
        let blank = build_evolve_prompt(&sig, "<bpmn:definitions/>", Some("   "));
        assert!(!blank.contains("Operator guidance"));
    }

    #[test]
    fn parses_bare_json_array() {
        let raw = r#"[{"name":"a","rationale":"fewer steps","model":"<x/>"}]"#;
        let c = parse_structural_candidates(raw).unwrap();
        assert_eq!(c.len(), 1);
        assert_eq!(c[0].name, "a");
        assert_eq!(c[0].rationale.as_deref(), Some("fewer steps"));
        assert_eq!(c[0].model, "<x/>");
    }

    #[test]
    fn parses_fenced_json_with_prose() {
        let raw = "Sure! Here are my ideas:\n```json\n[{\"model\":\"<a/>\"},{\"name\":\"two\",\"model\":\"<b/>\"}]\n```\nHope that helps.";
        let c = parse_structural_candidates(raw).unwrap();
        assert_eq!(c.len(), 2);
        assert_eq!(c[0].name, "llm-candidate-1"); // name defaulted
        assert_eq!(c[1].name, "two");
    }

    #[test]
    fn drops_empty_models_and_errors_when_none_left() {
        let raw = r#"[{"name":"blank","model":"   "}]"#;
        assert!(parse_structural_candidates(raw).is_err());
    }

    #[test]
    fn errors_on_unparsable_output() {
        assert!(parse_structural_candidates("I cannot help with that.").is_err());
    }
}
