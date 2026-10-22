//! M3 — the **production (live-source, generation-skipped) path**.
//!
//! In test/demo the harness *generates* a base dataset by running mock workers
//! through the SimRunner (M0). In production there is nothing to generate: the
//! dataset already exists as the live cluster's traces. This module skips step 1
//! and reads the live dataset through the **T1 read contract** (`contracts.rs`),
//! folding it into the *baseline a candidate search must beat* for one process —
//! including the tail (p95/p99) and queue/service signals the
//! `processos-latent-process-exploration.md` analysis (§2) flags as the signals
//! that means alone hide.
//!
//! Candidate *evaluation* (steps 2–5) is unchanged and still runs on a runner:
//! the SimRunner for an offline what-if, or the M3 ClusterRunner for at-scale
//! realism. This module produces only the live baseline; it never writes to Nano.

use serde::Serialize;

use crate::contracts::NanoClient;
use crate::report::{self, Insights, ProcessInsight};

/// The live baseline for one process: the generation-skipped entry into the
/// optimize loop. It is the production analogue of [`super::rank::VariantResult`]'s
/// `baseline` — "here is what the process does today; beat it."
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProductionBaseline {
    /// Always `"live"` — distinguishes this from a SimRunner-generated baseline.
    pub source: String,
    pub nano_base_url: String,
    pub process_id: String,
    /// How many trace details were folded to build the baseline.
    pub sampled_instances: usize,
    /// The folded live performance of this process (per-element, with tails and
    /// the queue/service split carried by the T1 contract).
    pub baseline: ProcessInsight,
    /// What to do next: candidate evaluation needs a runner (Sim or Cluster).
    pub notes: Vec<String>,
}

/// Read the live dataset from Nano and fold it into a [`ProductionBaseline`] for
/// `process_id`. When `process_id` is `None`, the process with the most sampled
/// instances is chosen (the busiest is the most worth optimizing). Network I/O
/// is confined here; the fold/selection is the pure [`baseline_from_insights`].
pub async fn build_baseline(
    nano: &NanoClient,
    process_id: Option<&str>,
    limit: usize,
    sample: usize,
) -> Result<ProductionBaseline, String> {
    let insights = report::build(nano, limit, sample).await?;
    baseline_from_insights(insights, process_id)
}

/// Select one process from a folded [`Insights`] and frame it as the live
/// baseline. Pure (no I/O) so it is unit-testable without a cluster.
pub(crate) fn baseline_from_insights(
    insights: Insights,
    process_id: Option<&str>,
) -> Result<ProductionBaseline, String> {
    let Insights {
        nano_base_url,
        sampled_instances,
        processes,
        ..
    } = insights;

    if processes.is_empty() {
        return Err(
            "no live traces to build a baseline from; start some instances on the cluster \
             (or widen `limit`/`sample`)"
                .to_string(),
        );
    }

    let baseline = match process_id {
        Some(id) => processes
            .into_iter()
            .find(|p| p.process_id == id)
            .ok_or_else(|| format!("process id '{id}' has no sampled live traces"))?,
        // Busiest process = most sampled instances.
        None => processes
            .into_iter()
            .max_by_key(|p| p.instances)
            .ok_or_else(|| "no sampled live traces to derive a baseline from".to_string())?,
    };

    let mut notes = vec![
        "Live-source path: generation skipped; this baseline is folded from real \
         Nano traces over the public T1 contract."
            .to_string(),
        "To rank candidates against this baseline, run a candidate set through a \
         runner (SimRunner for an offline what-if, ClusterRunner for at-scale)."
            .to_string(),
    ];
    if let Some(b) = &baseline.bottleneck {
        notes.push(format!(
            "Headline bottleneck: element '{}' (avg {} ms self-duration).",
            b.element_id,
            b.avg_duration_ms.unwrap_or(0)
        ));
    }

    Ok(ProductionBaseline {
        source: "live".to_string(),
        nano_base_url,
        process_id: baseline.process_id.clone(),
        sampled_instances,
        baseline,
        notes,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::report::{ElementInsight, Insights, ProcessInsight, Totals};

    fn proc(id: &str, instances: usize, bottleneck_ms: u64) -> ProcessInsight {
        let bn = ElementInsight {
            element_id: "charge".into(),
            count: instances,
            avg_duration_ms: Some(bottleneck_ms),
            avg_queue_ms: Some(bottleneck_ms / 2),
            avg_service_ms: Some(bottleneck_ms / 2),
            incidents: 0,
            job_failures: 0,
        };
        ProcessInsight {
            process_id: id.into(),
            instances,
            completed: instances,
            incident_instances: 0,
            avg_duration_ms: Some(bottleneck_ms),
            p95_duration_ms: Some(bottleneck_ms),
            p99_duration_ms: Some(bottleneck_ms),
            bottleneck: Some(bn.clone()),
            elements: vec![bn],
        }
    }

    fn insights(processes: Vec<ProcessInsight>) -> Insights {
        Insights {
            generated_at_ms: 0,
            nano_base_url: "http://nano".into(),
            dataset_total: None,
            sampled_instances: processes.iter().map(|p| p.instances).sum(),
            totals: Totals::default(),
            live: None,
            processes,
            incident_clusters: Vec::new(),
            content: Default::default(),
        }
    }

    #[test]
    fn selects_the_named_process() {
        let i = insights(vec![proc("a", 3, 10), proc("b", 5, 80)]);
        let b = baseline_from_insights(i, Some("a")).unwrap();
        assert_eq!(b.process_id, "a");
        assert_eq!(b.source, "live");
        assert_eq!(b.baseline.instances, 3);
    }

    #[test]
    fn defaults_to_the_busiest_process() {
        let i = insights(vec![proc("a", 3, 10), proc("b", 5, 80)]);
        let b = baseline_from_insights(i, None).unwrap();
        assert_eq!(b.process_id, "b", "busiest (most sampled) is chosen");
        assert!(b.notes.iter().any(|n| n.contains("bottleneck")));
    }

    #[test]
    fn errors_when_named_process_absent() {
        let i = insights(vec![proc("a", 3, 10)]);
        assert!(baseline_from_insights(i, Some("missing")).is_err());
    }

    #[test]
    fn errors_when_no_live_traces() {
        let i = insights(vec![]);
        assert!(baseline_from_insights(i, None).is_err());
    }
}
