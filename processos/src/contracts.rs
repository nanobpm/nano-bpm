//! The **read contract** with Nano: typed mirrors of the gateway's public
//! `/console/api` trace + metrics DTOs, and a thin HTTP client that fetches them.
//!
//! These structs are deliberately a *copy* of the gateway's wire shapes (camelCase
//! JSON), not a shared crate import — that is what keeps the dependency one-way.
//! Nano owns the producer side; ProcessOS owns this consumer mirror and versions it
//! independently. If a field Nano emits is missing here, `serde` ignores it; if a
//! field we read is absent, it deserialises to `None`/default.
//!
//! Some mirrored fields aren't consumed by the Stage-T1 Insights fold yet (e.g.
//! `version`, `business_id`, `job_type`); they are kept so the contract type is
//! faithful and later stages can read them without re-deriving the shape.
#![allow(dead_code)]

use serde::Deserialize;

/// One row of `GET /console/api/traces` (a `trace::TraceSummaryDto`).
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TraceSummary {
    pub instance_key: String,
    pub process_id: String,
    pub version: Option<i32>,
    #[serde(default)]
    pub business_id: Option<String>,
    pub outcome: String,
    pub started_at: u64,
    #[serde(default)]
    pub ended_at: Option<u64>,
    #[serde(default)]
    pub duration_ms: Option<u64>,
    pub element_count: usize,
    pub incident_count: usize,
}

/// `GET /console/api/traces/{key}` (a `trace::InstanceTraceDto`).
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct InstanceTrace {
    pub instance_key: String,
    pub process_id: String,
    pub version: Option<i32>,
    pub outcome: String,
    #[serde(default)]
    pub started_at: u64,
    #[serde(default)]
    pub duration_ms: Option<u64>,
    #[serde(default)]
    pub elements: Vec<Element>,
    #[serde(default)]
    pub incidents: Vec<Incident>,
    /// Instance creation inputs (Tier-1 capture). Present only when the source
    /// node ran with `NANOBPMN_TRACE_VARIABLES`/`NANOBPMN_TRACE_STIMULI` (i.e.
    /// `c8 nano --capture`); `None` otherwise. Drives recorded-input replay.
    #[serde(default)]
    pub creation_variables: Option<Variables>,
    /// The instance's terminal ROOT variable state (#1739), present when the
    /// source captured variables and the instance has ended. Replay compares
    /// its terminal output against this, separately from the stimulus inputs.
    #[serde(default)]
    pub terminal_variables: Option<Variables>,
    /// The ordered Tier-2 recorded-input stimulus log. Present only under
    /// `NANOBPMN_TRACE_STIMULI`; `None` otherwise.
    #[serde(default)]
    pub stimuli: Option<Vec<Stimulus>>,
    /// True when the per-instance stimulus cap dropped later inputs — the log is
    /// then incomplete and the instance is not safe to replay.
    #[serde(default)]
    pub stimuli_truncated: bool,
    /// Application-domain **content** signals projected onto the trace (ProcessOS
    /// domain-signal plane, `docs/processos-domain-signal-plane.md` §2). Additive
    /// and absent-safe: a trace that projects none deserializes exactly as before
    /// (empty vec). The reasoner keys on the generic `role`/`scope` here, never on
    /// an app's own `kind` — see [`DomainSignal`].
    #[serde(default)]
    pub domain_signals: Vec<DomainSignal>,
    /// The delayed correctness/value signal (outcome-truth, doc 3 §2), when the app
    /// supplies a richer one than the instance's terminal `outcome`. Absent by
    /// default; consumers fall back to `outcome` when this is `None`.
    #[serde(default)]
    pub outcome_truth: Option<OutcomeTruth>,
}

/// The **generic, closed** role vocabulary the reasoner keys on — the domain-free
/// projection of an app's own signal `kind` (doc #6 §2.2). Patterns are expressed
/// over `role`, never over app `kind`s, so the reasoner stays domain-agnostic. An
/// unknown/unmapped app role deserializes to [`Role::Note`] (`#[serde(other)]`), so
/// a new app kind can never leak an unhandled variant into the reasoner.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, serde::Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub enum Role {
    /// A durable fact/learning discovered during execution (e.g. blackboard `learning`).
    Knowledge,
    /// An assertion of ownership/intent over a resource (e.g. a file `claim`).
    Claim,
    /// A constraint/decision that narrows later work (e.g. `constraint-change`).
    Constraint,
    /// An observed defect/failure class.
    Defect,
    /// A handoff of work/context between actors.
    Handoff,
    /// A decision record.
    Decision,
    /// The catch-all: an unclassified note, and the fallback for any unknown app role.
    #[default]
    #[serde(other)]
    Note,
}

impl Role {
    /// The stable lowercase label for this role (matches the serde wire form).
    pub fn as_str(self) -> &'static str {
        match self {
            Role::Knowledge => "knowledge",
            Role::Claim => "claim",
            Role::Constraint => "constraint",
            Role::Defect => "defect",
            Role::Handoff => "handoff",
            Role::Decision => "decision",
            Role::Note => "note",
        }
    }
}

/// A generic scope bag: *what* a signal is about, in domain-free dimensions. This is
/// what lets the reasoner express sibling-scope patterns — "repeated across sibling
/// `actor`s within one `instance`" (redundant-recompute / contention) — with no
/// domain word (doc #6 §1.2). Extra generic dims (e.g. `wave`) are preserved in
/// `extra` rather than requiring a named field.
#[derive(Debug, Clone, Deserialize, serde::Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct Scope {
    /// The process instance the signal is about (the ambient `__instance` join key).
    #[serde(default)]
    pub instance: Option<String>,
    /// The BPMN element the signal is about, when narrower than the instance.
    #[serde(default)]
    pub element: Option<String>,
    /// The actor/worker the signal is about (e.g. nano-workforce `author_task`).
    #[serde(default)]
    pub actor: Option<String>,
    /// A partition/cohort dimension (e.g. a `wave`), when the app scopes by one.
    #[serde(default)]
    pub partition: Option<String>,
    /// Any further generic scope dimensions the app supplies (e.g. `wave` as a
    /// number), preserved verbatim without inventing a named field per domain.
    #[serde(flatten, default)]
    pub extra: std::collections::BTreeMap<String, serde_json::Value>,
}

/// One application-domain content signal projected onto a trace (doc #6 §2). It is
/// **observation, never replay input** (blackboard/domain content is non-replayable).
/// Two generic dimensions — [`Role`] and [`Scope`] — carry the reasoning weight and
/// keep the reasoner domain-free.
#[derive(Debug, Clone, Deserialize, serde::Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct DomainSignal {
    /// App-declared namespace, e.g. `"nano-workforce/blackboard"`.
    #[serde(default)]
    pub ns: String,
    /// The app's own signal kind (its domain vocabulary), mapped onto `role`.
    #[serde(default)]
    pub kind: String,
    /// The generic role the reasoner keys on (unknown → [`Role::Note`]).
    #[serde(default)]
    pub role: Role,
    /// The generic scope: which instance/element/actor/partition the signal is about.
    #[serde(default)]
    pub scope: Scope,
    /// Best-effort BPMN element anchor (may be absent).
    #[serde(default)]
    pub element_id: Option<String>,
    /// Signal timestamp (ms since epoch), when known.
    #[serde(default)]
    pub at: Option<u64>,
    /// Free-text payload / description.
    #[serde(default)]
    pub body: Option<String>,
    /// Idempotency key mirroring the app's own dedupe key, when present.
    #[serde(default)]
    pub dedupe_key: Option<String>,
    /// Origin marker distinguishing **measured** evidence from an **agent prior**
    /// (e.g. `"measured"`, `"blackboard"`, `"agent-retro"`). A non-measured origin
    /// enters the reasoner strictly as a **prior that raises a pattern's prior** and
    /// must be confirmed by measured cross-corpus evidence + `outcomeTruth` before any
    /// suggestion — measured evidence and agent priors are never conflated
    /// ("retro nominates, ProcessOS measures, human decides"). Absent → treated as
    /// measured/observed.
    #[serde(default)]
    pub provenance: Option<String>,
    /// Prior strength in `0.0..=1.0` for a hypothesis-origin signal; absent for
    /// measured observations.
    #[serde(default)]
    pub confidence: Option<f64>,
}

impl DomainSignal {
    /// True when this signal is measured/observed evidence rather than an agent
    /// prior/hypothesis. Absent provenance ⇒ measured. Origins mentioning `agent`,
    /// `retro`, `hypothesis`, or `prior` are treated as non-measured priors, which
    /// the reasoner must confirm with measured evidence before acting on.
    pub fn is_measured(&self) -> bool {
        match self.provenance.as_deref() {
            None => true,
            Some(p) => {
                let p = p.to_ascii_lowercase();
                !(p.contains("agent")
                    || p.contains("retro")
                    || p.contains("hypothesis")
                    || p.contains("prior"))
            }
        }
    }
}

/// The delayed correctness/value signal for an instance (doc #6 §2, doc 3 §2). All
/// fields optional and absent-safe; consumers fall back to the instance `outcome`
/// when the whole struct is `None`.
#[derive(Debug, Clone, Deserialize, serde::Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct OutcomeTruth {
    /// A richer status than the terminal `outcome`, e.g. `merged | escalated | abandoned`.
    #[serde(default)]
    pub status: Option<String>,
    /// How many rounds the instance took to converge, when meaningful.
    #[serde(default)]
    pub rounds_to_converge: Option<u32>,
    /// Count of rework events observed on the instance.
    #[serde(default)]
    pub rework_events: Option<u32>,
    /// An optional scalar objective value for the §6 objective.
    #[serde(default)]
    pub value: Option<f64>,
}

/// A captured variable map on a trace (mirrors the gateway's `VariablesDto`).
/// When the snapshot exceeded the node's byte cap, `values` is `None` and
/// `truncated` is true — the instance is then not replayable.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Variables {
    #[serde(default)]
    pub truncated: bool,
    #[serde(default)]
    pub bytes: usize,
    #[serde(default)]
    pub values: Option<serde_json::Value>,
}

/// One recorded external input on the Tier-2 log (mirrors `StimulusDto`). `kind`
/// is one of `jobCompleted` | `userTaskCompleted` | `message` | `timer` |
/// `variablesSet`; `reference` is the job *type* for `jobCompleted` (so replay
/// matches by semantic type, not element position) and the element id for
/// message/timer.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Stimulus {
    pub seq: u32,
    pub at: u64,
    pub kind: String,
    #[serde(default)]
    pub reference: Option<String>,
    #[serde(default)]
    pub variables: Option<Variables>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Element {
    pub element_id: String,
    #[serde(default)]
    pub duration_ms: Option<u64>,
    #[serde(default)]
    pub incidents: u32,
    #[serde(default)]
    pub job: Option<Job>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Job {
    #[serde(rename = "type")]
    pub job_type: String,
    #[serde(default)]
    pub queue_ms: Option<u64>,
    #[serde(default)]
    pub service_ms: Option<u64>,
    #[serde(default)]
    pub failures: u32,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Incident {
    pub element_id: String,
    pub kind: String,
    pub reason: String,
}

/// The subset of `GET /console/api/metrics` (a `MetricsDto`) ProcessOS surfaces as
/// live gauges. Unused fields are ignored by `serde`. It is re-emitted verbatim in
/// the Insights document, so it is `Serialize` as well as `Deserialize`.
#[derive(Debug, Clone, Deserialize, serde::Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct Metrics {
    pub timestamp_ms: u64,
    pub active_instances: i64,
    pub creates_total: u64,
    pub completions_total: u64,
    #[serde(default)]
    pub connections_active: i64,
    #[serde(default)]
    pub resident_bytes: Option<u64>,
}

/// `GET /v2/topology` — the cluster's brokers. ProcessOS reads it to discover the
/// per-node console endpoints, because trace data is **partition-local**: a single
/// node's `/console/api/traces` only sees the instances its partitions own, so a
/// cluster-wide measurement must union every node.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Topology {
    #[serde(default)]
    pub brokers: Vec<Broker>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Broker {
    pub node_id: i32,
    pub host: String,
    pub port: u16,
}

/// HTTP client for Nano's public read contract. One-way: it only ever issues GETs
/// against the gateway's `/console/api` surface — never the journal, read DB, or
/// engine internals.
#[derive(Clone)]
pub struct NanoClient {
    base_url: String,
    http: reqwest::Client,
}

impl NanoClient {
    pub fn new(base_url: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into().trim_end_matches('/').to_string(),
            http: reqwest::Client::new(),
        }
    }

    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    /// `GET /console/api/traces?limit=` — recent instance trace summaries.
    pub async fn list_traces(&self, limit: usize) -> Result<Vec<TraceSummary>, String> {
        let url = format!("{}/console/api/traces?limit={}", self.base_url, limit);
        self.get_json(&url).await
    }

    /// `GET /console/api/traces/{key}` — one instance's full canonical trace.
    pub async fn trace(&self, instance_key: &str) -> Result<InstanceTrace, String> {
        let url = format!("{}/console/api/traces/{}", self.base_url, instance_key);
        self.get_json(&url).await
    }

    /// `GET /console/api/metrics` — live node gauges.
    pub async fn metrics(&self) -> Result<Metrics, String> {
        let url = format!("{}/console/api/metrics", self.base_url);
        self.get_json(&url).await
    }

    /// `GET /v2/topology` — the cluster's brokers, used to discover per-node
    /// console endpoints for a cluster-wide (all-partition) trace union.
    pub async fn topology(&self) -> Result<Topology, String> {
        let url = format!("{}/v2/topology", self.base_url);
        self.get_json(&url).await
    }

    /// POST `path` (relative to the base URL) with a JSON body and decode the JSON
    /// response. Used by the cockpit to drive the control surface (create pilot
    /// instances, search user tasks / variables) over Nano's public v2 REST API.
    pub async fn post_json<T: for<'de> Deserialize<'de>>(
        &self,
        path: &str,
        body: &serde_json::Value,
    ) -> Result<T, String> {
        let url = format!("{}{}", self.base_url, path);
        let res = self
            .http
            .post(&url)
            .json(body)
            .send()
            .await
            .map_err(|e| format!("POST {url}: {e}"))?;
        if !res.status().is_success() {
            return Err(format!("POST {url}: HTTP {}", res.status()));
        }
        res.json::<T>()
            .await
            .map_err(|e| format!("decode {url}: {e}"))
    }

    /// POST `path` expecting a success status with no body of interest (e.g. a
    /// `204` user-task completion). Returns `Ok(())` on any 2xx.
    pub async fn post_no_content(
        &self,
        path: &str,
        body: &serde_json::Value,
    ) -> Result<(), String> {
        let url = format!("{}{}", self.base_url, path);
        let res = self
            .http
            .post(&url)
            .json(body)
            .send()
            .await
            .map_err(|e| format!("POST {url}: {e}"))?;
        if !res.status().is_success() {
            return Err(format!("POST {url}: HTTP {}", res.status()));
        }
        Ok(())
    }

    /// GET `path` (relative to the base URL) and decode the JSON response. Used to
    /// fetch a deployed process definition's BPMN XML for forking an experiment.
    pub async fn get_path<T: for<'de> Deserialize<'de>>(&self, path: &str) -> Result<T, String> {
        let url = format!("{}{}", self.base_url, path);
        self.get_json(&url).await
    }

    /// GET `path` (relative to the base URL) and return the raw response body as a
    /// string (the process-definition XML endpoint returns `text/xml`, not JSON).
    pub async fn get_text(&self, path: &str) -> Result<String, String> {
        let url = format!("{}{}", self.base_url, path);
        let res = self
            .http
            .get(&url)
            .send()
            .await
            .map_err(|e| format!("GET {url}: {e}"))?;
        if !res.status().is_success() {
            return Err(format!("GET {url}: HTTP {}", res.status()));
        }
        res.text().await.map_err(|e| format!("read {url}: {e}"))
    }

    async fn get_json<T: for<'de> Deserialize<'de>>(&self, url: &str) -> Result<T, String> {
        let res = self
            .http
            .get(url)
            .send()
            .await
            .map_err(|e| format!("GET {url}: {e}"))?;
        if !res.status().is_success() {
            return Err(format!("GET {url}: HTTP {}", res.status()));
        }
        res.json::<T>()
            .await
            .map_err(|e| format!("decode {url}: {e}"))
    }

    /// Liveness probe used by the supervisor while waiting for a freshly-spawned
    /// own engine to start serving. `true` iff `GET /v2/topology` returns 2xx.
    pub async fn health_ok(&self) -> bool {
        let url = format!("{}/v2/topology", self.base_url);
        matches!(self.http.get(&url).send().await, Ok(r) if r.status().is_success())
    }

    /// `POST /v2/deployments` (multipart, field `resources`) — deploy a single BPMN
    /// resource. Used by the supervisor to install the pilot process on the own
    /// engine after it boots. Idempotent on the engine side (a byte-identical
    /// redeploy is a no-op), so it is safe to call on every startup.
    pub async fn deploy_bpmn(
        &self,
        filename: &str,
        xml: &str,
    ) -> Result<serde_json::Value, String> {
        let url = format!("{}/v2/deployments", self.base_url);
        let part = reqwest::multipart::Part::text(xml.to_string())
            .file_name(filename.to_string())
            .mime_str("application/xml")
            .map_err(|e| format!("multipart part: {e}"))?;
        let form = reqwest::multipart::Form::new().part("resources", part);
        let res = self
            .http
            .post(&url)
            .multipart(form)
            .send()
            .await
            .map_err(|e| format!("POST {url}: {e}"))?;
        if !res.status().is_success() {
            return Err(format!("POST {url}: HTTP {}", res.status()));
        }
        res.json::<serde_json::Value>()
            .await
            .map_err(|e| format!("decode {url}: {e}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn instance_trace_roundtrips_with_domain_signals_and_outcome_truth() {
        let json = serde_json::json!({
            "instanceKey": "i1",
            "processId": "p",
            "outcome": "completed",
            "startedAt": 0,
            "domainSignals": [{
                "ns": "nano-workforce/blackboard",
                "kind": "claim",
                "role": "claim",
                "scope": { "instance": "i1", "actor": "author_task", "wave": 2 },
                "dedupeKey": "k1",
                "provenance": "measured"
            }],
            "outcomeTruth": { "status": "merged", "roundsToConverge": 3, "reworkEvents": 1 }
        });
        let t: InstanceTrace = serde_json::from_value(json).unwrap();
        assert_eq!(t.domain_signals.len(), 1);
        let s = &t.domain_signals[0];
        assert_eq!(s.role, Role::Claim);
        assert_eq!(s.scope.actor.as_deref(), Some("author_task"));
        // A generic extra scope dim is preserved verbatim, not dropped.
        assert_eq!(s.scope.extra.get("wave"), Some(&serde_json::json!(2)));
        assert!(s.is_measured());
        assert_eq!(
            t.outcome_truth.as_ref().unwrap().rounds_to_converge,
            Some(3)
        );
    }

    #[test]
    fn instance_trace_without_the_slot_is_absent_safe() {
        let json = serde_json::json!({
            "instanceKey": "i1", "processId": "p", "outcome": "active", "startedAt": 0
        });
        let t: InstanceTrace = serde_json::from_value(json).unwrap();
        assert!(t.domain_signals.is_empty());
        assert!(t.outcome_truth.is_none());
    }

    #[test]
    fn unknown_app_role_deserializes_to_note() {
        let s: DomainSignal =
            serde_json::from_value(serde_json::json!({ "role": "some-app-specific-role" }))
                .unwrap();
        assert_eq!(s.role, Role::Note);
    }

    #[test]
    fn provenance_marks_agent_origin_as_unmeasured() {
        let measured: DomainSignal = serde_json::from_value(serde_json::json!({})).unwrap();
        assert!(measured.is_measured());
        for p in ["agent-retro", "hypothesis", "PRIOR", "agent"] {
            let s: DomainSignal =
                serde_json::from_value(serde_json::json!({ "provenance": p })).unwrap();
            assert!(!s.is_measured(), "{p} should be a prior");
        }
    }
}
