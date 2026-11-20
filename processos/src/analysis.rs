//! **LLM-driven data analysis over a trace dataset** (the open-ended sensing surface).
//!
//! The pre-built analyzers (`corpus::infer`, `harness::queueing`, `cluster`) encode
//! *our* priors — "here are the pathologies we anticipated". The research goal is the
//! opposite: can the cockpit droid *reason over the trace data and form its own
//! hypotheses*? To make the hypothesis space open, this module flattens a
//! [`TraceSource`] into in-memory **DuckDB** tables and exposes a **read-only,
//! row-capped** [`Analysis::query`] so the droid can write SQL, see the result, and
//! reason — instead of picking from a fixed menu.
//!
//! The schema is the *affordance*: it pre-derives the cheap temporal primitives
//! (`hour`, `dow`, `is_weekend`, a real `started_ts`) that steer the droid toward good
//! questions, **without** pre-bucketing into named windows (that labelling is exactly
//! the inference we want the droid to perform itself).
//!
//! Safety: the connection is in-memory and ephemeral, and `query` admits only a single
//! `SELECT`/`WITH` statement (DDL/DML keywords are rejected), so a tool call can read
//! but never mutate. The bundled DuckDB engine links no system library.

use duckdb::types::{TimeUnit, Value};
use duckdb::{params, Connection};

use crate::contracts::InstanceTrace;
use crate::dataset::TraceSource;

/// Hard cap on rows returned to the model (keeps the tool channel bounded — the droid
/// asks for aggregates, not raw dumps).
const MAX_ROWS: usize = 200;
/// Per-cell character cap (a stray wide column can't blow the context budget).
const MAX_CELL: usize = 240;

/// An in-memory analytic view of a trace dataset: three flat tables behind a read-only
/// SQL surface.
pub struct Analysis {
    conn: Connection,
    instance_count: usize,
    job_count: usize,
    incident_count: usize,
}

/// A capped query result, JSON-friendly for the LLM tool channel.
#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<String>>,
    /// Total rows the query produced (may exceed `rows.len()` when capped).
    pub row_count: usize,
    /// True when `row_count > rows.len()` — the model saw only the first `MAX_ROWS`.
    pub truncated: bool,
}

impl Analysis {
    /// Build the analytic view from already-materialised traces.
    pub fn build(traces: &[InstanceTrace]) -> Result<Self, String> {
        let conn = Connection::open_in_memory().map_err(|e| format!("duckdb open: {e}"))?;
        conn.execute_batch(SCHEMA)
            .map_err(|e| format!("duckdb schema: {e}"))?;

        let mut job_count = 0usize;
        let mut incident_count = 0usize;

        {
            let mut inst = conn
                .appender("instances")
                .map_err(|e| format!("appender instances: {e}"))?;
            for t in traces {
                let started = t.started_at;
                inst.append_row(params![
                    t.instance_key,
                    t.process_id,
                    t.version,
                    t.outcome,
                    started as i64,
                    hour_of(started) as i32,
                    dow_of(started) as i32,
                    is_weekend(started),
                    t.duration_ms.map(|d| d as i64),
                    t.incidents.len() as i32,
                ])
                .map_err(|e| format!("append instance: {e}"))?;
            }
            inst.flush().map_err(|e| format!("flush instances: {e}"))?;
        }

        {
            let mut jobs = conn
                .appender("jobs")
                .map_err(|e| format!("appender jobs: {e}"))?;
            for t in traces {
                let started = t.started_at;
                let mut seq = 0i32;
                for el in &t.elements {
                    let Some(job) = &el.job else { continue };
                    job_count += 1;
                    jobs.append_row(params![
                        t.instance_key,
                        t.process_id,
                        el.element_id,
                        job.job_type,
                        job.queue_ms.map(|q| q as i64),
                        job.service_ms.map(|s| s as i64),
                        job.failures as i32,
                        started as i64,
                        hour_of(started) as i32,
                        dow_of(started) as i32,
                        is_weekend(started),
                        t.outcome,
                        seq,
                    ])
                    .map_err(|e| format!("append job: {e}"))?;
                    seq += 1;
                }
            }
            jobs.flush().map_err(|e| format!("flush jobs: {e}"))?;
        }

        {
            let mut inc = conn
                .appender("incidents")
                .map_err(|e| format!("appender incidents: {e}"))?;
            for t in traces {
                let started = t.started_at;
                for incident in &t.incidents {
                    incident_count += 1;
                    inc.append_row(params![
                        t.instance_key,
                        incident.element_id,
                        incident.kind,
                        incident.reason,
                        started as i64,
                        hour_of(started) as i32,
                        dow_of(started) as i32,
                        is_weekend(started),
                    ])
                    .map_err(|e| format!("append incident: {e}"))?;
                }
            }
            inc.flush().map_err(|e| format!("flush incidents: {e}"))?;
        }

        // Add a real TIMESTAMP column derived from the epoch-ms so the droid can use
        // DuckDB's date functions (date_trunc, dayname, …) directly.
        conn.execute_batch(
            "ALTER TABLE instances ADD COLUMN started_ts TIMESTAMP;\n\
             UPDATE instances SET started_ts = make_timestamp(started_at * 1000);\n\
             ALTER TABLE jobs ADD COLUMN started_ts TIMESTAMP;\n\
             UPDATE jobs SET started_ts = make_timestamp(started_at * 1000);\n\
             ALTER TABLE incidents ADD COLUMN started_ts TIMESTAMP;\n\
             UPDATE incidents SET started_ts = make_timestamp(started_at * 1000);",
        )
        .map_err(|e| format!("derive timestamps: {e}"))?;

        Ok(Self {
            conn,
            instance_count: traces.len(),
            job_count,
            incident_count,
        })
    }

    /// Gather every trace from a source (bounded by `limit`) and build the view.
    pub async fn from_source(src: &TraceSource, limit: usize) -> Result<Self, String> {
        let summaries = src.list_traces(limit).await?;
        let mut traces = Vec::with_capacity(summaries.len());
        for s in &summaries {
            if let Ok(t) = src.trace(&s.instance_key).await {
                traces.push(t);
            }
        }
        if traces.is_empty() {
            return Err("no traces available to analyse".into());
        }
        Self::build(&traces)
    }

    pub fn instance_count(&self) -> usize {
        self.instance_count
    }
    pub fn job_count(&self) -> usize {
        self.job_count
    }
    pub fn incident_count(&self) -> usize {
        self.incident_count
    }

    /// The schema description handed to the model so it can author queries.
    pub fn schema_doc(&self) -> &'static str {
        SCHEMA_DOC
    }

    /// Export the three flat tables as `<dir>/{instances,jobs,incidents}.csv` (with
    /// headers). CSV is chosen over Parquet so a bare Python interpreter (stdlib `csv`)
    /// can read them with no `pyarrow`/`pandas` dependency, while a rich interpreter can
    /// still `read_csv`/`read_csv_auto` them.
    pub fn export_csv(&self, dir: &std::path::Path) -> Result<(), String> {
        for table in ["instances", "jobs", "incidents"] {
            let path = dir.join(format!("{table}.csv"));
            let sql = format!(
                "COPY (SELECT * FROM {table}) TO '{}' (HEADER, FORMAT CSV)",
                path.display()
            );
            self.conn
                .execute_batch(&sql)
                .map_err(|e| format!("export {table}.csv: {e}"))?;
        }
        Ok(())
    }

    /// Run one read-only `SELECT`/`WITH` query, capped to [`MAX_ROWS`].
    pub fn query(&self, sql: &str) -> Result<QueryResult, String> {
        let stmt_sql = guard_read_only(sql)?;

        let mut stmt = self
            .conn
            .prepare(&stmt_sql)
            .map_err(|e| format!("prepare failed: {e}"))?;

        // DuckDB only knows the result schema after execution, so run the query first,
        // then read column names from the executed statement behind `Rows`.
        let mut rows = stmt.query([]).map_err(|e| format!("execute failed: {e}"))?;
        let columns: Vec<String> = rows.as_ref().map(|s| s.column_names()).unwrap_or_default();
        let ncols = columns.len();

        let mut out: Vec<Vec<String>> = Vec::new();
        let mut row_count = 0usize;
        while let Some(row) = rows.next().map_err(|e| format!("row read failed: {e}"))? {
            row_count += 1;
            if out.len() < MAX_ROWS {
                let mut r = Vec::with_capacity(ncols);
                for i in 0..ncols {
                    let v: Value = row
                        .get(i)
                        .map_err(|e| format!("cell read failed (col {i}): {e}"))?;
                    r.push(clip(fmt_value(&v)));
                }
                out.push(r);
            }
        }

        Ok(QueryResult {
            truncated: row_count > out.len(),
            row_count,
            columns,
            rows: out,
        })
    }

    /// Fit the per-job-type load the queueing model needs, straight from the `jobs`
    /// table: the arrival count and the dataset span (for λ), the mean `service_ms`
    /// (for S), and the observed queue-wait quantiles (the status-quo tail the
    /// scaling is measured against). `job_type` filters to one type; `None` fits
    /// every type. Ordered worst-tail first. This is the measured input to
    /// [`crate::harness::queueing::scale_job`].
    pub fn job_loads(
        &self,
        job_type: Option<&str>,
    ) -> Result<Vec<crate::harness::queueing::JobLoad>, String> {
        // A single observation window (the whole-dataset job span) makes arrival
        // rates comparable across job types.
        let sql = "\
            WITH win AS (SELECT (max(started_at) - min(started_at)) AS span_ms FROM jobs) \
            SELECT job_type, \
                   count(*) AS samples, \
                   (SELECT span_ms FROM win) AS window_ms, \
                   CAST(round(avg(service_ms)) AS BIGINT) AS avg_service_ms, \
                   CAST(round(quantile_cont(queue_ms, 0.5)) AS BIGINT) AS p50_wait_ms, \
                   CAST(round(quantile_cont(queue_ms, 0.99)) AS BIGINT) AS p99_wait_ms, \
                   COALESCE(sum(failures), 0) AS failures \
            FROM jobs \
            WHERE (? IS NULL OR job_type = ?) \
            GROUP BY job_type \
            ORDER BY p99_wait_ms DESC NULLS LAST";

        let mut stmt = self
            .conn
            .prepare(sql)
            .map_err(|e| format!("prepare job_loads: {e}"))?;
        let mut rows = stmt
            .query(params![job_type, job_type])
            .map_err(|e| format!("execute job_loads: {e}"))?;

        // Non-null BIGINT columns read as i64; a job type with only null service/queue
        // (never happens for a real service task) clamps to 0 and is flagged unfittable
        // downstream. NUM-safe: negatives clamp to 0.
        let as_u64 = |v: i64| -> u64 { v.max(0) as u64 };
        let mut out = Vec::new();
        while let Some(row) = rows.next().map_err(|e| format!("job_loads row: {e}"))? {
            let job_type: String = row.get(0).map_err(|e| format!("job_type: {e}"))?;
            let samples: i64 = row.get(1).map_err(|e| format!("samples: {e}"))?;
            let window_ms: i64 = row.get(2).map_err(|e| format!("window_ms: {e}"))?;
            let service_ms: Option<i64> = row.get(3).map_err(|e| format!("service_ms: {e}"))?;
            let p50: Option<i64> = row.get(4).map_err(|e| format!("p50: {e}"))?;
            let p99: Option<i64> = row.get(5).map_err(|e| format!("p99: {e}"))?;
            let failures: i64 = row.get(6).map_err(|e| format!("failures: {e}"))?;
            out.push(crate::harness::queueing::JobLoad {
                job_type,
                samples: as_u64(samples),
                window_ms: as_u64(window_ms),
                service_ms: as_u64(service_ms.unwrap_or(0)),
                observed_p50_wait_ms: as_u64(p50.unwrap_or(0)),
                observed_p99_wait_ms: as_u64(p99.unwrap_or(0)),
                failures: as_u64(failures),
            });
        }
        Ok(out)
    }
}

const SCHEMA: &str = "\
CREATE TABLE instances (\
  instance_key   VARCHAR,\
  process_id     VARCHAR,\
  version        INTEGER,\
  outcome        VARCHAR,\
  started_at     BIGINT,\
  hour           INTEGER,\
  dow            INTEGER,\
  is_weekend     BOOLEAN,\
  duration_ms    BIGINT,\
  incident_count INTEGER\
);\
CREATE TABLE jobs (\
  instance_key     VARCHAR,\
  process_id       VARCHAR,\
  element_id       VARCHAR,\
  job_type         VARCHAR,\
  queue_ms         BIGINT,\
  service_ms       BIGINT,\
  failures         INTEGER,\
  started_at       BIGINT,\
  hour             INTEGER,\
  dow              INTEGER,\
  is_weekend       BOOLEAN,\
  instance_outcome VARCHAR,\
  seq              INTEGER\
);\
CREATE TABLE incidents (\
  instance_key VARCHAR,\
  element_id   VARCHAR,\
  kind         VARCHAR,\
  reason       VARCHAR,\
  started_at   BIGINT,\
  hour         INTEGER,\
  dow          INTEGER,\
  is_weekend   BOOLEAN\
);";

/// Documentation injected into the system prompt so the model knows the tables.
const SCHEMA_DOC: &str = "\
You can query an in-memory DuckDB over a captured trace dataset. Three tables:\n\
\n\
instances(instance_key, process_id, version, outcome, started_at /*epoch ms*/,\n\
          hour /*0-23 UTC*/, dow /*0=Mon..6=Sun*/, is_weekend, duration_ms,\n\
          incident_count, started_ts /*TIMESTAMP*/)\n\
jobs(instance_key, process_id, element_id, job_type, queue_ms /*wait before service*/,\n\
     service_ms /*busy time*/, failures, started_at, hour, dow, is_weekend,\n\
     instance_outcome, started_ts, seq /*0-based execution order within the instance*/)\n\
incidents(instance_key, element_id, kind, reason, started_at, hour, dow,\n\
          is_weekend, started_ts)\n\
\n\
One jobs row per executed service/user task; queue_ms is the wait in the worker pool\n\
(the tail signal for under-provisioning). Use DuckDB SQL: quantile_cont(x, 0.99),\n\
date_trunc, dayname(started_ts), corr(a,b), etc. Read-only: a single SELECT/WITH only.";

/// Validate the SQL is a single read-only statement and return it trimmed.
fn guard_read_only(sql: &str) -> Result<String, String> {
    // Strip a trailing semicolon, then reject any embedded statement separator.
    let trimmed = sql.trim().trim_end_matches(';').trim();
    if trimmed.is_empty() {
        return Err("empty query".into());
    }
    if trimmed.contains(';') {
        return Err("only a single statement is allowed".into());
    }
    let lower = trimmed.to_ascii_lowercase();
    if !(lower.starts_with("select") || lower.starts_with("with")) {
        return Err("only read-only SELECT/WITH queries are allowed".into());
    }
    // Word-level scan for mutating/side-effecting keywords (CTE-hidden writes etc.).
    const BANNED: &[&str] = &[
        "insert", "update", "delete", "drop", "create", "alter", "attach", "detach", "copy",
        "pragma", "install", "load", "export", "import", "call", "set", "truncate", "replace",
        "vacuum",
    ];
    for word in lower.split(|c: char| !c.is_ascii_alphanumeric() && c != '_') {
        if BANNED.contains(&word) {
            return Err(format!("disallowed keyword '{word}' in a read-only query"));
        }
    }
    Ok(trimmed.to_string())
}

fn hour_of(ms: u64) -> u32 {
    ((ms / 3_600_000) % 24) as u32
}

/// Day of week with Monday = 0. The unix epoch (1970-01-01) was a Thursday, so the
/// `+3` rotates Thursday(=0 naive) to its Mon=0 index of 3.
fn dow_of(ms: u64) -> u32 {
    (((ms / 86_400_000) + 3) % 7) as u32
}

fn is_weekend(ms: u64) -> bool {
    dow_of(ms) >= 5
}

fn clip(mut s: String) -> String {
    if s.len() > MAX_CELL {
        s.truncate(MAX_CELL);
        s.push('…');
    }
    s
}

fn fmt_value(v: &Value) -> String {
    match v {
        Value::Null => "NULL".to_string(),
        Value::Boolean(b) => b.to_string(),
        Value::TinyInt(n) => n.to_string(),
        Value::SmallInt(n) => n.to_string(),
        Value::Int(n) => n.to_string(),
        Value::BigInt(n) => n.to_string(),
        Value::HugeInt(n) => n.to_string(),
        Value::UTinyInt(n) => n.to_string(),
        Value::USmallInt(n) => n.to_string(),
        Value::UInt(n) => n.to_string(),
        Value::UBigInt(n) => n.to_string(),
        Value::Float(f) => format!("{f}"),
        Value::Double(f) => format!("{f}"),
        Value::Decimal(d) => d.to_string(),
        Value::Text(s) => s.clone(),
        Value::Timestamp(unit, n) => fmt_timestamp(*unit, *n),
        other => format!("{other:?}"),
    }
}

fn fmt_timestamp(unit: TimeUnit, n: i64) -> String {
    let secs = match unit {
        TimeUnit::Second => n as f64,
        TimeUnit::Millisecond => n as f64 / 1_000.0,
        TimeUnit::Microsecond => n as f64 / 1_000_000.0,
        TimeUnit::Nanosecond => n as f64 / 1_000_000_000.0,
    };
    format!("{secs:.3}s-epoch")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contracts::{Element, Incident, InstanceTrace, Job};

    fn job_el(id: &str, ty: &str, queue: u64, service: u64) -> Element {
        Element {
            element_id: id.into(),
            duration_ms: Some(queue + service),
            incidents: 0,
            job: Some(Job {
                job_type: ty.into(),
                queue_ms: Some(queue),
                service_ms: Some(service),
                failures: 0,
            }),
        }
    }

    fn trace(key: &str, started_at: u64, queue: u64) -> InstanceTrace {
        InstanceTrace {
            instance_key: key.into(),
            process_id: "loan-approval".into(),
            version: Some(1),
            outcome: "completed".into(),
            started_at,
            duration_ms: Some(queue + 1000),
            elements: vec![job_el("Task_CreditCheck", "credit-check", queue, 1000)],
            incidents: vec![],
            creation_variables: None,
            terminal_variables: None,
            stimuli: None,
            stimuli_truncated: false,
            domain_signals: vec![],
            outcome_truth: None,
        }
    }

    // 2024-01-01 00:00:00 UTC is a Monday.
    const MON_2024: u64 = 1_704_067_200_000;

    #[test]
    fn dow_and_hour_derivation_is_correct() {
        assert_eq!(dow_of(MON_2024), 0, "2024-01-01 is a Monday");
        assert_eq!(hour_of(MON_2024 + 9 * 3_600_000), 9);
        assert!(!is_weekend(MON_2024));
        assert!(is_weekend(MON_2024 + 5 * 86_400_000)); // Saturday
    }

    #[test]
    fn builds_tables_and_runs_an_aggregate_query() {
        let traces = vec![
            trace("1", MON_2024 + 9 * 3_600_000, 50_000), // Mon 09:00, big wait
            trace("2", MON_2024 + 9 * 3_600_000, 60_000), // Mon 09:00, big wait
            trace("3", MON_2024 + 2 * 3_600_000, 100),    // Mon 02:00, tiny wait
        ];
        let a = Analysis::build(&traces).expect("build");
        assert_eq!(a.instance_count(), 3);
        assert_eq!(a.job_count(), 3);

        let r = a
            .query(
                "SELECT hour, count(*) AS n, quantile_cont(queue_ms, 0.99) AS p99 \
                 FROM jobs GROUP BY hour ORDER BY p99 DESC",
            )
            .expect("query");
        assert_eq!(r.columns, vec!["hour", "n", "p99"]);
        // The 09:00 bucket has the largest tail.
        assert_eq!(r.rows[0][0], "9");
        let p99: f64 = r.rows[0][2].parse().unwrap();
        assert!(p99 > 50_000.0, "09:00 p99 should be large, got {p99}");
    }

    #[test]
    fn job_loads_fits_arrival_and_service_from_the_jobs_table() {
        // Two credit-check jobs 1s apart, each 1000ms service (see `trace`).
        let traces = vec![
            trace("1", MON_2024, 50_000),
            trace("2", MON_2024 + 1000, 60_000),
        ];
        let a = Analysis::build(&traces).expect("build");

        let all = a.job_loads(None).expect("job_loads");
        assert_eq!(all.len(), 1);
        let cc = &all[0];
        assert_eq!(cc.job_type, "credit-check");
        assert_eq!(cc.samples, 2);
        assert_eq!(cc.window_ms, 1000); // span between the two started_at
        assert_eq!(cc.service_ms, 1000); // mean service
        assert!(
            cc.observed_p99_wait_ms >= 59_000,
            "p99 wait should sit near the larger recorded wait, got {}",
            cc.observed_p99_wait_ms
        );

        // The filter isolates one job type; an unknown one yields nothing.
        assert_eq!(a.job_loads(Some("credit-check")).unwrap().len(), 1);
        assert!(a.job_loads(Some("nope")).unwrap().is_empty());
    }

    #[test]
    fn rejects_non_read_only_sql() {
        let a = Analysis::build(&[trace("1", MON_2024, 10)]).expect("build");
        assert!(a.query("DROP TABLE jobs").is_err());
        assert!(a.query("INSERT INTO jobs VALUES (1)").is_err());
        assert!(a.query("SELECT 1; DELETE FROM jobs").is_err());
        assert!(a.query("WITH x AS (SELECT 1) DELETE FROM jobs").is_err());
        assert!(a.query("SELECT count(*) FROM jobs").is_ok());
    }

    #[test]
    fn incidents_table_is_populated() {
        let mut t = trace("1", MON_2024, 10);
        t.incidents.push(Incident {
            element_id: "Task_CreditCheck".into(),
            kind: "CREDIT_BUREAU_ERROR".into(),
            reason: "bureau timeout".into(),
        });
        let a = Analysis::build(&[t]).expect("build");
        assert_eq!(a.incident_count(), 1);
        let r = a
            .query("SELECT kind, count(*) FROM incidents GROUP BY kind")
            .expect("query");
        assert_eq!(r.rows[0][0], "CREDIT_BUREAU_ERROR");
    }
}
