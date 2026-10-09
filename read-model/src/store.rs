//! The SQLite-backed read model (the query side of a CQRS split).
//!
//! Camunda 8 keeps the broker's execution state separate from the data Operate
//! and the Query API read: an exporter streams the record log into an external
//! store, and reads are served from there, eventually consistent. This module is
//! that store for nanobpmn. The engine's hot [`crate::journal::Journal`] holds
//! only *live* execution state (completed instances are evicted once exported);
//! every `search*`/`get*` query is answered from here instead.
//!
//! The store is a pure projection of the engine's event stream: each event is
//! upserted into denormalized tables (so a row already carries the
//! process-definition identity a result needs, with no cross-table joins at read
//! time). Because it is fully derived, it can always be rebuilt by replaying the
//! journal — the journal remains the single source of truth. A persisted
//! `exported_position` lets a warm restart skip rows it already holds; if the
//! database is missing, stale, or a schema mismatch, it is recreated and
//! rebuilt from scratch.
//!
//! A non-persistent (`:memory:`) store backs the engine's in-memory mode so
//! reads still work while nothing is persisted.

use std::path::{Path, PathBuf};
use std::sync::Mutex;

use nanobpmn_engine_core::{
    AgentHistoryCommitStatus, AgentHistoryContent, AgentHistoryContentType, AgentHistoryRecord,
    AgentHistoryRole, AgentInstance, AgentInstanceStatus, Event, IncidentKind, IncidentState,
    JobKind, JobState, Key, ListenerEventType, ProcessInstanceState, TaskListenerEventType,
    UserTaskState, Value, partition_of,
};
use rusqlite::{Connection, OptionalExtension, params};

use crate::backend;

/// Monotonic read-model schema version, recorded in `meta(schema_version)`.
///
/// **Bump this by one whenever [`SCHEMA`] changes** (the CI drift guard
/// `schema_edit_requires_version_bump` fails the build if you forget). It lets an
/// already-current database short-circuit the additive reconcile on open, and it
/// is the monotonic ladder the issue #831 fix is built around.
const SCHEMA_VERSION: i64 = 11;
/// The schema version that introduced `event_waits`.
const EVENT_WAITS_SCHEMA_VERSION: i64 = 9;
/// `meta` key flagging that `event_waits` awaits an engine-state backfill.
const EVENT_WAITS_BACKFILL_KEY: &str = "event_waits_backfill_pending";

/// The content fingerprint of [`SCHEMA`] as of the current [`SCHEMA_VERSION`].
///
/// This is **only** a CI/test drift assertion — the guard test
/// `schema_edit_requires_version_bump` asserts `schema_fingerprint()` still
/// equals this constant, so any edit to `SCHEMA` fails the build until the author
/// bumps [`SCHEMA_VERSION`] and refreshes this value. It is **never** a runtime
/// wipe trigger (that destructive behaviour was the root cause of issue #831).
#[cfg(test)]
const SCHEMA_FINGERPRINT: i64 = 9054396959573802666;

/// The read model is a SQLite projection of the engine's event stream. Its
/// on-disk schema used to be identified by a content fingerprint of [`SCHEMA`],
/// and **any** edit to `SCHEMA` (even a purely additive column) made
/// [`ReadStore::ensure_schema`] DROP every table and recreate from scratch on the
/// next open. That was the root cause of issue #831: once the journal has
/// compacted (the steady state), a wiped read model sits below the compaction
/// floor, so the #732 boot recovery can only reproject *live* instances from the
/// engine snapshot — every completed/terminal instance, which lived **only** in
/// the read model, is silently and unrecoverably lost. It recurred on every
/// schema-changing release.
///
/// The schema now evolves via **non-destructive additive migration**
/// ([`ReadStore::reconcile_to_schema`]): on open the live database is brought
/// *up to* [`SCHEMA`] by adding any missing tables, columns and indexes
/// (`CREATE TABLE`, `ALTER TABLE ADD COLUMN`, `CREATE INDEX`) — **existing rows
/// are never dropped**. The target shape is *derived* from `SCHEMA` itself
/// (introspected from a throwaway in-memory database built from it), so there is
/// no hand-maintained migration ladder to drift out of sync: adding a column or
/// table to `SCHEMA` *is* the migration. Destructive rebuild is reserved for the
/// explicit [`ReadStore::reset`] path (a corrupt/truncated journal forcing a full
/// replay), where a reprojection restores the data anyway.
fn schema_fingerprint() -> i64 {
    fnv1a_64(SCHEMA.as_bytes())
}

/// FNV-1a (64-bit). Split out from [`schema_fingerprint`] so the hash itself is
/// unit-testable against known vectors and can't silently change behaviour.
fn fnv1a_64(bytes: &[u8]) -> i64 {
    const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;
    let mut hash = FNV_OFFSET;
    for byte in bytes {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash as i64
}

pub(crate) const SCHEMA: &str = "
CREATE TABLE process_definitions (
    key        INTEGER PRIMARY KEY,
    process_id TEXT NOT NULL,
    version    INTEGER NOT NULL,
    name       TEXT,
    xml        TEXT NOT NULL DEFAULT '',
    start_form_id TEXT
);
-- UNIQUE enforces one row per (process_id, version) so a redeploy of the same
-- version can never create ambiguous \"latest version per id\" rows, and the
-- index also backs the MAX(version)/ORDER BY lookups below.
CREATE UNIQUE INDEX idx_process_definitions_id_version
    ON process_definitions(process_id, version);
CREATE TABLE process_instances (
    key                    INTEGER PRIMARY KEY,
    process_id             TEXT NOT NULL,
    process_definition_id  TEXT NOT NULL,
    process_definition_key TEXT NOT NULL,
    version                INTEGER NOT NULL,
    state                  INTEGER NOT NULL,
    start_date_ms          INTEGER NOT NULL,
    has_incident           INTEGER NOT NULL,
    tags                   TEXT NOT NULL,
    business_id            TEXT,
    parent_process_instance_key INTEGER,
    parent_element_instance_key INTEGER,
    suspended_date_ms INTEGER
);
CREATE TABLE jobs (
    business_id            TEXT,
    key                    INTEGER PRIMARY KEY,
    instance_key           INTEGER NOT NULL,
    element_instance_key   INTEGER NOT NULL,
    element_id             TEXT NOT NULL,
    job_type               TEXT NOT NULL,
    state                  INTEGER NOT NULL,
    retries                INTEGER NOT NULL,
    worker                 TEXT,
    deadline_ms            INTEGER,
    process_definition_id  TEXT NOT NULL,
    process_definition_key TEXT NOT NULL,
    job_kind               INTEGER NOT NULL DEFAULT 0,
    listener_event_type    INTEGER NOT NULL DEFAULT 0,
    created_at_ms          INTEGER NOT NULL DEFAULT 0,
    -- Job timing for the Camunda-parity read model (#1344). `last_update_ms` is
    -- the record-timestamp of the most recent projected job event (CREATED and
    -- every subsequent FAILED / TIMED_OUT / RETRIES_UPDATED / TIMEOUT_UPDATED /
    -- ERROR_THROWN / COMPLETED / CANCELED — activation is NOT projected, matching
    -- Camunda). `end_ms` is the record-timestamp of the active→terminal
    -- COMPLETED / CANCELED transition only (failures, timeouts and errors leave it
    -- NULL). Both are NULL for rows migrated from a pre-#1344 database.
    last_update_ms         INTEGER,
    end_ms                 INTEGER,
    -- Declared read-set (`fetchVariables`) recorded on the durable
    -- `JobActivated` event: the variable names the worker asked for on the most
    -- recent activation that declared a non-empty set. Preserved across a later
    -- fetch-all re-activation (not overwritten by a declaration-free activation).
    -- Engine-native read provenance for reification (issue #986). A JSON array;
    -- '[]' means no durable activation has declared a set (fetch-all / undeclared
    -- reads) or the job has not been activated with a declared set.
    read_set               TEXT NOT NULL DEFAULT '[]',
    lease_token            TEXT,
    -- Zeebe job `errorMessage` / `errorCode` / `hasFailedWithRetriesLeft`
    -- (#1327): the last worker-reported failure/thrown-error message, the last
    -- thrown error code, and whether the last FAILED / ERROR_THROWN left
    -- retries > 0.
    error_message          TEXT,
    error_code             TEXT,
    has_failed_with_retries_left INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE incidents (
    key                    INTEGER PRIMARY KEY,
    instance_key           INTEGER NOT NULL,
    element_instance_key   INTEGER NOT NULL,
    element_id             TEXT NOT NULL,
    kind                   INTEGER NOT NULL,
    state                  INTEGER NOT NULL,
    reason                 TEXT NOT NULL,
    job_key                INTEGER,
    created_at_ms          INTEGER NOT NULL,
    process_definition_id  TEXT NOT NULL,
    process_definition_key TEXT NOT NULL
);
CREATE TABLE meta (k TEXT PRIMARY KEY, v INTEGER NOT NULL);
CREATE TABLE user_tasks (
    business_id            TEXT,
    key                    INTEGER PRIMARY KEY,
    instance_key           INTEGER NOT NULL,
    element_instance_key   INTEGER NOT NULL,
    element_id             TEXT NOT NULL,
    state                  INTEGER NOT NULL,
    assignee               TEXT,
    candidate_groups       TEXT NOT NULL DEFAULT '[]',
    candidate_users        TEXT NOT NULL DEFAULT '[]',
    due_date               TEXT,
    follow_up_date         TEXT,
    priority               INTEGER NOT NULL DEFAULT 50,
    created_at_ms          INTEGER NOT NULL,
    process_definition_id  TEXT NOT NULL,
    process_definition_key TEXT NOT NULL,
    process_definition_version INTEGER NOT NULL,
    form_key               INTEGER,
    external_form_reference TEXT
);
CREATE TABLE variables (
    key                    INTEGER PRIMARY KEY AUTOINCREMENT,
    instance_key           INTEGER NOT NULL,
    scope_key              INTEGER NOT NULL,
    name                   TEXT NOT NULL,
    value                  TEXT NOT NULL,
    process_definition_id  TEXT NOT NULL,
    process_definition_key TEXT NOT NULL,
    UNIQUE(scope_key, name)
);
CREATE TABLE decision_requirements (
    drg_id        TEXT PRIMARY KEY,
    drg_key       INTEGER NOT NULL,
    name          TEXT NOT NULL,
    version       INTEGER NOT NULL,
    resource_name TEXT NOT NULL DEFAULT '',
    xml           TEXT NOT NULL DEFAULT ''
);
CREATE TABLE decision_definitions (
    decision_id                   TEXT PRIMARY KEY,
    decision_key                  INTEGER NOT NULL,
    name                          TEXT NOT NULL,
    version                       INTEGER NOT NULL,
    decision_requirements_key     INTEGER NOT NULL,
    decision_requirements_id      TEXT NOT NULL,
    decision_requirements_name    TEXT NOT NULL DEFAULT '',
    decision_requirements_version INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE decision_instances (
    business_id            TEXT,
    eval_instance_key         TEXT PRIMARY KEY,
    decision_evaluation_key   INTEGER NOT NULL,
    idx                       INTEGER NOT NULL,
    decision_id               TEXT NOT NULL,
    decision_key              INTEGER NOT NULL,
    decision_name             TEXT NOT NULL,
    decision_type             TEXT NOT NULL,
    version                   INTEGER NOT NULL,
    decision_requirements_id  TEXT NOT NULL,
    decision_requirements_key INTEGER NOT NULL,
    root_decision_key         INTEGER NOT NULL,
    instance_key              INTEGER NOT NULL,
    element_instance_key      INTEGER NOT NULL,
    process_definition_key    TEXT NOT NULL DEFAULT '',
    state                     TEXT NOT NULL,
    evaluation_failure        TEXT,
    evaluation_date_ms        INTEGER NOT NULL,
    result_json               TEXT NOT NULL,
    inputs_json               TEXT NOT NULL,
    rules_json                TEXT NOT NULL,
    tenant_id                 TEXT NOT NULL
);
CREATE TABLE definition_elements (
    process_definition_key INTEGER NOT NULL,
    element_id             TEXT NOT NULL,
    element_type           TEXT NOT NULL,
    element_name           TEXT,
    PRIMARY KEY (process_definition_key, element_id)
);
CREATE TABLE element_instances (
    element_instance_key   INTEGER PRIMARY KEY,
    instance_key           INTEGER NOT NULL,
    process_definition_id  TEXT NOT NULL,
    process_definition_key TEXT NOT NULL,
    element_id             TEXT NOT NULL,
    element_name           TEXT,
    element_type           TEXT NOT NULL,
    state                  INTEGER NOT NULL,
    start_date_ms          INTEGER NOT NULL,
    end_date_ms            INTEGER,
    scope_key              INTEGER NOT NULL DEFAULT 0,
    incident_key           INTEGER,
    has_incident           INTEGER NOT NULL DEFAULT 0,
    tenant_id              TEXT NOT NULL DEFAULT '<default>'
);
CREATE INDEX idx_element_instances_instance ON element_instances(instance_key);
CREATE TABLE message_subscriptions (
    business_id            TEXT,
    subscription_key       INTEGER PRIMARY KEY,
    instance_key           INTEGER NOT NULL,
    element_instance_key   INTEGER NOT NULL,
    element_id             TEXT NOT NULL,
    message_name           TEXT NOT NULL,
    correlation_key        TEXT NOT NULL,
    created_at_ms          INTEGER NOT NULL DEFAULT 0,
    non_interrupting       INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX idx_message_subscriptions_instance ON message_subscriptions(instance_key);
CREATE TABLE event_waits (
    wait_key               INTEGER PRIMARY KEY,
    wait_type              TEXT NOT NULL,
    instance_key           INTEGER NOT NULL,
    element_instance_key   INTEGER NOT NULL,
    element_id             TEXT NOT NULL,
    detail                 TEXT NOT NULL,
    due_at_ms              INTEGER,
    non_interrupting       INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX idx_event_waits_instance ON event_waits(instance_key);
CREATE TABLE correlated_message_subscriptions (
    business_id            TEXT,
    message_key            INTEGER NOT NULL,
    subscription_key       INTEGER NOT NULL,
    instance_key           INTEGER NOT NULL,
    element_instance_key   INTEGER NOT NULL,
    element_id             TEXT NOT NULL,
    message_name           TEXT NOT NULL,
    correlation_key        TEXT NOT NULL,
    correlation_time_ms    INTEGER NOT NULL,
    partition_id           INTEGER NOT NULL,
    PRIMARY KEY (message_key, subscription_key)
);
CREATE INDEX idx_correlated_message_subscriptions_instance ON correlated_message_subscriptions(instance_key);
CREATE TABLE forms (
    form_key      INTEGER PRIMARY KEY,
    form_id       TEXT NOT NULL,
    version       INTEGER NOT NULL,
    schema        TEXT NOT NULL,
    resource_name TEXT NOT NULL DEFAULT '',
    tenant_id     TEXT NOT NULL DEFAULT '<default>'
);
CREATE INDEX idx_forms_id ON forms(form_id);
CREATE TABLE resources (
    resource_key   INTEGER PRIMARY KEY,
    resource_id    TEXT NOT NULL,
    resource_name  TEXT NOT NULL,
    version        INTEGER NOT NULL,
    version_tag    TEXT,
    content        TEXT NOT NULL,
    tenant_id      TEXT NOT NULL DEFAULT '<default>'
);
CREATE INDEX idx_resources_id ON resources(resource_id);
CREATE TABLE agent_instances (
    agent_instance_key         INTEGER PRIMARY KEY,
    agent_definition_key       INTEGER NOT NULL DEFAULT 0,
    element_instance_key       INTEGER NOT NULL,
    element_id                 TEXT NOT NULL,
    process_instance_key       INTEGER NOT NULL,
    root_process_instance_key  INTEGER NOT NULL,
    process_definition_key     INTEGER NOT NULL,
    process_definition_id      TEXT NOT NULL,
    process_definition_version INTEGER NOT NULL DEFAULT 0,
    tenant_id                  TEXT NOT NULL,
    status                     TEXT NOT NULL,
    agent_type                 TEXT NOT NULL,
    model                      TEXT,
    provider                   TEXT,
    system_prompt              TEXT,
    system_prompt_json         TEXT,
    max_tokens                 INTEGER NOT NULL DEFAULT -1,
    max_model_calls            INTEGER NOT NULL DEFAULT -1,
    max_tool_calls             INTEGER NOT NULL DEFAULT -1,
    input_tokens               INTEGER NOT NULL DEFAULT 0,
    output_tokens              INTEGER NOT NULL DEFAULT 0,
    reasoning_token_count      INTEGER NOT NULL DEFAULT 0,
    cache_creation_token_count INTEGER NOT NULL DEFAULT 0,
    cache_read_token_count     INTEGER NOT NULL DEFAULT 0,
    model_calls                INTEGER NOT NULL DEFAULT 0,
    tool_calls                 INTEGER NOT NULL DEFAULT 0,
    job_key                    INTEGER NOT NULL DEFAULT 0,
    tools_json                 TEXT NOT NULL DEFAULT '[]',
    creation_date_ms           INTEGER NOT NULL,
    last_updated_date_ms       INTEGER NOT NULL,
    completion_date_ms         INTEGER,
    process_definition_version_tag TEXT,
    element_instance_keys_json TEXT NOT NULL DEFAULT '[]',
    job_lease                 TEXT NOT NULL DEFAULT ''
);
CREATE INDEX idx_agent_instances_process_instance ON agent_instances(process_instance_key);
CREATE TABLE agent_history (
    agent_history_key          INTEGER PRIMARY KEY,
    agent_instance_key         INTEGER NOT NULL,
    element_instance_key       INTEGER NOT NULL,
    process_instance_key       INTEGER NOT NULL,
    root_process_instance_key  INTEGER NOT NULL,
    process_definition_key     INTEGER NOT NULL,
    process_definition_id      TEXT NOT NULL,
    tenant_id                  TEXT NOT NULL,
    job_key                    INTEGER NOT NULL DEFAULT 0,
    loop_iteration             INTEGER NOT NULL,
    role                       TEXT NOT NULL,
    produced_at_ms             INTEGER NOT NULL,
    content_json               TEXT NOT NULL DEFAULT '[]',
    system_prompt              TEXT,
    system_prompt_json         TEXT,
    tool_calls_json            TEXT NOT NULL DEFAULT '[]',
    input_tokens               INTEGER,
    output_tokens              INTEGER,
    reasoning_token_count      INTEGER,
    cache_creation_token_count INTEGER,
    cache_read_token_count     INTEGER,
    duration_ms                INTEGER,
    history_item_id            TEXT,
    tools_json                 TEXT NOT NULL DEFAULT '[]',
    model                      TEXT,
    provider                   TEXT,
    is_duplicate               INTEGER NOT NULL DEFAULT 0,
    commit_status              TEXT NOT NULL,
    job_lease                  TEXT NOT NULL DEFAULT '',
    limits_json                TEXT,
    metrics_json               TEXT
);
CREATE INDEX idx_agent_history_instance ON agent_history(agent_instance_key);
";

// --- read-model schema migration (issue #831) ---
//
// The additive-migration machinery below is deliberately *derivation-based*: the
// target shape is introspected from a throwaway in-memory database built from
// [`SCHEMA`], so there is a single source of truth (`SCHEMA`) and no
// hand-maintained migration ladder that could drift out of sync with it.

/// A column as reported by `PRAGMA table_info` — enough to reconstruct a legal
/// `ALTER TABLE ADD COLUMN` for any *additive* column.
struct ColumnShape {
    name: String,
    decl_type: String,
    notnull: bool,
    dflt: Option<String>,
    primary_key: bool,
}

/// The introspected shape of a database: table name -> (create statement, columns
/// in declared order), plus non-auto index name -> create statement.
struct SchemaShape {
    tables: std::collections::BTreeMap<String, (String, Vec<ColumnShape>)>,
    indexes: std::collections::BTreeMap<String, String>,
}

/// Introspects the shape of the database behind `conn` (its user tables, their
/// columns, and their explicit indexes).
fn introspect_shape(conn: &Connection) -> rusqlite::Result<SchemaShape> {
    let mut tables = std::collections::BTreeMap::new();
    let table_meta: Vec<(String, String)> = {
        let mut stmt = conn.prepare(
            "SELECT name, sql FROM sqlite_master \
             WHERE type = 'table' AND name NOT LIKE 'sqlite_%'",
        )?;
        let rows = stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))?;
        rows.collect::<rusqlite::Result<Vec<_>>>()?
    };
    for (name, create_sql) in table_meta {
        let mut cols = Vec::new();
        let mut stmt = conn.prepare(&format!(
            "PRAGMA table_info(\"{}\")",
            name.replace('"', "\"\"")
        ))?;
        let rows = stmt.query_map([], |r| {
            Ok(ColumnShape {
                name: r.get::<_, String>(1)?,
                decl_type: r.get::<_, String>(2)?,
                notnull: r.get::<_, i64>(3)? != 0,
                dflt: r.get::<_, Option<String>>(4)?,
                primary_key: r.get::<_, i64>(5)? != 0,
            })
        })?;
        for col in rows {
            cols.push(col?);
        }
        tables.insert(name, (create_sql, cols));
    }
    let mut indexes = std::collections::BTreeMap::new();
    {
        // Only indexes with an explicit `sql` (created by a CREATE INDEX
        // statement); auto-indexes backing UNIQUE/PRIMARY KEY have a NULL sql and
        // are recreated implicitly with their table.
        let mut stmt = conn.prepare(
            "SELECT name, sql FROM sqlite_master \
             WHERE type = 'index' AND sql IS NOT NULL AND name NOT LIKE 'sqlite_%'",
        )?;
        let rows = stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))?;
        for row in rows {
            let (name, sql) = row?;
            indexes.insert(name, sql);
        }
    }
    Ok(SchemaShape { tables, indexes })
}

/// The introspected shape of [`SCHEMA`], built by executing it into a throwaway
/// in-memory database. This is the migration *target* (single source of truth).
fn target_shape() -> rusqlite::Result<SchemaShape> {
    let conn = Connection::open_in_memory()?;
    conn.execute_batch(SCHEMA)?;
    introspect_shape(&conn)
}

/// Non-destructively brings the live database at `conn` up to [`SCHEMA`]: creates
/// any missing table, adds any missing column (`ALTER TABLE ADD COLUMN`), and
/// creates any missing index. Historical numeric lease columns are converted to
/// text without losing their values or any rows. Idempotent: a partially-applied
/// run is completed on the next open.
fn reconcile_to_schema(conn: &Connection) -> rusqlite::Result<()> {
    let target = target_shape()?;
    let live = introspect_shape(conn)?;
    let mut ddl = String::new();
    for (table, (create_sql, target_cols)) in &target.tables {
        match live.tables.get(table) {
            None => {
                // Missing table: create it verbatim from the target statement,
                // preserving UNIQUE / AUTOINCREMENT / PRIMARY KEY that a
                // reconstructed DDL would lose.
                ddl.push_str(create_sql);
                ddl.push_str(";\n");
            }
            Some((_, live_cols)) => {
                let have: std::collections::HashSet<&str> =
                    live_cols.iter().map(|c| c.name.as_str()).collect();
                for col in target_cols {
                    let old = live_cols.iter().find(|old| old.name == col.name);
                    let lease_affinity = matches!(col.name.as_str(), "lease_token" | "job_lease")
                        && col.decl_type == "TEXT"
                        && old.is_some_and(|old| old.decl_type != "TEXT");
                    let nullable_history_metric = table == "agent_history"
                        && col.decl_type == "INTEGER"
                        && !col.notnull
                        && !col.primary_key
                        && old.is_some_and(|old| old.notnull);
                    if lease_affinity || nullable_history_metric {
                        // INTEGER affinity would coerce fresh opaque tokens such as "0007".
                        // Historical metrics also need their NOT NULL constraint relaxed.
                        // Column replacement preserves values and derives the new shape from SCHEMA.
                        let legacy = format!("__nano_legacy_{}", col.name);
                        let cast = if lease_affinity {
                            format!("CAST(\"{legacy}\" AS TEXT)")
                        } else {
                            format!("\"{legacy}\"")
                        };
                        let value = if col.notnull {
                            format!(
                                "COALESCE({cast}, {})",
                                col.dflt
                                    .as_deref()
                                    .expect("non-null lease column has a schema default")
                            )
                        } else {
                            cast
                        };
                        ddl.push_str(&format!(
                            "SAVEPOINT column_shape;\n\
                             ALTER TABLE \"{table}\" RENAME COLUMN \"{}\" TO \"{legacy}\";\n",
                            col.name,
                        ));
                        ddl.push_str(&add_column_ddl(table, col));
                        ddl.push_str(&format!(
                            "\nUPDATE \"{table}\" SET \"{}\" = {value};\n\
                             ALTER TABLE \"{table}\" DROP COLUMN \"{legacy}\";\n\
                             RELEASE column_shape;\n",
                            col.name,
                        ));
                    }
                    if !have.contains(col.name.as_str()) {
                        ddl.push_str(&add_column_ddl(table, col));
                        ddl.push('\n');
                    }
                }
            }
        }
    }
    for (name, create_sql) in &target.indexes {
        if !live.indexes.contains_key(name) {
            ddl.push_str(create_sql);
            ddl.push_str(";\n");
        }
    }
    if !ddl.is_empty() {
        conn.execute_batch(&ddl)?;
    }
    Ok(())
}

/// Builds a legal `ALTER TABLE ADD COLUMN` for an additive column. SQLite
/// requires a NOT NULL column added to a (possibly non-empty) table to carry a
/// non-NULL default; an additive `SCHEMA` change must therefore give new NOT NULL
/// columns a `DEFAULT`, which this faithfully reproduces from the target shape.
fn add_column_ddl(table: &str, col: &ColumnShape) -> String {
    let mut s = format!(
        "ALTER TABLE \"{}\" ADD COLUMN \"{}\" {}",
        table.replace('"', "\"\""),
        col.name.replace('"', "\"\""),
        col.decl_type
    );
    if let Some(d) = &col.dflt {
        s.push_str(" DEFAULT ");
        s.push_str(d);
    }
    if col.notnull {
        s.push_str(" NOT NULL");
    }
    s.push(';');
    s
}

/// User tables (excluding SQLite's internal `sqlite_%` tables) present in `conn`.
fn list_user_tables(conn: &Connection) -> rusqlite::Result<Vec<String>> {
    let mut stmt = conn.prepare(
        "SELECT name FROM sqlite_master \
         WHERE type = 'table' AND name NOT LIKE 'sqlite_%'",
    )?;
    let names = stmt.query_map([], |r| r.get::<_, String>(0))?;
    names.collect::<rusqlite::Result<Vec<_>>>()
}

/// Drops every user table in `conn` (derived from `sqlite_master`, so it can
/// never fall behind `SCHEMA`). Used only by the destructive [`ReadStore::reset`].
fn drop_all_user_tables(conn: &Connection) -> rusqlite::Result<()> {
    let mut drop_sql = String::new();
    for name in list_user_tables(conn)? {
        drop_sql.push_str(&format!(
            "DROP TABLE IF EXISTS \"{}\";",
            name.replace('"', "\"\"")
        ));
    }
    if !drop_sql.is_empty() {
        conn.execute_batch(&drop_sql)?;
    }
    Ok(())
}

/// Creates the full [`SCHEMA`] on an empty database and stamps the current
/// version/fingerprint with `exported_position = 0`.
fn create_fresh_schema(conn: &Connection) -> rusqlite::Result<()> {
    conn.execute_batch(SCHEMA)?;
    conn.execute(
        "INSERT INTO meta (k, v) VALUES ('schema_version', ?1) \
         ON CONFLICT(k) DO UPDATE SET v = excluded.v",
        params![SCHEMA_VERSION],
    )?;
    conn.execute(
        "INSERT INTO meta (k, v) VALUES ('schema_fingerprint', ?1) \
         ON CONFLICT(k) DO UPDATE SET v = excluded.v",
        params![schema_fingerprint()],
    )?;
    conn.execute(
        "INSERT INTO meta (k, v) VALUES ('exported_position', 0) \
         ON CONFLICT(k) DO UPDATE SET v = excluded.v",
        [],
    )?;
    Ok(())
}

// --- durable terminal-audit archive (issue #831) ---
//
// Completed/terminal instances live ONLY in the read model, so a below-floor
// snapshot reprojection (#732 — which recovers only *live* instances from the
// engine snapshot) silently loses them. The archive is a co-located SQLite file
// sharing [`SCHEMA`] (so it evolves via the same additive migrations and is never
// destructively wiped) into which terminal instances are copied as they become
// terminal, and replayed back on top of a reprojection.

/// Instance-scoped dependent tables copied alongside a terminal
/// `process_instances` row (keyed by `instance_key`).
const TERMINAL_ARCHIVE_DEP_TABLES: [&str; 3] = ["user_tasks", "variables", "decision_instances"];

/// Attaches the terminal-audit archive database at `archive` to `conn` under the
/// schema name `terminal_archive`. The filename is bound as a parameter so no
/// path escaping is needed.
fn attach_archive(conn: &Connection, archive: &Path) -> rusqlite::Result<()> {
    conn.execute(
        "ATTACH DATABASE ?1 AS terminal_archive",
        params![archive.to_string_lossy()],
    )?;
    // The archive is the *durable source of truth* for completed history: unlike
    // the derived read model (rebuildable from the journal), a terminal instance
    // evicted below the snapshot floor lives ONLY here, so losing a recently
    // archived commit on power/OS loss is unrecoverable. `synchronous` is a
    // per-attached-database setting, so we give `terminal_archive` its own
    // power-safe profile (default FULL) on every attach rather than inheriting the
    // read model's throughput-tuned NORMAL from the main connection. Harmless on
    // the read-only replay path; archive writes are infrequent (only as instances
    // become terminal), so the per-commit fsync is off the read model's hot path.
    // See `archive_sync_pragma`.
    conn.pragma_update(
        Some(rusqlite::DatabaseName::Attached("terminal_archive")),
        "synchronous",
        archive_sync_pragma(),
    )?;
    Ok(())
}

/// `synchronous` durability level for the terminal-audit archive (issue #831).
/// Defaults to `FULL` (per-commit fsync, power-loss safe) because the archive is
/// the durable source of truth for completed history and is *not* rebuildable
/// from the journal once an instance has been evicted below the snapshot floor —
/// so it must not inherit the read model's throughput-tuned `NORMAL`.
/// `NANOBPMN_ARCHIVE_SYNC` overrides (e.g. `NORMAL` to trade archive durability
/// for speed, matching the read model).
fn archive_sync_pragma() -> String {
    std::env::var("NANOBPMN_ARCHIVE_SYNC").unwrap_or_else(|_| "FULL".into())
}

/// Column names common to the `table` copies in the two attached databases
/// `src_schema` and `dst_schema` (e.g. `main` and `terminal_archive`), quoted
/// and comma-joined for use in a cross-database `INSERT (<cols>) SELECT <cols>`.
///
/// Additive migrations (`ALTER TABLE ADD COLUMN`) append columns to *existing*
/// tables, so two copies of the same logical table — a freshly-created archive
/// vs. a migrated live DB, or vice versa — can end up with different *physical*
/// column orders, or one may (transiently) carry a column the other lacks. A
/// positional `SELECT *` copy would then silently write values into the wrong
/// columns and corrupt the destination; enumerating the shared columns by name
/// makes the copy order-independent and drift-safe (issue #831).
fn shared_column_list(
    conn: &Connection,
    src_schema: &str,
    dst_schema: &str,
    table: &str,
) -> rusqlite::Result<String> {
    let columns_of = |schema: &str| -> rusqlite::Result<Vec<String>> {
        let mut stmt = conn.prepare(&format!(
            "PRAGMA {schema}.table_info(\"{}\")",
            table.replace('"', "\"\"")
        ))?;
        let cols = stmt
            .query_map([], |r| r.get::<_, String>(1))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(cols)
    };
    let dst: std::collections::HashSet<String> = columns_of(dst_schema)?.into_iter().collect();
    let list = columns_of(src_schema)?
        .into_iter()
        .filter(|c| dst.contains(c))
        .map(|c| format!("\"{}\"", c.replace('"', "\"\"")))
        .collect::<Vec<_>>()
        .join(", ");
    Ok(list)
}

/// Copies the given terminal instances (and their instance-scoped dependent rows)
/// from the live store on `conn` into the durable archive at `archive`
/// (`INSERT OR REPLACE`, append-only in effect since keys are unique and
/// monotonic). Runs outside the export transaction as autocommit statements: the
/// archive is a best-effort durability backstop, so cross-file atomicity is not
/// required (a torn capture is simply re-captured, and reprojection degrades to
/// the prior #732 behaviour for anything unarchived).
fn copy_terminal_to_archive(
    conn: &Connection,
    archive: &Path,
    keys: &[Key],
) -> rusqlite::Result<()> {
    if keys.is_empty() {
        return Ok(());
    }
    attach_archive(conn, archive)?;
    let result = (|| -> rusqlite::Result<()> {
        // Materialize the keys into a temp table rather than string-joining them
        // into a single inline `IN (...)` literal: a large catch-up/rebuild batch
        // can hold very many terminal keys, and an inline list would build an
        // enormous SQL statement (slow, and eventually past SQLite's SQL-length
        // limit). A temp table keeps every copy statement a fixed size regardless
        // of batch size, mirroring the eviction path's `_evict` pattern.
        conn.execute_batch(
            "CREATE TEMP TABLE IF NOT EXISTS _terminal_copy(key INTEGER PRIMARY KEY);
             DELETE FROM _terminal_copy;",
        )?;
        {
            let mut insert =
                conn.prepare("INSERT OR IGNORE INTO _terminal_copy(key) VALUES (?1)")?;
            for k in keys {
                insert.execute(params![*k as i64])?;
            }
        }
        let pi_cols = shared_column_list(conn, "main", "terminal_archive", "process_instances")?;
        conn.execute(
            &format!(
                "INSERT OR REPLACE INTO terminal_archive.process_instances ({pi_cols}) \
                 SELECT {pi_cols} FROM main.process_instances \
                 WHERE key IN (SELECT key FROM _terminal_copy)"
            ),
            [],
        )?;
        for table in TERMINAL_ARCHIVE_DEP_TABLES {
            let cols = shared_column_list(conn, "main", "terminal_archive", table)?;
            conn.execute(
                &format!(
                    "INSERT OR REPLACE INTO terminal_archive.{table} ({cols}) \
                     SELECT {cols} FROM main.{table} \
                     WHERE instance_key IN (SELECT key FROM _terminal_copy)"
                ),
                [],
            )?;
        }
        conn.execute_batch("DELETE FROM _terminal_copy")?;
        Ok(())
    })();
    // Always detach, even on error, so a later attach does not fail with
    // "database terminal_archive is already in use".
    let _ = conn.execute_batch("DETACH DATABASE terminal_archive");
    result
}

// --- enum <-> integer code mappings (kept beside the engine enums) ---

const fn instance_state_code(s: ProcessInstanceState) -> i64 {
    match s {
        ProcessInstanceState::Active => 0,
        ProcessInstanceState::Completed => 1,
        ProcessInstanceState::Terminated => 2,
        // A transient cancelling state (ADR 0037 §6): the instance's tokens are
        // discarded and a `canceling` task-listener chain is draining before it
        // becomes `Terminated`. Projected as its own code so the read model can
        // show "cancelling".
        ProcessInstanceState::Terminating => 3,
        // A suspended instance is still in-flight: it keeps the `Active` base
        // code (0) so `WHERE state = 0` active-count / orphan queries still
        // count it, and its suspension is tracked out-of-band by the nullable
        // `suspended_date_ms` column — the single source of truth from which the
        // derived `Suspended` state and the `suspendedDate` value are both read.
        ProcessInstanceState::Suspended => 0,
    }
}

/// The terminal process-instance state codes (`Completed`, `Terminated`),
/// **derived** from the canonical [`instance_state_code`] mapping rather than
/// hard-coded. Every terminal-selection query (eviction, adaptive pruning,
/// terminal-archive copy/backfill — issue #831) builds its `state IN (...)`
/// predicate from this single source via [`terminal_state_predicate`], so the
/// SQL can never drift from the enum-to-int codes.
const TERMINAL_INSTANCE_STATE_CODES: [i64; 2] = [
    instance_state_code(ProcessInstanceState::Completed),
    instance_state_code(ProcessInstanceState::Terminated),
];

/// Builds a `<column> IN (<terminal codes>)` SQL predicate from
/// [`TERMINAL_INSTANCE_STATE_CODES`]. Use this instead of writing a literal
/// `state IN (1, 2)`, so the terminal-state code set has one source of truth.
fn terminal_state_predicate(column: &str) -> String {
    let [completed, terminated] = TERMINAL_INSTANCE_STATE_CODES;
    format!("{column} IN ({completed}, {terminated})")
}
fn instance_state_from(code: i64) -> ProcessInstanceState {
    match code {
        1 => ProcessInstanceState::Completed,
        2 => ProcessInstanceState::Terminated,
        3 => ProcessInstanceState::Terminating,
        _ => ProcessInstanceState::Active,
    }
}

/// Server-side filter for the console's paged process-instance list
/// ([`ReadStore::process_instances_page`] / [`ReadStore::process_instance_count`]).
///
/// Both dimensions are optional; `None` means "no constraint on that dimension"
/// (so `InstanceFilter::default()` == today's unfiltered list, byte-for-byte).
/// The two present dimensions combine with **AND**.
///
/// This is the single source of truth for the filter derivation: the SQL page
/// query, the SQL count, and the multi-shard in-memory merge
/// (`server::readstore`) all derive from the same [`sql_predicate`] /
/// [`matches`] pair, so a filtered page and its pager total can never drift, and
/// a single-shard node filters identically to a sharded one.
///
/// [`sql_predicate`]: InstanceFilter::sql_predicate
/// [`matches`]: InstanceFilter::matches
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct InstanceFilter {
    /// Restrict to a single lifecycle state (`None` = any state).
    pub state: Option<ProcessInstanceState>,
    /// Restrict to instances with (`Some(true)`) / without (`Some(false)`) an
    /// open incident (`None` = no incident constraint).
    pub has_incident: Option<bool>,
}

impl InstanceFilter {
    /// True when no dimension constrains the result — the unfiltered path.
    pub fn is_unfiltered(&self) -> bool {
        self.state.is_none() && self.has_incident.is_none()
    }

    /// SQL predicate fragment (without a leading `WHERE`) built from **literal
    /// integer codes** — `state` is a typed enum mapped through the canonical
    /// [`instance_state_code`] and `has_incident` a bool, so no user-supplied
    /// string ever reaches SQL (no injection surface). Empty when unfiltered.
    fn sql_predicate(&self) -> String {
        let mut clauses: Vec<String> = Vec::new();
        if let Some(state) = self.state {
            clauses.push(format!("state = {}", instance_state_code(state)));
            // `Active` and `Suspended` share base code 0 (see
            // [`instance_state_code`]), so `state = 0` alone cannot tell them
            // apart. Disambiguate by the nullable `suspended_date_ms` column —
            // the single source of truth `map_instance` derives `Suspended` from
            // — so a console filter for one never returns the other. Every other
            // state has a distinct code and needs no extra clause.
            match state {
                ProcessInstanceState::Active => {
                    clauses.push("suspended_date_ms IS NULL".to_string());
                }
                ProcessInstanceState::Suspended => {
                    clauses.push("suspended_date_ms IS NOT NULL".to_string());
                }
                _ => {}
            }
        }
        if let Some(has_incident) = self.has_incident {
            clauses.push(format!("has_incident = {}", i64::from(has_incident)));
        }
        clauses.join(" AND ")
    }

    /// The `WHERE …` clause (with a leading space + `WHERE`) to splice into a
    /// query, or the empty string when unfiltered.
    fn where_clause(&self) -> String {
        let predicate = self.sql_predicate();
        if predicate.is_empty() {
            String::new()
        } else {
            format!(" WHERE {predicate}")
        }
    }

    /// In-memory row predicate used by the multi-shard merge — must stay
    /// semantically identical to [`sql_predicate`](Self::sql_predicate) so a
    /// sharded node filters exactly like a single-shard node.
    pub fn matches(&self, row: &ProcessInstanceRow) -> bool {
        if let Some(state) = self.state
            && row.state != state
        {
            return false;
        }
        if let Some(has_incident) = self.has_incident
            && row.has_incident != has_incident
        {
            return false;
        }
        true
    }
}

const fn job_state_code(s: JobState) -> i64 {
    match s {
        JobState::Created => 0,
        JobState::Activated => 1,
        JobState::Failed => 2,
        JobState::Errored => 3,
        JobState::Completed => 4,
        JobState::Canceled => 5,
    }
}

/// The terminal job-state codes that carry **no** `endTime` (`Failed`,
/// `Errored`), **derived** from the canonical [`job_state_code`] mapping rather
/// than hard-coded. These are the states the `lastUpdateTime`-freeze arm must
/// hold at (a terminal `endTime`-less job must not have `lastUpdateTime` pushed
/// past its first terminal transition — #1344). Mirrors
/// [`TERMINAL_INSTANCE_STATE_CODES`] so the SQL can never drift from the
/// enum-to-int codes should [`job_state_code`] ever be renumbered.
const ENDTIMELESS_TERMINAL_JOB_STATE_CODES: [i64; 2] = [
    job_state_code(JobState::Failed),
    job_state_code(JobState::Errored),
];

/// Builds a `<column> IN (<endTime-less terminal job codes>)` SQL predicate from
/// [`ENDTIMELESS_TERMINAL_JOB_STATE_CODES`]. Use this instead of writing a
/// literal `state IN (2, 3)`, so the freeze arm has one source of truth (mirrors
/// [`terminal_state_predicate`]).
fn endtimeless_terminal_job_predicate(column: &str) -> String {
    let [failed, errored] = ENDTIMELESS_TERMINAL_JOB_STATE_CODES;
    format!("{column} IN ({failed}, {errored})")
}
fn job_state_from(code: i64) -> JobState {
    match code {
        1 => JobState::Activated,
        2 => JobState::Failed,
        3 => JobState::Errored,
        4 => JobState::Completed,
        5 => JobState::Canceled,
        _ => JobState::Created,
    }
}

/// Lifecycle state of an element (flow-node) instance, mirroring Camunda 8's
/// `ElementInstanceStateEnum`. Stored as the integer code below.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ElementInstanceState {
    Active,
    Completed,
    Terminated,
}

fn element_instance_state_code(s: ElementInstanceState) -> i64 {
    match s {
        ElementInstanceState::Active => 0,
        ElementInstanceState::Completed => 1,
        ElementInstanceState::Terminated => 2,
    }
}
fn element_instance_state_from(code: i64) -> ElementInstanceState {
    match code {
        1 => ElementInstanceState::Completed,
        2 => ElementInstanceState::Terminated,
        _ => ElementInstanceState::Active,
    }
}

/// Wall-clock milliseconds since the Unix epoch, used to stamp element-instance
/// start/end dates at projection time (the lifecycle events carry no
/// engine-authored timestamp — see [`ElementInstanceRow`]).
///
/// The clock source is per-platform: the same projection runs on the gateway
/// server (`native`) and the in-browser test engine (`wasm`), which have
/// different clocks. Only the *source* differs; the value semantics (Unix-epoch
/// milliseconds) are identical, so the projection code above is unchanged.
#[cfg(not(target_arch = "wasm32"))]
fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// `wasm32-unknown-unknown` has no platform clock — `std::time::SystemTime::now()`
/// unconditionally panics there ("time not implemented on this platform"), which
/// would abort the projection on its very first `export`. Read the wall clock
/// from JavaScript's `Date.now()` (available in both the browser and node)
/// instead: the exact browser/node analog of the native Unix-epoch millisecond
/// clock, so element-instance timestamps stay meaningful and the projection runs
/// without panicking.
#[cfg(target_arch = "wasm32")]
fn now_ms() -> u64 {
    js_sys::Date::now() as u64
}

/// Encodes a [`JobKind`] into the `(job_kind, listener_event_type)` column pair
/// stored on the jobs read-model row (ADR 0037). Ordinary element jobs store
/// `(0, 0)`; execution-listener jobs store `(1, start=0/end=1)`; task-listener
/// jobs store `(2, creating=0/assigning=1/updating=2/completing=3/canceling=4)`.
/// The listener index/scope are not projected.
fn job_kind_codes(kind: &JobKind) -> (i64, i64) {
    match kind {
        JobKind::BpmnElement => (0, 0),
        JobKind::ExecutionListener { event_type, .. } => (
            1,
            match event_type {
                ListenerEventType::Start => 0,
                ListenerEventType::End => 1,
            },
        ),
        JobKind::TaskListener { event_type, .. } => (
            2,
            match event_type {
                TaskListenerEventType::Creating => 0,
                TaskListenerEventType::Assigning => 1,
                TaskListenerEventType::Updating => 2,
                TaskListenerEventType::Completing => 3,
                TaskListenerEventType::Canceling => 4,
            },
        ),
    }
}

/// Reconstructs the display-relevant [`JobKind`] from the stored column pair.
/// The listener index/scope/user-task key are not persisted, so they default
/// to `0`.
fn job_kind_from(job_kind: i64, listener_event_type: i64) -> JobKind {
    match job_kind {
        1 => JobKind::ExecutionListener {
            event_type: if listener_event_type == 1 {
                ListenerEventType::End
            } else {
                ListenerEventType::Start
            },
            index: 0,
            scope: 0,
        },
        2 => JobKind::TaskListener {
            event_type: match listener_event_type {
                1 => TaskListenerEventType::Assigning,
                2 => TaskListenerEventType::Updating,
                3 => TaskListenerEventType::Completing,
                4 => TaskListenerEventType::Canceling,
                _ => TaskListenerEventType::Creating,
            },
            index: 0,
            user_task_key: 0,
        },
        _ => JobKind::BpmnElement,
    }
}

fn user_task_state_code(s: UserTaskState) -> i64 {
    match s {
        UserTaskState::Created => 0,
        UserTaskState::Completed => 1,
        UserTaskState::Canceled => 2,
    }
}
fn user_task_state_from(code: i64) -> UserTaskState {
    match code {
        1 => UserTaskState::Completed,
        2 => UserTaskState::Canceled,
        _ => UserTaskState::Created,
    }
}

fn incident_state_code(s: IncidentState) -> i64 {
    match s {
        IncidentState::Active => 0,
        IncidentState::Resolved => 1,
    }
}
fn incident_state_from(code: i64) -> IncidentState {
    match code {
        1 => IncidentState::Resolved,
        _ => IncidentState::Active,
    }
}

fn incident_kind_code(k: IncidentKind) -> i64 {
    match k {
        IncidentKind::JobNoRetries => 0,
        IncidentKind::NoMatchingSequenceFlow => 1,
        IncidentKind::UnhandledError => 2,
        IncidentKind::ExpressionEvaluation => 3,
        IncidentKind::DecisionEvaluation => 4,
        IncidentKind::CalledElementError => 5,
        IncidentKind::IoMapping => 6,
    }
}
fn incident_kind_from(code: i64) -> IncidentKind {
    match code {
        1 => IncidentKind::NoMatchingSequenceFlow,
        2 => IncidentKind::UnhandledError,
        3 => IncidentKind::ExpressionEvaluation,
        4 => IncidentKind::DecisionEvaluation,
        5 => IncidentKind::CalledElementError,
        // 6 and the legacy 7 (formerly `IoMappingOutput`, collapsed into the one
        // `IoMapping` kind in #946) both map to `IoMapping`.
        6 | 7 => IncidentKind::IoMapping,
        _ => IncidentKind::JobNoRetries,
    }
}

// --- denormalized read rows (carry everything a result projection needs) ---

pub struct ProcessInstanceRow {
    pub key: Key,
    pub process_id: String,
    pub process_definition_id: String,
    pub process_definition_key: String,
    pub version: i32,
    pub state: ProcessInstanceState,
    pub start_date_ms: u64,
    pub has_incident: bool,
    pub tags: Vec<String>,
    pub business_id: Option<String>,
    /// C8 parent linkage for a call-activity **child** process instance: the
    /// calling instance's key and the spawning call-activity element instance
    /// key. Both `None` for a top-level instance. Surfaced so the C8
    /// `parentProcessInstanceKey` field/filter return real data.
    pub parent_process_instance_key: Option<Key>,
    pub parent_element_instance_key: Option<Key>,
    /// Epoch-milliseconds instant at which this instance most recently entered
    /// `Suspended` (via [`nanobpmn_engine_core::Command::SuspendInstance`]), or
    /// `None` when it is not currently suspended. This nullable column is the
    /// **single source of truth** for suspension: [`map_instance`] derives the
    /// `Suspended` state from its presence, and the gateway's `suspendedDate`
    /// result field is read straight from it.
    pub suspended_date_ms: Option<u64>,
}

/// Resolves the `rootProcessInstanceKey` for `key` (C8 parity, issue #977) by
/// walking the `parentProcessInstanceKey` chain — supplied by `lookup` — to the
/// top-level ancestor. A top-level instance (no parent) roots to its own key; a
/// call-activity child (issue #808), however deeply nested, roots to the
/// top-level instance that started the whole tree.
///
/// The walk is parameterised by `lookup` so it is the **single** root-resolution
/// algorithm shared by every read surface: the server's sharded
/// `ReadModel` routes each hop across partitions, the single-partition
/// [`ReadStore`] (and the `engine-wasm` `TestEngine`) looks up in place — same
/// chain, one implementation, no drift.
///
/// Best-effort at the boundaries: if `key` itself is unknown, or an ancestor row
/// has been pruned/not-yet-projected, the walk stops and returns the furthest
/// ancestor key it could observe (the child key, or that missing parent's key)
/// rather than fabricating a root. A `visited` set guards against a corrupt
/// parent cycle (e.g. a lasso `A → B → A`), terminating at the first ring member
/// re-encountered instead of looping.
///
/// This is a one-shot convenience over [`resolve_root_with_cache`]: it resolves a
/// single key with a throwaway cache. Callers projecting many rows in one
/// response should use a [`RootResolver`] instead so the walked chain is memoised
/// across rows.
pub fn resolve_root_process_instance_key(
    key: Key,
    mut lookup: impl FnMut(Key) -> Option<ProcessInstanceRow>,
) -> Key {
    let mut cache = std::collections::HashMap::new();
    resolve_root_with_cache(key, &mut lookup, &mut cache)
}

/// The canonical root walk, memoising **every** key it visits (not just `key`).
///
/// All keys on one parent chain share the same top-level ancestor, so a single
/// walk that touches `N` ancestors seeds `N` cache entries — a later sibling or
/// descendant row in the same response reuses the shared prefix (or the whole
/// chain) instead of re-walking it. This collapses projecting a page from
/// `O(rows × chain-depth)` point lookups to `O(distinct keys)`.
///
/// Termination and boundary semantics match [`resolve_root_process_instance_key`]:
/// a definitive top-level root (parent `None`) and a best-effort furthest-observed
/// ancestor (a pruned/absent parent row) are cached for the whole path, because
/// every key on the path resolves to that same value. A detected cycle terminates
/// at the re-encountered ring member and is **not** cached, since a corrupt ring
/// has no well-defined root to memoise.
fn resolve_root_with_cache(
    key: Key,
    lookup: &mut dyn FnMut(Key) -> Option<ProcessInstanceRow>,
    cache: &mut std::collections::HashMap<Key, Key>,
) -> Key {
    if let Some(&root) = cache.get(&key) {
        return root;
    }
    walk_root_to_top(
        key,
        Vec::new(),
        std::collections::HashSet::new(),
        lookup,
        cache,
    )
}

/// Like [`resolve_root_with_cache`], but for an entry row the caller **already
/// holds** (every projection row comes straight out of `process_instances()`),
/// so the walk skips the redundant point lookup of the entry's *own* key and
/// reads its `parent_process_instance_key` link directly. A top-level entry
/// (parent `None`) self-roots with **no** lookup at all — collapsing an
/// otherwise `O(page_size)` burst of point queries for a page of top-level rows.
///
/// Semantically identical to `resolve_root_with_cache(entry.key, …)`: the same
/// `visited`/`cache`/cycle-termination behaviour (the entry key is seeded into
/// `visited` and `path` exactly as the first loop step would), differing only by
/// not re-reading a row already in hand. Both entry points share the single
/// [`walk_root_to_top`] core, so there is no second root-walk to drift.
fn resolve_root_from_row_with_cache(
    entry: &ProcessInstanceRow,
    lookup: &mut dyn FnMut(Key) -> Option<ProcessInstanceRow>,
    cache: &mut std::collections::HashMap<Key, Key>,
) -> Key {
    if let Some(&root) = cache.get(&entry.key) {
        return root;
    }
    match entry.parent_process_instance_key {
        // Definitive top-level ancestor: self-roots, no lookup needed.
        None => {
            cache.insert(entry.key, entry.key);
            entry.key
        }
        Some(parent) => {
            let mut visited = std::collections::HashSet::new();
            visited.insert(entry.key);
            walk_root_to_top(parent, vec![entry.key], visited, lookup, cache)
        }
    }
}

/// The shared root-walk core: from `current` (with any already-walked `path` and
/// `visited` prefix), climb the `parentProcessInstanceKey` chain to the
/// top-level ancestor, memoising **every** key visited (see
/// [`resolve_root_with_cache`] for the caching/termination guarantees).
fn walk_root_to_top(
    mut current: Key,
    mut path: Vec<Key>,
    mut visited: std::collections::HashSet<Key>,
    lookup: &mut dyn FnMut(Key) -> Option<ProcessInstanceRow>,
    cache: &mut std::collections::HashMap<Key, Key>,
) -> Key {
    let root = loop {
        // A previously-resolved ancestor short-circuits the rest of the walk.
        if let Some(&cached) = cache.get(&current) {
            break cached;
        }
        if !visited.insert(current) {
            // Cycle: terminate at the re-encountered ring member without
            // poisoning the cache with an ambiguous root.
            return current;
        }
        match lookup(current) {
            // `current`'s row is absent: an unknown entry key self-roots, and a
            // named-but-pruned ancestor is the furthest observable root. Either
            // way `current` is the root for the whole path.
            None => break current,
            Some(row) => match row.parent_process_instance_key {
                // Definitive top-level ancestor (`row.key == current`).
                None => break row.key,
                Some(parent) => {
                    path.push(current);
                    current = parent;
                }
            },
        }
    };
    // Seed the whole walked path plus its terminal so common ancestors are reused.
    for k in path {
        cache.insert(k, root);
    }
    cache.insert(current, root);
    root
}

/// A per-response memoiser over [`resolve_root_with_cache`]. Built once around a
/// `lookup` closure (a `ReadStore` / sharded `ReadModel` point lookup) and shared
/// across every row projected in one search/get response, so a call-activity
/// hierarchy's parent chain is walked once and every descendant row reuses it —
/// see [`resolve_root_with_cache`] for the caching guarantee.
///
/// Interior mutability lets it be shared behind `&self` through a projection map,
/// exactly like the read model it wraps; the cache cannot go stale because the
/// read model is immutable for the life of a response.
pub struct RootResolver<'a> {
    lookup: std::cell::RefCell<Box<dyn FnMut(Key) -> Option<ProcessInstanceRow> + 'a>>,
    cache: std::cell::RefCell<std::collections::HashMap<Key, Key>>,
}

impl<'a> RootResolver<'a> {
    /// Wraps `lookup` (a `parentProcessInstanceKey`-carrying row point lookup)
    /// in a fresh, empty memoiser.
    pub fn new(lookup: impl FnMut(Key) -> Option<ProcessInstanceRow> + 'a) -> Self {
        Self {
            lookup: std::cell::RefCell::new(Box::new(lookup)),
            cache: std::cell::RefCell::new(std::collections::HashMap::new()),
        }
    }

    /// The `rootProcessInstanceKey` for `key`, memoising the whole walked chain.
    pub fn root_process_instance_key(&self, key: Key) -> Key {
        let mut lookup = self.lookup.borrow_mut();
        let mut cache = self.cache.borrow_mut();
        resolve_root_with_cache(key, &mut **lookup, &mut cache)
    }

    /// The `rootProcessInstanceKey` for an **already-loaded** row, skipping the
    /// redundant point lookup of the row's own key (a top-level row self-roots
    /// with no lookup at all). Prefer this over [`Self::root_process_instance_key`]
    /// whenever the projection already holds the [`ProcessInstanceRow`] — it is
    /// semantically identical but avoids one point query per projected row. See
    /// [`resolve_root_from_row_with_cache`].
    pub fn root_of_row(&self, row: &ProcessInstanceRow) -> Key {
        let mut lookup = self.lookup.borrow_mut();
        let mut cache = self.cache.borrow_mut();
        resolve_root_from_row_with_cache(row, &mut **lookup, &mut cache)
    }
}

pub struct JobRow {
    pub key: Key,
    pub instance_key: Key,
    pub element_instance_key: Key,
    pub element_id: String,
    pub job_type: String,
    pub state: JobState,
    pub retries: i32,
    pub worker: Option<String>,
    pub deadline_ms: Option<u64>,
    pub process_definition_id: String,
    pub process_definition_key: String,
    /// Ordinary BPMN-element job or an execution-listener job (ADR 0037). Only
    /// the display-relevant discriminant (kind + listener event type) is
    /// preserved in the read model; the listener index/scope are not projected.
    pub kind: JobKind,
    /// Wall-clock instant (epoch ms) the job was created, carried from
    /// [`crate::Event::JobCreated`]. `0` for jobs created before the engine
    /// recorded the field. Feeds the `/v2/jobs/statistics/*` `created` counters.
    pub created_at_ms: u64,
    /// Record-timestamp (epoch ms) of the most recent projected job event — the
    /// Camunda `lastUpdateTime` (#1344). Set on `JobCreated` and every subsequent
    /// projected job event (fail, timeout, retries update, timeout update, error,
    /// completion, cancellation). `None` for rows migrated from a pre-#1344
    /// database that never saw a fresh job event.
    pub last_update_ms: Option<u64>,
    /// Record-timestamp (epoch ms) of the active→terminal `JobCompleted` /
    /// `JobCanceled` transition — the Camunda `endTime` (#1344). `None` while the
    /// job is live, and for a job that reached a non-completing terminal state
    /// (failed / errored). Stamped once; a re-delivery of the terminal event is a
    /// no-op.
    pub end_ms: Option<u64>,
    /// The declared read-set (`fetchVariables`) recorded on the most recent
    /// durable [`crate::Event::JobActivated`] for this job that declared a
    /// non-empty set — the variable names the worker was handed. Preserved across
    /// a later fetch-all re-activation (a declaration-free activation does not
    /// overwrite it). Engine-native read provenance for reification (issue #986).
    /// Empty when no durable activation has declared a set (fetch-all / undeclared
    /// reads), or when the job's activation was never durably recorded (e.g.
    /// leader-local activation, which does not export `JobActivated`).
    pub read_set: Vec<String>,
    pub lease_token: Option<String>,
    /// The owning process instance's `businessId` as it stood when this
    /// artifact was created (snapshot — a later assignment does not enrich it).
    pub business_id: Option<String>,
    /// The last worker-reported message from [`crate::Event::JobFailed`] /
    /// [`crate::Event::JobErrorThrown`] (Zeebe job `errorMessage`, #1327).
    pub error_message: Option<String>,
    /// The last thrown error code from [`crate::Event::JobErrorThrown`].
    pub error_code: Option<String>,
    /// Whether the last fail / thrown error left the job with retries > 0
    /// (Zeebe exporter `jobFailedWithRetriesLeft`).
    pub has_failed_with_retries_left: bool,
}

pub struct UserTaskRow {
    pub key: Key,
    pub instance_key: Key,
    pub element_instance_key: Key,
    pub element_id: String,
    pub state: UserTaskState,
    pub assignee: Option<String>,
    pub candidate_groups: Vec<String>,
    pub candidate_users: Vec<String>,
    pub due_date: Option<String>,
    pub follow_up_date: Option<String>,
    pub priority: i32,
    pub created_at_ms: u64,
    pub process_definition_id: String,
    pub process_definition_key: String,
    pub process_definition_version: i32,
    pub form_key: Option<Key>,
    pub external_form_reference: Option<String>,
    /// The owning process instance's `businessId` as it stood when this
    /// artifact was created (snapshot — a later assignment does not enrich it).
    pub business_id: Option<String>,
}

pub struct IncidentRow {
    pub key: Key,
    pub instance_key: Key,
    pub element_instance_key: Key,
    pub element_id: String,
    pub kind: IncidentKind,
    pub state: IncidentState,
    pub reason: String,
    pub job_key: Option<Key>,
    pub created_at_ms: u64,
    pub process_definition_id: String,
    pub process_definition_key: String,
}

/// A projected element (flow-node) instance record, materialized from the
/// engine's per-element lifecycle events (`ElementActivating`/`ElementActivated`
/// /`ElementCompleted`) for the element-instance query API. One row per element
/// instance the engine activates.
///
/// `start_date_ms`/`end_date_ms` are stamped at projection time (the lifecycle
/// events carry no engine-authored timestamp), so a full read-model rebuild from
/// the journal re-dates them; engine-authored element timestamps are a follow-up.
pub struct ElementInstanceRow {
    pub element_instance_key: Key,
    pub instance_key: Key,
    pub process_definition_id: String,
    pub process_definition_key: String,
    pub element_id: String,
    pub element_name: Option<String>,
    /// Camunda element `type` spelling (e.g. `SERVICE_TASK`), resolved from the
    /// deployed model via `definition_elements`; `UNKNOWN` when unresolved.
    pub element_type: String,
    pub state: ElementInstanceState,
    pub start_date_ms: u64,
    pub end_date_ms: Option<u64>,
    /// The scope-owning element instance (enclosing sub-process/multi-instance
    /// body), or `0` for the process-level scope.
    pub scope_key: Key,
    pub incident_key: Option<Key>,
    pub has_incident: bool,
    pub tenant_id: String,
}

/// A projected open message subscription: a running element instance parked on a
/// message catch (intermediate catch event, receive task or message boundary),
/// materialized from `MessageSubscriptionCreated`. Interrupting subscriptions are
/// dropped on `MessageCorrelated`/`RemoteMessageCorrelation`; a non-interrupting
/// boundary subscription stays open (it can correlate repeatedly) and is dropped
/// only on `MessageSubscriptionCanceled` or instance termination. Each correlation
/// is additionally recorded in `correlated_message_subscriptions`.
/// Feeds the MESSAGE variant of the element-instance wait-state API.
pub struct MessageSubscriptionRow {
    pub subscription_key: Key,
    pub instance_key: Key,
    pub element_instance_key: Key,
    pub element_id: String,
    pub message_name: String,
    pub correlation_key: String,
    /// Projection-time timestamp (ms since epoch) of when this subscription row
    /// was first materialised; surfaces as `lastUpdatedDate` in the search API.
    pub created_at_ms: u64,
    /// The owning process instance's `businessId` as it stood when this
    /// artifact was created (snapshot — a later assignment does not enrich it).
    pub business_id: Option<String>,
}

/// The kind of a non-job, non-message event wait (the TIMER / SIGNAL /
/// CONDITION variants of the element-instance wait-state API).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EventWaitType {
    Timer,
    Signal,
    Condition,
}

impl EventWaitType {
    /// The stored `wait_type` code (also the spec's `waitStateType` value).
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Timer => "TIMER",
            Self::Signal => "SIGNAL",
            Self::Condition => "CONDITION",
        }
    }

    fn parse(code: &str) -> Option<Self> {
        [Self::Timer, Self::Signal, Self::Condition]
            .into_iter()
            .find(|t| t.as_str() == code)
    }
}

/// A projected open timer / signal / conditional wait: a running element
/// instance parked on (or guarded by a boundary for) a timer, a signal
/// subscription or a conditional subscription. Materialized from
/// `TimerCreated` / `SignalSubscriptionCreated` /
/// `ConditionalSubscriptionCreated`; dropped when it settles (fires, correlates
/// or is cancelled) or its instance ends. A non-interrupting boundary signal or
/// condition stays open after firing, mirroring the engine (it can fire again).
pub struct EventWaitRow {
    /// The timer key or subscription key (one engine key space).
    pub wait_key: Key,
    pub wait_type: EventWaitType,
    pub instance_key: Key,
    pub element_instance_key: Key,
    pub element_id: String,
    /// The signal name (SIGNAL) or condition expression (CONDITION); empty for
    /// a TIMER.
    pub detail: String,
    /// When a TIMER is due (ms since epoch); `None` for the other types.
    pub due_at_ms: Option<u64>,
}

/// A projected *correlated* message subscription: the historical record of a
/// message that correlated to an instance-scoped subscription, materialized from
/// `MessageCorrelated`/`RemoteMessageCorrelation` (capturing the message name and
/// correlation key from the open subscription row before it is dropped). Feeds
/// the `searchCorrelatedMessageSubscriptions` API. Unlike open subscriptions,
/// these rows are retained after the subscription settles.
pub struct CorrelatedMessageSubscriptionRow {
    pub message_key: Key,
    pub subscription_key: Key,
    pub instance_key: Key,
    pub element_instance_key: Key,
    pub element_id: String,
    pub message_name: String,
    pub correlation_key: String,
    /// Projection-time timestamp (ms since epoch) of the correlation.
    pub correlation_time_ms: u64,
    /// The id of the partition that correlated the message.
    pub partition_id: i32,
    /// The owning process instance's `businessId` as it stood when this
    /// artifact was created (snapshot — a later assignment does not enrich it).
    pub business_id: Option<String>,
}

pub struct ProcessDefinitionRow {
    pub key: Key,
    pub process_id: String,
    pub version: i32,
    pub name: Option<String>,
    pub is_latest: bool,
}

pub struct VariableRow {
    pub key: Key,
    pub instance_key: Key,
    pub scope_key: Key,
    pub name: String,
    /// The variable's value as a serialized-JSON string (e.g. `"text"`, `42`,
    /// `true`), mirroring Camunda's wire representation.
    pub value: String,
    pub process_definition_id: String,
    pub process_definition_key: String,
}

/// A projected decision-instance record (one per evaluated decision in a
/// `businessRuleTask`'s decision evaluation), materialized from
/// [`Event::DecisionEvaluated`] for the DecisionInstance query API.
pub struct DecisionInstanceRow {
    /// `<decisionEvaluationKey>-<index>`, the decision instance's unique id.
    pub eval_instance_key: String,
    pub decision_evaluation_key: Key,
    /// 1-based index of this decision within its evaluation.
    pub idx: i64,
    pub decision_id: String,
    pub decision_key: Key,
    pub decision_name: String,
    /// Camunda decision type spelling (e.g. `DECISION_TABLE`).
    pub decision_type: String,
    pub version: i32,
    pub decision_requirements_id: String,
    pub decision_requirements_key: Key,
    pub root_decision_key: Key,
    pub instance_key: Key,
    pub element_instance_key: Key,
    /// The owning process definition key (decimal string), or empty when the
    /// instance row is not colocated in this shard.
    pub process_definition_key: String,
    /// Camunda decision-instance state spelling (`EVALUATED` / `FAILED`).
    pub state: String,
    pub evaluation_failure: Option<String>,
    pub evaluation_date_ms: u64,
    /// The decision output as a JSON-document string.
    pub result_json: String,
    /// Serialized `Vec<EvaluatedInput>` (engine-core DMN audit).
    pub inputs_json: String,
    /// Serialized `Vec<MatchedRule>` (engine-core DMN audit).
    pub rules_json: String,
    pub tenant_id: String,
    /// The owning process instance's `businessId` as it stood when this
    /// artifact was created (snapshot — a later assignment does not enrich it).
    pub business_id: Option<String>,
}

/// A projected decision-requirements-graph (one per DRG id, latest version).
#[derive(Debug, Clone)]
pub struct DecisionRequirementsRow {
    pub drg_id: String,
    pub drg_key: Key,
    pub name: String,
    pub version: i32,
    /// Synthesized `{drg_id}.dmn` (the engine does not retain the original name).
    pub resource_name: String,
    /// The verbatim DMN XML the graph was parsed from (empty for graphs built
    /// programmatically rather than parsed).
    pub xml: String,
}

/// A projected decision definition (one per decision id, latest version) with its
/// owning DRG's id/name/version denormalized in for querying.
#[derive(Debug, Clone)]
pub struct DecisionDefinitionRow {
    pub decision_id: String,
    pub decision_key: Key,
    pub name: String,
    pub version: i32,
    pub decision_requirements_key: Key,
    pub decision_requirements_id: String,
    pub decision_requirements_name: String,
    pub decision_requirements_version: i32,
}

/// A projected form row, one per deployed form version (keyed by `form_key`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FormRow {
    pub form_id: String,
    pub form_key: Key,
    pub version: i32,
    pub schema: String,
    pub resource_name: String,
    pub tenant_id: String,
}

/// A projected generic-resource row, one per deployed resource version (keyed by
/// `resource_key`). For a generic resource `resource_id` equals `resource_name`
/// (the filename); versions increment per `resource_id`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResourceRow {
    pub resource_key: Key,
    pub resource_id: String,
    pub resource_name: String,
    pub version: i32,
    pub version_tag: Option<String>,
    pub content: String,
    pub tenant_id: String,
}

/// The metadata-only projection of a generic-resource row — every column of
/// [`ResourceRow`] except the (potentially large) `content` blob. Used by the
/// list/search path (`searchResources`), whose response returns only metadata,
/// so it never pays to read `content` for every projected version. Full content
/// is reserved for the by-key endpoints (`getResource*`), mirroring how process
/// definitions list only `key/process_id/version` and fetch the BPMN `xml`
/// separately by key.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResourceMetaRow {
    pub resource_key: Key,
    pub resource_id: String,
    pub resource_name: String,
    pub version: i32,
    pub version_tag: Option<String>,
    pub tenant_id: String,
}

/// Minimum `-wal` sidecar size (bytes) before the adaptive pruner spends a
/// `wal_checkpoint(TRUNCATE)`. Below this the raised autocheckpoint keeps the WAL
/// bounded, so the pruner skips the extra copy-back+truncate — cutting the former
/// ~5/s TRUNCATE storm (each a full-WAL copy-back into the random-access main DB)
/// down to an occasional file-space reclaim. Default 32 MiB, deliberately below
/// the autocheckpoint backstop so the pruner (off the exporter thread) does the
/// checkpointing first and the exporter's inline autocheckpoint rarely fires.
/// `NANOBPMN_READ_WAL_TRUNCATE_MB` overrides (0 = checkpoint on every pruner wake,
/// the pre-throttle behavior, for A/B).
#[cfg(feature = "native")]
fn read_wal_truncate_bytes() -> u64 {
    std::env::var("NANOBPMN_READ_WAL_TRUNCATE_MB")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(32)
        * 1024
        * 1024
}

/// Reads a SQLite database's size as `(file_bytes, live_bytes)` from its header:
/// `file_bytes = page_count × page_size` (the whole allocated file, freelist
/// included) and `live_bytes = (page_count − freelist_count) × page_size` (the
/// pages holding actual data). All three PRAGMAs are O(1) header reads, so this
/// is cheap enough for a hot loop.
///
/// This is the single canonical implementation of read-model space accounting.
/// The `server` crate re-exports it as `crate::sqlite_space::page_stats` so its
/// disk-relative retention / var-spill sizing reuse the exact same derivation
/// (no drift surface). Native-only: it underpins the server-only pruning and
/// disk-sizing methods below, none of which exist on the wasm (in-memory) build.
#[cfg(feature = "native")]
pub fn page_stats(conn: &Connection) -> (u64, u64) {
    let page_count: i64 = conn
        .query_row("PRAGMA page_count", [], |r| r.get(0))
        .unwrap_or(0);
    let freelist: i64 = conn
        .query_row("PRAGMA freelist_count", [], |r| r.get(0))
        .unwrap_or(0);
    let page_size: i64 = conn
        .query_row("PRAGMA page_size", [], |r| r.get(0))
        .unwrap_or(4096);
    let ps = page_size.max(0) as u64;
    let file = page_count.max(0) as u64 * ps;
    let live = (page_count - freelist).max(0) as u64 * ps;
    (file, live)
}

/// Evicts up to `batch` of the **oldest** terminal instances (and their child
/// rows) in a single small transaction, returning how many were deleted.
///
/// Unlike [`ReadStore::prune_terminal_instances`] this takes no keep-count and
/// does **no `OFFSET` scan**: `ORDER BY key ASC LIMIT batch` walks the primary
/// key index from the oldest key, and since the oldest instances are the ones
/// that completed long ago it collects a full batch after scanning ~`batch`
/// rows (plus any still-active stragglers). That makes each sweep O(batch)
/// rather than O(keep_target), so the decoupled pruner can outpace inserts even
/// while the exporter saturates the writer. Operates on a caller-owned
/// connection (the pruner's second connection to the shard) so it never blocks
/// the exporter's mutex; the two serialize only at SQLite's write lock, briefly,
/// per small batch.
#[cfg(feature = "native")]
fn prune_oldest_terminal(conn: &mut Connection, batch: usize) -> rusqlite::Result<usize> {
    if batch == 0 {
        return Ok(0);
    }
    let tx = conn.transaction()?;
    tx.execute_batch(
        "CREATE TEMP TABLE IF NOT EXISTS _evict(key INTEGER PRIMARY KEY);
         DELETE FROM _evict;",
    )?;
    let evicted = tx.execute(
        &format!(
            "INSERT INTO _evict(key) \
             SELECT key FROM process_instances WHERE {} \
             ORDER BY key ASC LIMIT ?1",
            terminal_state_predicate("state")
        ),
        params![batch as i64],
    )?;
    if evicted == 0 {
        tx.commit()?;
        return Ok(0);
    }
    for table in ["variables", "jobs", "incidents", "user_tasks"] {
        tx.execute(
            &format!("DELETE FROM {table} WHERE instance_key IN (SELECT key FROM _evict)"),
            [],
        )?;
    }
    tx.execute(
        "DELETE FROM process_instances WHERE key IN (SELECT key FROM _evict)",
        [],
    )?;
    tx.commit()?;
    Ok(evicted)
}

/// The read model. Wraps a single SQLite connection behind a mutex: SQLite
/// serializes writes anyway, and this keeps the projection (exporter thread) and
/// the queries (request handlers) on one shared database — including for the
/// `:memory:` backend, where separate connections would not see each other's
/// data. The mutex is independent of the engine lock, so reads never contend
/// with engine writes.
pub struct ReadStore {
    conn: Mutex<Connection>,
    /// In-process write-coordination lock shared by the two independent SQLite
    /// writers on this shard's WAL file: the exporter (`export`, on `conn`) and
    /// the decoupled adaptive pruner (`adaptive_prune_once`, on the separate
    /// connection from `prune_connection`). WAL permits only one writer, so a
    /// long pruner delete + `wal_checkpoint(TRUNCATE)` would otherwise trip the
    /// other connection's `busy_timeout` and surface as `database is locked`
    /// (dropped/retried export batches — see #96/#97). Gating both writers on
    /// this mutex turns that cross-connection SQLite lock race into a cheap
    /// in-process wait, so their writes interleave cleanly and never error.
    /// Held only around actual write statements and short enough (the pruner
    /// acquires it per delete chunk) that neither side is starved; reads
    /// (`page_stats`, request-handler queries) never take it.
    write_lock: Mutex<()>,
    /// The shard's on-disk path (None for `:memory:`). Retained so the decoupled
    /// adaptive pruner can open its own second connection to the same WAL file and
    /// evict on an independent schedule, rather than competing for CPU with
    /// projection inside the single exporter thread. Only the `native` backend's
    /// server-only orchestration reads it; the wasm (in-memory) build never has a
    /// path, so the field is inert there.
    #[cfg_attr(not(feature = "native"), allow(dead_code))]
    path: Option<PathBuf>,
    /// Path of the co-located **durable terminal-audit archive** (issue #831), or
    /// `None` for `:memory:` stores and for the archive store itself (which never
    /// nests an archive). Completed/terminal instances are copied here as they
    /// become terminal, so that history survives a below-floor snapshot
    /// reprojection (#732) — the reprojection only recovers *live* instances from
    /// the engine snapshot, and terminal instances lived **only** in the read
    /// model. See [`ReadStore::archive_terminal_instances`] /
    /// [`ReadStore::replay_terminal_archive`].
    archive_path: Option<PathBuf>,
}

/// Result of projecting a batch of events into the read model.
pub struct ExportOutcome {
    /// Keys of instances that made a *genuine* Active->terminal transition in
    /// this batch (idempotent re-deliveries excluded). Used to evict hot engine
    /// state exactly once per instance.
    pub terminal_keys: Vec<Key>,
    /// Exact net change to the in-flight instance gauge for this batch:
    /// `+genuine_creates - genuine_terminals`. Because it counts only real state
    /// transitions (never raw event occurrences), re-delivered create/terminal
    /// events contribute zero, so the gauge cannot drift under idempotent replay.
    pub inflight_delta: i64,
}

impl ReadStore {
    /// Opens the read store at `path`, or an in-memory database when `path` is
    /// `None`. A persistent database whose schema version lags is brought current
    /// via **non-destructive additive migration** ([`reconcile_to_schema`]), so
    /// existing rows are preserved (issue #831). A destructive drop-and-recreate
    /// is reserved for the case where the live schema is genuinely incompatible
    /// with an additive migration (or cannot be read); a subsequent rebuild from
    /// the journal then repopulates it.
    pub fn open(path: Option<&Path>) -> rusqlite::Result<Self> {
        Self::open_inner(path, true)
    }

    /// Co-located durable terminal-audit archive path for a read-model file:
    /// `read-model.sqlite` -> `read-model.terminal-archive.sqlite` (issue #831).
    fn terminal_archive_path(base: &Path) -> PathBuf {
        let stem = base
            .file_stem()
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_else(|| "read-model".into());
        let name = match base.extension() {
            Some(ext) => format!("{stem}.terminal-archive.{}", ext.to_string_lossy()),
            None => format!("{stem}.terminal-archive"),
        };
        base.with_file_name(name)
    }

    /// Opens the read store. When `with_archive` and the store is file-backed, a
    /// co-located durable terminal-audit archive is opened once (creating/migrating
    /// its schema — it shares [`SCHEMA`], so it benefits from the same additive
    /// migrations and is never destructively wiped) and its path is retained.
    fn open_inner(path: Option<&Path>, with_archive: bool) -> rusqlite::Result<Self> {
        let conn = backend::open_connection(path)?;
        let archive_path = match (with_archive, path) {
            (true, Some(p)) => {
                let ap = Self::terminal_archive_path(p);
                // Open once to create/migrate the archive schema and prove it is
                // writable, then drop the connection — capture/replay re-attach it.
                Self::open_inner(Some(&ap), false)?;
                Some(ap)
            }
            _ => None,
        };
        let store = Self {
            conn: Mutex::new(conn),
            write_lock: Mutex::new(()),
            path: path.map(|p| p.to_path_buf()),
            archive_path,
        };
        store.ensure_schema()?;
        // A persistent store whose schema already matched is opened without any
        // write so far, so a read-only file (or directory) would not surface
        // until the first exporter batch — where it logs "attempt to write a
        // readonly database" every time and silently never advances the read
        // model. Probe writability now so that case fails fast at startup.
        if path.is_some() {
            store.check_writable()?;
        }
        // One-time capture of any terminal history that predates the archive, so
        // it is durable from the first boot of this fix (issue #831).
        if with_archive && path.is_some() {
            store.backfill_terminal_archive();
        }
        Ok(store)
    }

    /// Performs a trivial no-op write to confirm the database (and the directory
    /// it lives in) are writable. A self-assignment changes no data but still
    /// opens a write transaction and creates the rollback journal, exercising
    /// both file and directory permissions.
    fn check_writable(&self) -> rusqlite::Result<()> {
        let conn = self.conn.lock().expect("read store poisoned");
        conn.execute("UPDATE meta SET v = v WHERE k = 'schema_fingerprint'", [])?;
        Ok(())
    }

    fn ensure_schema(&self) -> rusqlite::Result<()> {
        let conn = self.conn.lock().expect("read store poisoned");
        Self::ensure_schema_on(&conn)
    }

    /// Brings the database at `conn` up to [`SCHEMA`] **non-destructively**
    /// (issue #831). Three cases:
    ///
    /// * **Fresh** (no user tables): create the whole schema from [`SCHEMA`] and
    ///   stamp `schema_version`/`exported_position = 0`.
    /// * **Already current** (`meta.schema_version == SCHEMA_VERSION`): nothing to
    ///   do — the fast path on every warm restart.
    /// * **Older, or a legacy fingerprint-only database**: additively reconcile
    ///   the live schema up to [`SCHEMA`] (add missing tables/columns/indexes,
    ///   never dropping data) and stamp the new version. `exported_position` is
    ///   preserved, so the projection is **not** reset below the compaction floor
    ///   — this is exactly the case that used to wipe completed history.
    fn ensure_schema_on(conn: &Connection) -> rusqlite::Result<()> {
        let user_tables = list_user_tables(conn)?;
        if user_tables.is_empty() {
            create_fresh_schema(conn)?;
            return Ok(());
        }
        let stored_version: Option<i64> = conn
            .query_row("SELECT v FROM meta WHERE k = 'schema_version'", [], |r| {
                r.get(0)
            })
            .optional()
            .unwrap_or(None);
        if stored_version == Some(SCHEMA_VERSION) {
            return Ok(());
        }
        // Older (or legacy fingerprint-only) database: migrate forward without
        // dropping any data. If the live schema is genuinely incompatible with an
        // additive migration (e.g. a foreign table missing a NOT NULL column that
        // cannot be back-filled), fall back to a destructive rebuild — the #732
        // below-floor recovery then reprojects live instances from the engine
        // snapshot. Additive evolution (the common case) never reaches this.
        if let Err(migrate_err) = reconcile_to_schema(conn) {
            tracing::warn!(
                error = %migrate_err,
                "read-model schema could not be additively migrated to the current \
                 version; rebuilding from scratch (live instances are recovered by the \
                 engine-snapshot reprojection, issue #732/#831)"
            );
            drop_all_user_tables(conn)?;
            create_fresh_schema(conn)?;
            return Ok(());
        }
        // Stamp version monotonically (never downgrade a database written by a
        // newer binary) and keep the fingerprint row in sync for tooling. Seed
        // `exported_position` only if absent — a warm database keeps its cursor,
        // so the projection is never reset below the compaction floor.
        // `event_waits` (v9) is projected from `*Created` events only, so a store
        // migrated from before v9 has no rows for waits armed before the upgrade.
        // Flag it; the boot catch-up backfills them from the engine snapshot
        // (`backfill_pending_event_waits`) before replaying the journal tail.
        if stored_version.is_none_or(|v| v < EVENT_WAITS_SCHEMA_VERSION) {
            conn.execute(
                "INSERT OR REPLACE INTO meta (k, v) VALUES (?1, 1)",
                params![EVENT_WAITS_BACKFILL_KEY],
            )?;
        }
        let new_version = stored_version.map_or(SCHEMA_VERSION, |v| v.max(SCHEMA_VERSION));
        conn.execute(
            "INSERT INTO meta (k, v) VALUES ('schema_version', ?1) \
             ON CONFLICT(k) DO UPDATE SET v = excluded.v",
            params![new_version],
        )?;
        conn.execute(
            "INSERT INTO meta (k, v) VALUES ('schema_fingerprint', ?1) \
             ON CONFLICT(k) DO UPDATE SET v = excluded.v",
            params![schema_fingerprint()],
        )?;
        conn.execute(
            "INSERT OR IGNORE INTO meta (k, v) VALUES ('exported_position', 0)",
            [],
        )?;
        Ok(())
    }

    /// How many journal events have already been durably projected. A boot
    /// catch-up replays only events at or after this offset.
    pub fn exported_position(&self) -> usize {
        let conn = self.conn.lock().expect("read store poisoned");
        let v: i64 = conn
            .query_row(
                "SELECT v FROM meta WHERE k = 'exported_position'",
                [],
                |r| r.get(0),
            )
            .unwrap_or(0);
        v.max(0) as usize
    }

    /// Advances `exported_position` by `n` events **without** projecting them
    /// into the instance/variable tables. Used by the remote-only projection
    /// sink: the heavy projection is offloaded to a remote target, but this
    /// shard is still the journal-compaction watermark keeper, so it must track
    /// how far the log has been handed off. This is a single integer `UPDATE`
    /// (tens of bytes of WAL) per batch — negligible next to full projection —
    /// so it removes the read model's dominant disk cost while keeping the
    /// compaction watermark honest. `n == 0` is a no-op.
    ///
    /// NOTE: like `export`, the watermark advances once the batch has been
    /// handed to the sink, not once a remote target has durably acknowledged it;
    /// ack-gated advancement (so a crash cannot compact past an un-acked batch)
    /// is a later milestone (see issue #133).
    pub fn advance_exported(&self, n: usize) -> rusqlite::Result<()> {
        if n == 0 {
            return Ok(());
        }
        let _write = self
            .write_lock
            .lock()
            .expect("read store write lock poisoned");
        let conn = self.conn.lock().expect("read store poisoned");
        conn.execute(
            "UPDATE meta SET v = v + ?1 WHERE k = 'exported_position'",
            params![n as i64],
        )?;
        Ok(())
    }

    /// Drops **every** user table and recreates the schema from scratch,
    /// resetting `exported_position` to 0. This is the sole remaining
    /// *destructive* rebuild path (contrast [`ReadStore::ensure_schema`], which is
    /// now additive — issue #831). It is used only when the persisted position is
    /// ahead of the journal (a corrupt or truncated log), forcing a full rebuild
    /// by replay, or by the below-floor reprojection recovery (issue #732), which
    /// immediately reseeds from the authoritative engine snapshot afterwards.
    ///
    /// The drop set is derived from `sqlite_master` (not a hand-maintained list,
    /// which silently drifts as `SCHEMA` gains tables), so it can never fall
    /// behind `SCHEMA`.
    pub fn reset(&self) -> rusqlite::Result<()> {
        // Serialize against the adaptive pruner's separate connection in-process
        // (see `write_lock`), exactly like `export`/`advance_exported`: `reset`
        // performs destructive DDL, so running it concurrently with a pruning /
        // export / replay WAL writer would race SQLite's lock and trip
        // `database is locked`.
        let _write = self
            .write_lock
            .lock()
            .expect("read store write lock poisoned");
        let conn = self.conn.lock().expect("read store poisoned");
        drop_all_user_tables(&conn)?;
        create_fresh_schema(&conn)?;
        Ok(())
    }

    /// Projects a batch of consecutive journal `events` into the store in one
    /// transaction and advances `exported_position` by `events.len()`. Returns
    /// the keys of instances that completed in this batch, so the caller can
    /// evict them from hot engine state. Projection is idempotent, so replaying
    /// an overlapping prefix is safe. Takes event references so a caller batching
    /// several `Arc<Vec<Event>>` can project them without deep-copying payloads.
    pub fn export(&self, events: &[&Event]) -> rusqlite::Result<ExportOutcome> {
        // Serialize against the adaptive pruner's separate connection in-process
        // (see `write_lock`) so the two WAL writers never race SQLite's lock and
        // trip `database is locked`; this is a cheap uncontended lock on the
        // common path (pruner idle) and a short wait when the pruner is active.
        let _write = self
            .write_lock
            .lock()
            .expect("read store write lock poisoned");
        let mut conn = self.conn.lock().expect("read store poisoned");
        let tx = conn.transaction()?;
        let mut terminal_keys = Vec::new();
        let mut inflight_delta: i64 = 0;
        let now = now_ms();
        for &event in events {
            let d = project(&tx, event, now)?;
            inflight_delta += d;
            // Collect only GENUINE terminal transitions (d < 0) for hot-state
            // eviction; a re-delivered terminal (d == 0) was already evicted.
            if d < 0
                && let Event::ProcessInstanceCompleted { instance_key }
                | Event::ProcessInstanceTerminated { instance_key } = event
            {
                terminal_keys.push(*instance_key);
            }
        }
        tx.cexecute(
            "UPDATE meta SET v = v + ?1 WHERE k = 'exported_position'",
            params![events.len() as i64],
        )?;
        tx.commit()?;
        // Durably archive any newly-terminal instances (issue #831) so their
        // audit history survives a below-floor snapshot reprojection, which can
        // only recover *live* instances from the engine snapshot. Best-effort: a
        // capture failure must never fail the (already-committed) projection or
        // stall the exporter, so it is logged and swallowed — the read model still
        // holds the terminal rows until they are pruned, and the next reprojection
        // path degrades to the prior (#732) behaviour for anything unarchived.
        if !terminal_keys.is_empty()
            && let Some(archive) = &self.archive_path
            && let Err(e) = copy_terminal_to_archive(&conn, archive, &terminal_keys)
        {
            tracing::warn!(
                error = %e,
                count = terminal_keys.len(),
                "failed to write terminal instances to the durable audit archive \
                 (issue #831); the read model still holds them until pruned"
            );
        }
        Ok(ExportOutcome {
            terminal_keys,
            inflight_delta,
        })
    }

    /// Copies every terminal instance currently in the read model into the
    /// durable archive **once** (issue #831), gated by a `meta` flag so it runs a
    /// single time per (re)built read model. This captures history that completed
    /// *before* the archive existed — the exact merlin.local data that was
    /// unrecoverable — so it becomes durable on the first boot of a binary carrying
    /// this fix, not only for instances that complete afterwards. Best-effort: a
    /// failure is logged and swallowed so it never blocks startup.
    fn backfill_terminal_archive(&self) {
        let Some(archive) = &self.archive_path else {
            return;
        };
        let conn = self.conn.lock().expect("read store poisoned");
        let done: i64 = conn
            .query_row(
                "SELECT v FROM meta WHERE k = 'terminal_archive_backfilled'",
                [],
                |r| r.get(0),
            )
            .optional()
            .unwrap_or(None)
            .unwrap_or(0);
        if done != 0 {
            return;
        }
        let result = (|| -> rusqlite::Result<()> {
            attach_archive(&conn, archive)?;
            let copy = (|| -> rusqlite::Result<()> {
                let terminal = terminal_state_predicate("state");
                let pi_cols =
                    shared_column_list(&conn, "main", "terminal_archive", "process_instances")?;
                conn.execute(
                    &format!(
                        "INSERT OR IGNORE INTO terminal_archive.process_instances ({pi_cols}) \
                         SELECT {pi_cols} FROM main.process_instances WHERE {terminal}"
                    ),
                    [],
                )?;
                for table in TERMINAL_ARCHIVE_DEP_TABLES {
                    let cols = shared_column_list(&conn, "main", "terminal_archive", table)?;
                    conn.execute(
                        &format!(
                            "INSERT OR IGNORE INTO terminal_archive.{table} ({cols}) \
                             SELECT {cols} FROM main.{table} WHERE instance_key IN \
                             (SELECT key FROM main.process_instances WHERE {terminal})"
                        ),
                        [],
                    )?;
                }
                Ok(())
            })();
            let _ = conn.execute_batch("DETACH DATABASE terminal_archive");
            copy?;
            conn.execute(
                "INSERT INTO meta (k, v) VALUES ('terminal_archive_backfilled', 1) \
                 ON CONFLICT(k) DO UPDATE SET v = excluded.v",
                [],
            )?;
            Ok(())
        })();
        if let Err(e) = result {
            tracing::warn!(
                error = %e,
                "failed to backfill pre-existing terminal history into the durable \
                 audit archive (issue #831); newly-completing instances are still archived"
            );
        }
    }

    /// Replays the durable terminal-audit archive (issue #831) into this shard,
    /// restoring completed/terminal instances (and their user tasks, variables and
    /// decision evaluations) that a snapshot reprojection could not recover from
    /// the live-only engine snapshot. `INSERT OR IGNORE` so a live row from the
    /// snapshot is never clobbered by an older archived copy. Returns the number
    /// of process instances restored. A no-op for `:memory:` stores or when the
    /// archive file does not yet exist.
    pub fn replay_terminal_archive(&self) -> rusqlite::Result<usize> {
        let Some(archive) = &self.archive_path else {
            return Ok(0);
        };
        if !archive.exists() {
            return Ok(0);
        }
        let _write = self
            .write_lock
            .lock()
            .expect("read store write lock poisoned");
        let conn = self.conn.lock().expect("read store poisoned");
        attach_archive(&conn, archive)?;
        let restored = (|| -> rusqlite::Result<usize> {
            let pi_cols =
                shared_column_list(&conn, "terminal_archive", "main", "process_instances")?;
            let n = conn.execute(
                &format!(
                    "INSERT OR IGNORE INTO main.process_instances ({pi_cols}) \
                     SELECT {pi_cols} FROM terminal_archive.process_instances"
                ),
                [],
            )?;
            for table in TERMINAL_ARCHIVE_DEP_TABLES {
                let cols = shared_column_list(&conn, "terminal_archive", "main", table)?;
                conn.execute(
                    &format!(
                        "INSERT OR IGNORE INTO main.{table} ({cols}) \
                         SELECT {cols} FROM terminal_archive.{table}"
                    ),
                    [],
                )?;
            }
            Ok(n)
        })();
        let _ = conn.execute_batch("DETACH DATABASE terminal_archive");
        restored
    }

    /// Backfills `event_waits` from the boot engine `state` if this store was
    /// migrated from a schema predating it (see [`Self::ensure_schema_on`]),
    /// then clears the pending flag. Call it BEFORE replaying the journal tail
    /// the `state` already reflects: replaying a `*Created` for a backfilled
    /// wait is a no-op and a later settle event deletes it, so the result
    /// matches a full projection. Returns whether a backfill ran.
    pub fn backfill_pending_event_waits(
        &self,
        state: &nanobpmn_engine_core::State,
    ) -> rusqlite::Result<bool> {
        let _write = self
            .write_lock
            .lock()
            .expect("read store write lock poisoned");
        let mut conn = self.conn.lock().expect("read store poisoned");
        let tx = conn.transaction()?;
        let pending = tx
            .query_row(
                "SELECT v FROM meta WHERE k = ?1",
                params![EVENT_WAITS_BACKFILL_KEY],
                |r| r.get::<_, i64>(0),
            )
            .optional()?
            .is_some();
        if pending {
            project_event_waits_from_state(&tx, state)?;
            tx.execute(
                "DELETE FROM meta WHERE k = ?1",
                params![EVENT_WAITS_BACKFILL_KEY],
            )?;
        }
        tx.commit()?;
        Ok(pending)
    }

    /// Rebuilds this (reset) shard's rows from a boot engine [`State`] snapshot,
    /// WITHOUT advancing `exported_position` — the caller then plants the cursor
    /// at the absolute event count this `State` already reflects (typically
    /// `total_events`). Used only by the below-compaction-floor recovery
    /// path (issue #732), where the journal events that would replay into the read
    /// model have been compacted away but the authoritative engine snapshot still
    /// holds every live entity. Idempotent — reused rows are guarded with
    /// `ON CONFLICT ... DO NOTHING` (mirroring the event projector), so it is
    /// safe over a freshly `reset()` shard.
    pub fn seed_from_engine_state(
        &self,
        state: &nanobpmn_engine_core::State,
    ) -> rusqlite::Result<()> {
        let _write = self
            .write_lock
            .lock()
            .expect("read store write lock poisoned");
        let mut conn = self.conn.lock().expect("read store poisoned");
        let tx = conn.transaction()?;
        project_engine_state(&tx, state, now_ms())?;
        tx.commit()?;
        Ok(())
    }

    /// Caps retained *terminal* (Completed/Terminated) process instances at
    /// `max_keep`, deleting the oldest beyond the cap together with all their
    /// dependent rows (variables, jobs, incidents, user tasks). Active instances
    /// are never touched. Returns the number of instances evicted.
    ///
    /// This bounds read-model memory to the working set instead of letting it
    /// grow without limit with cumulative throughput: without it, every
    /// completed instance — and its full variable payload — is retained forever,
    /// so a long-running engine's memory climbs indefinitely even with no active
    /// processes. An in-memory (`:memory:`) store never returns freed pages to
    /// the OS, so the win is *prevention* — pruning continuously keeps the page
    /// arena from ballooning in the first place (freed pages are reused by new
    /// instances). `max_keep == 0` disables pruning (unbounded history, the
    /// default — see `NANOBPMN_HISTORY_MAX_INSTANCES`).
    ///
    /// Terminal instances are ordered by `key`, which is monotonic in creation
    /// order, so the most recently created terminal instances are retained.
    ///
    /// `max_delete` bounds a single sweep to the *oldest* `max_delete` terminal
    /// instances beyond `max_keep` (0 = unbounded). This is essential when a
    /// store that has grown well past budget first crosses the retention
    /// watermark: pruning the entire backlog in one transaction would build a
    /// multi-gigabyte WAL and block this shard's single exporter thread for
    /// seconds, during which the unbounded exporter channel backs up with events
    /// and process RSS explodes. Bounding each sweep keeps every prune transaction
    /// small and quick so the exporter stays responsive; a backlog is worked down
    /// gently across successive sweeps, while steady-state sweeps (only a
    /// prune-threshold's worth of new completions exceed `max_keep`) delete a
    /// small batch and the store holds flat. After a non-empty sweep the WAL is
    /// checkpoint-truncated so it does not accumulate the freed pages.
    #[cfg(feature = "native")]
    pub fn prune_terminal_instances(
        &self,
        max_keep: usize,
        max_delete: usize,
    ) -> rusqlite::Result<usize> {
        if max_keep == 0 {
            return Ok(0);
        }
        let mut conn = self.conn.lock().expect("read store poisoned");
        let tx = conn.transaction()?;
        // Materialize the keys to evict: of the terminal instances beyond the most
        // recent `max_keep` (inner `LIMIT -1 OFFSET max_keep` = "all but the newest
        // max_keep"), take the OLDEST `max_delete` of them (outer `ORDER BY key ASC
        // LIMIT`). `max_delete == 0` => `LIMIT -1` (unbounded).
        tx.execute_batch(
            "CREATE TEMP TABLE IF NOT EXISTS _evict(key INTEGER PRIMARY KEY);
             DELETE FROM _evict;",
        )?;
        let del_limit: i64 = if max_delete == 0 {
            -1
        } else {
            max_delete as i64
        };
        let evicted = tx.execute(
            &format!(
                "INSERT INTO _evict(key) \
                 SELECT key FROM ( \
                   SELECT key FROM process_instances WHERE {} \
                   ORDER BY key DESC LIMIT -1 OFFSET ?1 \
                 ) ORDER BY key ASC LIMIT ?2",
                terminal_state_predicate("state")
            ),
            params![max_keep as i64, del_limit],
        )?;
        if evicted == 0 {
            tx.commit()?;
            return Ok(0);
        }
        for table in [
            "variables",
            "jobs",
            "incidents",
            "user_tasks",
            "element_instances",
        ] {
            tx.execute(
                &format!("DELETE FROM {table} WHERE instance_key IN (SELECT key FROM _evict)"),
                [],
            )?;
        }
        tx.execute(
            "DELETE FROM process_instances WHERE key IN (SELECT key FROM _evict)",
            [],
        )?;
        tx.commit()?;
        // Return the WAL's freed pages to a bounded size — but only once it has
        // grown meaningfully. TRUNCATE-ing after every sweep copies the whole WAL
        // back into the random-access main DB and was a dominant write-amplifier;
        // size-gating it (default 32 MiB) keeps the WAL bounded via the raised
        // autocheckpoint and reclaims file space only occasionally. TRUNCATE is
        // best-effort: a concurrent reader can hold it back, and that is fine —
        // the next sweep retries.
        if self.wal_len_bytes() >= read_wal_truncate_bytes() {
            let _ = conn.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_| Ok(()));
        }
        Ok(evicted)
    }

    // --- queries used by the search/get handlers ---

    /// The on-disk size of this shard's SQLite database in bytes, as
    /// `(file_bytes, live_bytes)`: `file_bytes` is the total allocated file
    /// (`page_count × page_size`, including freelist pages SQLite keeps for
    /// reuse and does not return to the OS without `VACUUM`); `live_bytes`
    /// excludes the freelist (`(page_count − freelist_count) × page_size`) and
    /// tracks the actual data. Adaptive retention keeps `live_bytes` near its
    /// budget by evicting old terminal instances; `file_bytes` stays at the
    /// high-watermark (freed pages are reused, not returned to the OS) and so
    /// plateaus rather than growing without bound.
    #[cfg(feature = "native")]
    pub fn db_page_stats(&self) -> (u64, u64) {
        let conn = self.conn.lock().expect("read store poisoned");
        page_stats(&conn)
    }

    /// Opens a second connection to this shard's database file for the decoupled
    /// adaptive pruner (see [`prune_oldest_terminal`]). Returns `Ok(None)` for an
    /// in-memory store (a second connection would be a distinct empty database),
    /// so the caller keeps pruning inline in that case. This connection's delete
    /// transactions are serialized against the exporter by the shared in-process
    /// [`ReadStore::write_lock`] (see [`ReadStore::adaptive_prune_once`]), so the
    /// two writers never race SQLite's WAL lock; `busy_timeout` remains only as a
    /// backstop for any writer this process does not coordinate (e.g. an external
    /// reader holding a checkpoint back).
    #[cfg(feature = "native")]
    pub fn prune_connection(&self) -> rusqlite::Result<Option<Connection>> {
        let Some(path) = self.path.as_ref() else {
            return Ok(None);
        };
        let conn = Connection::open(path)?;
        conn.busy_timeout(std::time::Duration::from_secs(5))?;
        Ok(Some(conn))
    }

    /// Runs one adaptive-prune wake on the pruner's own `conn`. Cheap when under
    /// budget: a single O(1) `page_stats` read and return. When `live_bytes`
    /// reaches `high_bytes`, evicts the oldest terminal instances in `batch`
    /// chunks until `live_bytes` falls to `low_bytes` (hysteresis prevents
    /// per-insert thrashing), or `max_deletes` rows have been evicted this wake
    /// (bounds how long the write lock is held away from the exporter), or no
    /// terminal instances remain. Returns the number evicted; checkpoint-truncates
    /// the WAL if it deleted anything so freed pages do not accumulate there.
    #[cfg(feature = "native")]
    pub fn adaptive_prune_once(
        &self,
        conn: &mut Connection,
        high_bytes: u64,
        low_bytes: u64,
        batch: usize,
        max_deletes: usize,
    ) -> rusqlite::Result<usize> {
        let (_, live) = page_stats(conn);
        if live < high_bytes {
            return Ok(0);
        }
        let mut total = 0usize;
        while total < max_deletes {
            let (_, live) = page_stats(conn);
            if live <= low_bytes {
                break;
            }
            let want = batch.min(max_deletes - total);
            // Gate each delete chunk on the shared in-process write lock so the
            // pruner's connection never writes to the WAL while the exporter's
            // does (no `database is locked`); the lock is released between chunks
            // so the exporter interleaves and is never starved for a whole wake.
            let evicted = {
                let _write = self
                    .write_lock
                    .lock()
                    .expect("read store write lock poisoned");
                prune_oldest_terminal(conn, want)?
            };
            if evicted == 0 {
                break;
            }
            total += evicted;
        }
        if total > 0 {
            // Checkpointing is deferred to the size-gated `maybe_checkpoint_wal`
            // (called every pruner wake): TRUNCATE-ing the whole WAL back into the
            // random-access main DB after *every* delete sweep (~5/s under
            // sustained pressure) was a dominant source of read-model write
            // amplification. The deletes' freed pages sit in the WAL until the
            // next size-gated checkpoint, bounded by the raised autocheckpoint.
        }
        Ok(total)
    }

    /// Size of this shard's `-wal` sidecar file in bytes (0 if absent / in-memory).
    /// An O(1) `stat`; cheap enough for the pruner's per-wake gate.
    #[cfg(feature = "native")]
    fn wal_len_bytes(&self) -> u64 {
        let Some(path) = self.path.as_ref() else {
            return 0;
        };
        let mut wal = path.clone().into_os_string();
        wal.push("-wal");
        std::fs::metadata(std::path::PathBuf::from(wal))
            .map(|m| m.len())
            .unwrap_or(0)
    }

    /// Size-gated WAL checkpoint, run once per pruner wake on the pruner's own
    /// connection (off the exporter's hot path). When the `-wal` sidecar has grown
    /// to at least [`read_wal_truncate_bytes`], TRUNCATE-checkpoints it back into
    /// the main DB and reclaims the WAL file space; otherwise a no-op. This
    /// concentrates all checkpoint copy-back into infrequent, coalesced passes
    /// instead of a per-delete-sweep storm, and keeps those passes off the single
    /// exporter thread so projection never stalls mid-checkpoint. Returns whether
    /// a checkpoint ran. Best-effort: a concurrent reader can hold TRUNCATE back,
    /// which is fine — the next wake retries.
    #[cfg(feature = "native")]
    pub fn maybe_checkpoint_wal(&self, conn: &Connection) -> bool {
        self.checkpoint_wal_if_larger_than(conn, read_wal_truncate_bytes())
    }

    /// Core of [`maybe_checkpoint_wal`] with an explicit byte threshold (so tests
    /// can exercise the gate without racing a process-global env var).
    #[cfg(feature = "native")]
    fn checkpoint_wal_if_larger_than(&self, conn: &Connection, threshold: u64) -> bool {
        if self.path.is_none() {
            return false;
        }
        if self.wal_len_bytes() < threshold {
            return false;
        }
        let _write = self
            .write_lock
            .lock()
            .expect("read store write lock poisoned");
        let _ = conn.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_| Ok(()));
        true
    }

    /// Total number of process instances (active + terminal) in this shard, via
    /// a live `COUNT(*)`. This is O(rows), so the adaptive-retention caller must
    /// only invoke it when a shard is over its byte budget (never on the
    /// below-budget ramp) and throttle it in time — a naive event-count proxy is
    /// unsafe here because projection is idempotent/re-delivered, so counting
    /// `ProcessInstanceCreated` events over-counts the deduplicated rows and
    /// inflates the keep target until pruning silently evicts nothing.
    pub fn instance_count(&self) -> usize {
        let conn = self.conn.lock().expect("read store poisoned");
        conn.query_row("SELECT COUNT(*) FROM process_instances", [], |r| {
            r.get::<_, i64>(0)
        })
        .map(|n| n.max(0) as usize)
        .unwrap_or(0)
    }

    /// The number of non-terminal (Active) process instances currently in the
    /// read model. Used once at startup to seed the in-flight backpressure gauge
    /// after a journal replay, so the watermark reflects recovered work.
    pub fn active_instance_count(&self) -> usize {
        let conn = self.conn.lock().expect("read store poisoned");
        conn.query_row(
            "SELECT COUNT(*) FROM process_instances WHERE state = 0",
            [],
            |r| r.get::<_, i64>(0),
        )
        .map(|n| n as usize)
        .unwrap_or(0)
    }

    /// Reconciles orphaned `Active` rows against authoritative engine state.
    ///
    /// A read row can be stranded in `Active` (`state = 0`) when its CREATE was
    /// projected here but the matching terminal event never was — e.g. this shard
    /// projected the create while it led the partition, then leadership moved and
    /// the completion was applied+exported by the *new* leader, so the terminal
    /// transition never reached this read model. Such a row inflates
    /// [`active_instance_count`](Self::active_instance_count) — and hence the
    /// in-flight admission gauge it seeds at boot — forever, even though the
    /// engine holds no such live instance (the engine evicts an instance the
    /// instant it reaches a terminal state).
    ///
    /// `live` is the set of instance keys the engine actually holds (hot ∪ cold)
    /// for the partitions this shard covers. Every `Active` row whose key is
    /// absent from `live` is transitioned to `Completed` (best effort: the engine
    /// evicted it on reaching a terminal state, and completion is the dominant
    /// drain path). The `WHERE state = 0` guard makes this idempotent and safe to
    /// race with a genuinely in-flight completion event: whichever applies first
    /// transitions the row, the other is a no-op, so the instance is counted
    /// exactly once. Returns the number of rows reconciled — the amount by which
    /// the in-flight gauge was over-counting.
    pub fn reconcile_orphaned_active(&self, live: &std::collections::HashSet<Key>) -> usize {
        let mut conn = self.conn.lock().expect("read store poisoned");
        let active: Vec<i64> = {
            let mut stmt = match conn.prepare("SELECT key FROM process_instances WHERE state = 0") {
                Ok(s) => s,
                Err(_) => return 0,
            };
            let rows = match stmt.query_map([], |r| r.get::<_, i64>(0)) {
                Ok(r) => r,
                Err(_) => return 0,
            };
            rows.filter_map(|r| r.ok()).collect()
        };
        let orphans: Vec<i64> = active
            .into_iter()
            .filter(|k| !live.contains(&(*k as Key)))
            .collect();
        if orphans.is_empty() {
            return 0;
        }
        let tx = match conn.transaction() {
            Ok(t) => t,
            Err(_) => return 0,
        };
        let completed = instance_state_code(ProcessInstanceState::Completed);
        let resolved = incident_state_code(IncidentState::Resolved);
        let active_inc = incident_state_code(IncidentState::Active);
        let mut reconciled = 0usize;
        for k in &orphans {
            if let Ok(1) = tx.cexecute(
                "UPDATE process_instances SET state = ?2, has_incident = 0 \
                 WHERE key = ?1 AND state = 0",
                params![*k, completed],
            ) {
                reconciled += 1;
            }
            // Close any still-open incident so the reconciled instance does not
            // surface as having an open incident.
            let _ = tx.cexecute(
                "UPDATE incidents SET state = ?2 WHERE instance_key = ?1 AND state = ?3",
                params![*k, resolved, active_inc],
            );
        }
        if tx.commit().is_err() {
            return 0;
        }
        reconciled
    }

    pub fn process_instances(&self) -> Vec<ProcessInstanceRow> {
        let conn = self.conn.lock().expect("read store poisoned");
        let mut stmt = conn
            .prepare(
                "SELECT key, process_id, process_definition_id, process_definition_key, \
                 version, state, start_date_ms, has_incident, tags, business_id, parent_process_instance_key, parent_element_instance_key, suspended_date_ms FROM process_instances",
            )
            .expect("prepare process_instances");
        let rows = stmt
            .query_map([], map_instance)
            .expect("query process_instances");
        rows.filter_map(Result::ok).collect()
    }

    /// Number of process instances matching `filter` — the page count for the
    /// console's paginated instance list. With [`InstanceFilter::default`] this
    /// is the cheap unfiltered `COUNT(*)`; with a filter it applies the **same**
    /// predicate the page query uses, so the pager total tracks the filtered set
    /// (never a desynced unfiltered count).
    pub fn process_instance_count(&self, filter: &InstanceFilter) -> i64 {
        let conn = self.conn.lock().expect("read store poisoned");
        let sql = format!(
            "SELECT COUNT(*) FROM process_instances{}",
            filter.where_clause()
        );
        conn.cquery_row(&sql, [], |r| r.get(0)).unwrap_or(0)
    }

    /// One page of process instances matching `filter`, newest first. Orders by
    /// `key DESC` — keys are monotonic so this is newest-first (the same
    /// ordering the retention prune uses) and rides the integer PRIMARY KEY
    /// index, so it is `O(limit + offset)` in SQLite rather than loading and
    /// sorting every row in memory (which is what made the console hang on large
    /// datasets). The filter predicate is single-sourced with
    /// [`process_instance_count`](Self::process_instance_count).
    pub fn process_instances_page(
        &self,
        limit: i64,
        offset: i64,
        filter: &InstanceFilter,
    ) -> Vec<ProcessInstanceRow> {
        let conn = self.conn.lock().expect("read store poisoned");
        let sql = format!(
            "SELECT key, process_id, process_definition_id, process_definition_key, \
                 version, state, start_date_ms, has_incident, tags, business_id, \
                 parent_process_instance_key, parent_element_instance_key, suspended_date_ms \
                 FROM process_instances{} ORDER BY key DESC LIMIT ?1 OFFSET ?2",
            filter.where_clause()
        );
        let mut stmt = conn.prepare(&sql).expect("prepare process_instances_page");
        let rows = stmt
            .query_map(params![limit, offset], map_instance)
            .expect("query process_instances_page");
        rows.filter_map(Result::ok).collect()
    }

    pub fn process_instance(&self, key: Key) -> Option<ProcessInstanceRow> {
        let conn = self.conn.lock().expect("read store poisoned");
        conn.query_row(
            "SELECT key, process_id, process_definition_id, process_definition_key, \
             version, state, start_date_ms, has_incident, tags, business_id, parent_process_instance_key, parent_element_instance_key, suspended_date_ms FROM process_instances WHERE key = ?1",
            params![key as i64],
            map_instance,
        )
        .optional()
        .expect("query process_instance")
    }

    /// Resolves `key`'s `rootProcessInstanceKey` (issue #977) by walking the
    /// `parentProcessInstanceKey` chain via this store's own point lookups. A
    /// call-activity hierarchy is partition-co-located — the engine mints a child
    /// on its parent's partition (`compose_key(self.partition_id, …)` in
    /// `engine-core`), so the whole parent → child → grandchild chain lives in a
    /// single store — hence a single-store walk resolves the true top-level root
    /// without any cross-partition routing. Delegates to the shared
    /// [`resolve_root_process_instance_key`] so this shares the gateway's exact
    /// algorithm.
    pub fn root_process_instance_key(&self, key: Key) -> Key {
        resolve_root_process_instance_key(key, |k| self.process_instance(k))
    }

    pub fn jobs(&self) -> Vec<JobRow> {
        let conn = self.conn.lock().expect("read store poisoned");
        let mut stmt = conn
            .prepare(
                "SELECT key, instance_key, element_instance_key, element_id, job_type, state, \
                 retries, worker, deadline_ms, process_definition_id, process_definition_key, \
                 job_kind, listener_event_type, created_at_ms, read_set, CAST(lease_token AS TEXT), \
                 business_id, error_message, error_code, has_failed_with_retries_left, \
                 last_update_ms, end_ms FROM jobs",
            )
            .expect("prepare jobs");
        let rows = stmt.query_map([], map_job).expect("query jobs");
        rows.filter_map(Result::ok).collect()
    }

    pub fn user_tasks(&self) -> Vec<UserTaskRow> {
        let conn = self.conn.lock().expect("read store poisoned");
        let mut stmt = conn
            .prepare(&format!("SELECT {USER_TASK_COLS} FROM user_tasks"))
            .expect("prepare user_tasks");
        let rows = stmt.query_map([], map_user_task).expect("query user_tasks");
        rows.filter_map(Result::ok).collect()
    }

    pub fn user_task(&self, key: Key) -> Option<UserTaskRow> {
        let conn = self.conn.lock().expect("read store poisoned");
        conn.query_row(
            &format!("SELECT {USER_TASK_COLS} FROM user_tasks WHERE key = ?1"),
            params![key as i64],
            map_user_task,
        )
        .optional()
        .expect("query user_task")
    }

    pub fn incidents(&self) -> Vec<IncidentRow> {
        let conn = self.conn.lock().expect("read store poisoned");
        let mut stmt = conn
            .prepare(
                "SELECT key, instance_key, element_instance_key, element_id, kind, state, \
                 reason, job_key, created_at_ms, process_definition_id, process_definition_key \
                 FROM incidents",
            )
            .expect("prepare incidents");
        let rows = stmt.query_map([], map_incident).expect("query incidents");
        rows.filter_map(Result::ok).collect()
    }

    pub fn incident(&self, key: Key) -> Option<IncidentRow> {
        let conn = self.conn.lock().expect("read store poisoned");
        conn.query_row(
            "SELECT key, instance_key, element_instance_key, element_id, kind, state, \
             reason, job_key, created_at_ms, process_definition_id, process_definition_key \
             FROM incidents WHERE key = ?1",
            params![key as i64],
            map_incident,
        )
        .optional()
        .expect("query incident")
    }

    /// All element-instance rows in this shard.
    pub fn element_instances(&self) -> Vec<ElementInstanceRow> {
        let conn = self.conn.lock().expect("read store poisoned");
        let mut stmt = conn
            .prepare(&format!(
                "SELECT {ELEMENT_INSTANCE_COLS} FROM element_instances"
            ))
            .expect("prepare element_instances");
        let rows = stmt
            .query_map([], map_element_instance)
            .expect("query element_instances");
        rows.filter_map(Result::ok).collect()
    }

    /// A single element instance by its key.
    pub fn element_instance(&self, key: Key) -> Option<ElementInstanceRow> {
        let conn = self.conn.lock().expect("read store poisoned");
        conn.query_row(
            &format!(
                "SELECT {ELEMENT_INSTANCE_COLS} FROM element_instances WHERE element_instance_key = ?1"
            ),
            params![key as i64],
            map_element_instance,
        )
        .optional()
        .expect("query element_instance")
    }

    /// The `Active` element instances for one process instance (its live token
    /// positions). Selects by `instance_key` via `idx_element_instances_instance`
    /// and filters to the `Active` state code in SQL, so this stays O(rows for
    /// this instance) rather than scanning every element instance in the shard.
    pub fn active_element_instances(&self, instance_key: Key) -> Vec<ElementInstanceRow> {
        let conn = self.conn.lock().expect("read store poisoned");
        let mut stmt = conn
            .prepare(&format!(
                "SELECT {ELEMENT_INSTANCE_COLS} FROM element_instances \
                 WHERE instance_key = ?1 AND state = ?2"
            ))
            .expect("prepare active_element_instances");
        let rows = stmt
            .query_map(
                params![
                    instance_key as i64,
                    element_instance_state_code(ElementInstanceState::Active)
                ],
                map_element_instance,
            )
            .expect("query active_element_instances");
        rows.filter_map(Result::ok).collect()
    }

    /// All open message subscriptions in this shard (MESSAGE wait states).
    pub fn message_subscriptions(&self) -> Vec<MessageSubscriptionRow> {
        let conn = self.conn.lock().expect("read store poisoned");
        let mut stmt = conn
            .prepare(&format!(
                "SELECT {MESSAGE_SUBSCRIPTION_COLS} FROM message_subscriptions"
            ))
            .expect("prepare message_subscriptions");
        let rows = stmt
            .query_map([], map_message_subscription)
            .expect("query message_subscriptions");
        rows.filter_map(Result::ok).collect()
    }

    /// All open timer / signal / conditional waits in this shard.
    pub fn event_waits(&self) -> Vec<EventWaitRow> {
        let conn = self.conn.lock().expect("read store poisoned");
        let mut stmt = conn
            .prepare(
                "SELECT wait_key, wait_type, instance_key, element_instance_key, element_id, \
                 detail, due_at_ms FROM event_waits",
            )
            .expect("prepare event_waits");
        let rows = stmt
            .query_map([], |r| {
                let code: String = r.get(1)?;
                let Some(wait_type) = EventWaitType::parse(&code) else {
                    return Ok(None);
                };
                Ok(Some(EventWaitRow {
                    wait_key: r.get::<_, i64>(0)? as Key,
                    wait_type,
                    instance_key: r.get::<_, i64>(2)? as Key,
                    element_instance_key: r.get::<_, i64>(3)? as Key,
                    element_id: r.get(4)?,
                    detail: r.get(5)?,
                    due_at_ms: r.get::<_, Option<i64>>(6)?.map(|v| v.max(0) as u64),
                }))
            })
            .expect("query event_waits");
        rows.filter_map(|r| r.ok().flatten()).collect()
    }

    /// All correlated (historical) message subscriptions in this shard.
    pub fn correlated_message_subscriptions(&self) -> Vec<CorrelatedMessageSubscriptionRow> {
        let conn = self.conn.lock().expect("read store poisoned");
        let mut stmt = conn
            .prepare(&format!(
                "SELECT {CORRELATED_MESSAGE_SUBSCRIPTION_COLS} FROM correlated_message_subscriptions"
            ))
            .expect("prepare correlated_message_subscriptions");
        let rows = stmt
            .query_map([], map_correlated_message_subscription)
            .expect("query correlated_message_subscriptions");
        rows.filter_map(Result::ok).collect()
    }

    /// All decision-instance rows in this shard.
    pub fn decision_instances(&self) -> Vec<DecisionInstanceRow> {
        let conn = self.conn.lock().expect("read store poisoned");
        let sql = format!("SELECT {DECISION_INSTANCE_COLS} FROM decision_instances");
        let mut stmt = conn.prepare(&sql).expect("prepare decision_instances");
        let rows = stmt
            .query_map([], map_decision_instance)
            .expect("query decision_instances");
        rows.filter_map(Result::ok).collect()
    }

    /// A single decision-instance by its `<decisionEvaluationKey>-<index>` id.
    pub fn decision_instance(&self, eval_instance_key: &str) -> Option<DecisionInstanceRow> {
        let conn = self.conn.lock().expect("read store poisoned");
        let sql = format!(
            "SELECT {DECISION_INSTANCE_COLS} FROM decision_instances WHERE eval_instance_key = ?1"
        );
        conn.query_row(&sql, params![eval_instance_key], map_decision_instance)
            .optional()
            .expect("query decision_instance")
    }

    /// Every decision-instance row sharing a `decision_evaluation_key` (one per
    /// evaluated decision in that evaluation), ordered by within-evaluation index.
    /// Used by the DeleteDecisionInstance handler to resolve the owning process
    /// instance (for partition routing) and to detect a not-found evaluation.
    pub fn decision_instances_by_evaluation_key(
        &self,
        decision_evaluation_key: Key,
    ) -> Vec<DecisionInstanceRow> {
        let conn = self.conn.lock().expect("read store poisoned");
        let sql = format!(
            "SELECT {DECISION_INSTANCE_COLS} FROM decision_instances \
             WHERE decision_evaluation_key = ?1 ORDER BY idx"
        );
        let mut stmt = conn
            .prepare(&sql)
            .expect("prepare decision_instances_by_evaluation_key");
        let rows = stmt
            .query_map(
                params![decision_evaluation_key as i64],
                map_decision_instance,
            )
            .expect("query decision_instances_by_evaluation_key");
        rows.filter_map(Result::ok).collect()
    }

    pub fn process_definitions(&self) -> Vec<ProcessDefinitionRow> {
        let conn = self.conn.lock().expect("read store poisoned");
        // Return EVERY deployed version (Camunda/Zeebe parity: each version is a
        // distinct, searchable process definition, e.g. so `version` filters and
        // by-key lookups resolve superseded versions). `is_latest` marks the
        // highest version per id for callers that want only the current one.
        let mut stmt = conn
            .prepare(
                "SELECT key, process_id, version, name, \
                 (version = MAX(version) OVER (PARTITION BY process_id)) AS is_latest \
                 FROM process_definitions",
            )
            .expect("prepare process_definitions");
        let rows = stmt
            .query_map([], |r| {
                Ok(ProcessDefinitionRow {
                    key: r.get::<_, i64>(0)? as Key,
                    process_id: r.get(1)?,
                    version: r.get(2)?,
                    name: r.get(3)?,
                    is_latest: r.get::<_, i64>(4)? != 0,
                })
            })
            .expect("query process_definitions");
        rows.filter_map(Result::ok).collect()
    }

    /// Fetches a single process definition by its `processDefinitionKey`,
    /// resolving any version (not just the latest), or `None` if no such key was
    /// ever deployed. Backs the get-by-key endpoint.
    pub fn process_definition_by_key(&self, key: Key) -> Option<ProcessDefinitionRow> {
        let conn = self.conn.lock().expect("read store poisoned");
        let mut stmt = conn
            .prepare(
                "SELECT key, process_id, version, name, \
                 (version = (SELECT MAX(version) FROM process_definitions \
                             WHERE process_id = pd.process_id)) AS is_latest \
                 FROM process_definitions pd WHERE key = ?1",
            )
            .expect("prepare process_definition_by_key");
        stmt.query_row([key as i64], |r| {
            Ok(ProcessDefinitionRow {
                key: r.get::<_, i64>(0)? as Key,
                process_id: r.get(1)?,
                version: r.get(2)?,
                name: r.get(3)?,
                is_latest: r.get::<_, i64>(4)? != 0,
            })
        })
        .optional()
        .expect("query process_definition_by_key")
    }

    pub fn decision_requirements(&self) -> Vec<DecisionRequirementsRow> {
        let conn = self.conn.lock().expect("read store poisoned");
        let mut stmt = conn
            .prepare(&format!(
                "SELECT {DECISION_REQUIREMENTS_COLS} FROM decision_requirements"
            ))
            .expect("prepare decision_requirements");
        let rows = stmt
            .query_map([], map_decision_requirements)
            .expect("query decision_requirements");
        rows.filter_map(Result::ok).collect()
    }

    /// A single decision-requirements graph by its numeric key.
    pub fn decision_requirements_by_key(&self, key: Key) -> Option<DecisionRequirementsRow> {
        let conn = self.conn.lock().expect("read store poisoned");
        conn.query_row(
            &format!(
                "SELECT {DECISION_REQUIREMENTS_COLS} FROM decision_requirements WHERE drg_key = ?1"
            ),
            params![key as i64],
            map_decision_requirements,
        )
        .optional()
        .expect("query decision_requirements_by_key")
    }

    /// The verbatim DMN XML for the DRG with `key`, or `None` when no such graph
    /// is projected. Empty-string XML (a graph built programmatically rather than
    /// parsed) is returned as `Some("")`.
    pub fn decision_requirements_xml(&self, key: Key) -> Option<String> {
        let conn = self.conn.lock().expect("read store poisoned");
        conn.query_row(
            "SELECT xml FROM decision_requirements WHERE drg_key = ?1",
            params![key as i64],
            |r| r.get::<_, String>(0),
        )
        .optional()
        .expect("query decision_requirements_xml")
    }

    pub fn decision_definitions(&self) -> Vec<DecisionDefinitionRow> {
        let conn = self.conn.lock().expect("read store poisoned");
        let mut stmt = conn
            .prepare(&format!(
                "SELECT {DECISION_DEFINITION_COLS} FROM decision_definitions"
            ))
            .expect("prepare decision_definitions");
        let rows = stmt
            .query_map([], map_decision_definition)
            .expect("query decision_definitions");
        rows.filter_map(Result::ok).collect()
    }

    /// A single decision definition by its numeric key.
    pub fn decision_definition_by_key(&self, key: Key) -> Option<DecisionDefinitionRow> {
        let conn = self.conn.lock().expect("read store poisoned");
        conn.query_row(
            &format!("SELECT {DECISION_DEFINITION_COLS} FROM decision_definitions WHERE decision_key = ?1"),
            params![key as i64],
            map_decision_definition,
        )
        .optional()
        .expect("query decision_definition_by_key")
    }

    /// The DMN XML of the DRG owning the decision definition with `key`, or `None`
    /// when no such decision is projected.
    pub fn decision_definition_xml(&self, key: Key) -> Option<String> {
        let conn = self.conn.lock().expect("read store poisoned");
        conn.query_row(
            "SELECT r.xml FROM decision_definitions d \
             JOIN decision_requirements r ON r.drg_key = d.decision_requirements_key \
             WHERE d.decision_key = ?1",
            params![key as i64],
            |r| r.get::<_, String>(0),
        )
        .optional()
        .expect("query decision_definition_xml")
    }

    /// A single deployed form by its per-version numeric key. Each deployed form
    /// version is retained under its own `form_key`, so a redeploy that mints a
    /// new key never invalidates an earlier one. `None` when no such form is
    /// projected.
    pub fn form_by_key(&self, key: Key) -> Option<FormRow> {
        let conn = self.conn.lock().expect("read store poisoned");
        conn.query_row(
            &format!("SELECT {FORM_COLS} FROM forms WHERE form_key = ?1"),
            params![key as i64],
            map_form,
        )
        .optional()
        .expect("query form_by_key")
    }

    /// The latest deployed form for a given form id (highest version), used to
    /// resolve a process start form (`GetStartProcessForm`) by its declared
    /// `formId`. `None` when no form with that id is projected.
    pub fn form_by_id(&self, form_id: &str) -> Option<FormRow> {
        let conn = self.conn.lock().expect("read store poisoned");
        conn.query_row(
            &format!(
                "SELECT {FORM_COLS} FROM forms WHERE form_id = ?1 \
                 ORDER BY version DESC LIMIT 1"
            ),
            params![form_id],
            map_form,
        )
        .optional()
        .expect("query form_by_id")
    }

    /// A single deployed generic resource by its per-version numeric key. Each
    /// deployed version is retained under its own `resource_key`, so a redeploy
    /// that mints a new key never invalidates an earlier one. `None` when no such
    /// resource is projected.
    pub fn resource_by_key(&self, key: Key) -> Option<ResourceRow> {
        let conn = self.conn.lock().expect("read store poisoned");
        conn.query_row(
            &format!("SELECT {RESOURCE_COLS} FROM resources WHERE resource_key = ?1"),
            params![key as i64],
            map_resource,
        )
        .optional()
        .expect("query resource_by_key")
    }

    /// Metadata (no `content`) for a single generic resource by key, for the
    /// by-key metadata endpoint (`getResource`), which returns no content.
    pub fn resource_by_key_meta(&self, key: Key) -> Option<ResourceMetaRow> {
        let conn = self.conn.lock().expect("read store poisoned");
        conn.query_row(
            &format!("SELECT {RESOURCE_META_COLS} FROM resources WHERE resource_key = ?1"),
            params![key as i64],
            map_resource_meta,
        )
        .optional()
        .expect("query resource_by_key_meta")
    }

    /// Metadata (no `content`) for every projected generic-resource row, for the
    /// list/search path. Omits the `content` blob so a search over many/large
    /// resources does not read every version's full body. Callers apply search
    /// filters / sort / pagination in the gateway.
    pub fn resources_meta(&self) -> Vec<ResourceMetaRow> {
        let conn = self.conn.lock().expect("read store poisoned");
        let mut stmt = conn
            .prepare(&format!(
                "SELECT {RESOURCE_META_COLS} FROM resources ORDER BY resource_key"
            ))
            .expect("prepare resources_meta");
        stmt.query_map([], map_resource_meta)
            .expect("query resources_meta")
            .filter_map(Result::ok)
            .collect()
    }

    /// when no such definition is projected (only the latest version per process
    /// id is retained, mirroring the engine). Empty-string XML (a definition
    /// built programmatically rather than parsed) is returned as `Some("")`.
    pub fn process_definition_xml(&self, key: Key) -> Option<String> {
        let conn = self.conn.lock().expect("read store poisoned");
        conn.query_row(
            "SELECT xml FROM process_definitions WHERE key = ?1",
            params![key as i64],
            |r| r.get::<_, String>(0),
        )
        .optional()
        .expect("query process_definition_xml")
    }

    /// The `zeebe:formDefinition formId` declared on a process definition's start
    /// event (its start form), by process-definition key. The outer `Option` is
    /// `None` when no such definition is projected; the inner is `None` when the
    /// definition exists but declares no start form.
    pub fn process_definition_start_form_id(&self, key: Key) -> Option<Option<String>> {
        let conn = self.conn.lock().expect("read store poisoned");
        conn.query_row(
            "SELECT start_form_id FROM process_definitions WHERE key = ?1",
            params![key as i64],
            |r| r.get::<_, Option<String>>(0),
        )
        .optional()
        .expect("query process_definition_start_form_id")
    }

    pub fn variables(&self) -> Vec<VariableRow> {
        let conn = self.conn.lock().expect("read store poisoned");
        let mut stmt = conn
            .prepare(
                "SELECT key, instance_key, scope_key, name, value, \
                 process_definition_id, process_definition_key FROM variables",
            )
            .expect("prepare variables");
        let rows = stmt.query_map([], map_variable).expect("query variables");
        rows.filter_map(Result::ok).collect()
    }

    pub fn variable(&self, key: Key) -> Option<VariableRow> {
        let conn = self.conn.lock().expect("read store poisoned");
        conn.query_row(
            "SELECT key, instance_key, scope_key, name, value, \
             process_definition_id, process_definition_key FROM variables WHERE key = ?1",
            params![key as i64],
            map_variable,
        )
        .optional()
        .expect("query variable")
    }

    /// Returns every variable belonging to a process instance, ordered by name
    /// for deterministic results. Used to assemble the variable payload returned
    /// by an `awaitCompletion` create request.
    pub fn instance_variables(&self, instance_key: Key) -> Vec<VariableRow> {
        let conn = self.conn.lock().expect("read store poisoned");
        let mut stmt = conn
            .prepare(
                "SELECT key, instance_key, scope_key, name, value, \
                 process_definition_id, process_definition_key FROM variables \
                 WHERE instance_key = ?1 ORDER BY name",
            )
            .expect("prepare instance variables");
        let rows = stmt
            .query_map(params![instance_key as i64], map_variable)
            .expect("query instance variables");
        rows.filter_map(Result::ok).collect()
    }
}

fn map_instance(r: &rusqlite::Row) -> rusqlite::Result<ProcessInstanceRow> {
    let tags_str: String = r.get(8)?;
    let tags = if tags_str.is_empty() {
        Vec::new()
    } else {
        tags_str.split(',').map(|s| s.to_string()).collect()
    };
    let mut row = ProcessInstanceRow {
        key: r.get::<_, i64>(0)? as Key,
        process_id: r.get(1)?,
        process_definition_id: r.get(2)?,
        process_definition_key: r.get(3)?,
        version: r.get(4)?,
        state: instance_state_from(r.get(5)?),
        start_date_ms: r.get::<_, i64>(6)? as u64,
        has_incident: r.get::<_, i64>(7)? != 0,
        tags,
        business_id: r.get(9)?,
        parent_process_instance_key: r.get::<_, Option<i64>>(10)?.map(|k| k as Key),
        parent_element_instance_key: r.get::<_, Option<i64>>(11)?.map(|k| k as Key),
        suspended_date_ms: r.get::<_, Option<i64>>(12)?.map(|k| k as u64),
    };
    // Single source of truth: an in-flight instance carrying a suspension
    // timestamp is `Suspended`. A terminal base state (Completed/Terminated/
    // Terminating) always wins — a suspension record never resurrects it.
    if row.suspended_date_ms.is_some() && row.state == ProcessInstanceState::Active {
        row.state = ProcessInstanceState::Suspended;
    }
    Ok(row)
}

fn map_job(r: &rusqlite::Row) -> rusqlite::Result<JobRow> {
    Ok(JobRow {
        key: r.get::<_, i64>(0)? as Key,
        instance_key: r.get::<_, i64>(1)? as Key,
        element_instance_key: r.get::<_, i64>(2)? as Key,
        element_id: r.get(3)?,
        job_type: r.get(4)?,
        state: job_state_from(r.get(5)?),
        retries: r.get(6)?,
        worker: r.get(7)?,
        deadline_ms: r.get::<_, Option<i64>>(8)?.map(|v| v as u64),
        process_definition_id: r.get(9)?,
        process_definition_key: r.get(10)?,
        kind: job_kind_from(r.get(11)?, r.get(12)?),
        // Clamp before the `u64` cast so a negative persisted timestamp (DB
        // corruption / manual edits / a bad migration) can never wrap to a huge
        // `u64` and skew `/v2/jobs/statistics/*`. Mirrors `map_message_subscription`
        // — one canonical convention for every `created_at_ms` mapper.
        created_at_ms: r.get::<_, i64>(13)?.max(0) as u64,
        // Stored as a JSON array (mirrors `candidate_groups`); a malformed value
        // degrades to an empty read-set rather than failing the whole row map.
        read_set: serde_json::from_str::<Vec<String>>(&r.get::<_, String>(14)?).unwrap_or_default(),
        lease_token: r.get(15)?,
        business_id: r.get(16)?,
        error_message: r.get(17)?,
        error_code: r.get(18)?,
        has_failed_with_retries_left: r.get::<_, i64>(19)? != 0,
        last_update_ms: r.get::<_, Option<i64>>(20)?.map(|v| v.max(0) as u64),
        end_ms: r.get::<_, Option<i64>>(21)?.map(|v| v.max(0) as u64),
    })
}

/// Column list for `user_tasks` selects, shared by scan and point lookup.
const USER_TASK_COLS: &str = "key, instance_key, element_instance_key, element_id, state, \
     assignee, candidate_groups, candidate_users, due_date, follow_up_date, \
     priority, created_at_ms, process_definition_id, process_definition_key, \
     process_definition_version, form_key, external_form_reference, business_id";

fn map_user_task(r: &rusqlite::Row) -> rusqlite::Result<UserTaskRow> {
    let candidate_groups: String = r.get(6)?;
    let candidate_users: String = r.get(7)?;
    Ok(UserTaskRow {
        key: r.get::<_, i64>(0)? as Key,
        instance_key: r.get::<_, i64>(1)? as Key,
        element_instance_key: r.get::<_, i64>(2)? as Key,
        element_id: r.get(3)?,
        state: user_task_state_from(r.get(4)?),
        assignee: r.get(5)?,
        candidate_groups: serde_json::from_str(&candidate_groups).unwrap_or_default(),
        candidate_users: serde_json::from_str(&candidate_users).unwrap_or_default(),
        due_date: r.get(8)?,
        follow_up_date: r.get(9)?,
        priority: r.get(10)?,
        created_at_ms: r.get::<_, i64>(11)?.max(0) as u64,
        process_definition_id: r.get(12)?,
        process_definition_key: r.get(13)?,
        process_definition_version: r.get(14)?,
        form_key: r.get::<_, Option<i64>>(15)?.map(|k| k as Key),
        external_form_reference: r.get(16)?,
        business_id: r.get(17)?,
    })
}

fn map_incident(r: &rusqlite::Row) -> rusqlite::Result<IncidentRow> {
    Ok(IncidentRow {
        key: r.get::<_, i64>(0)? as Key,
        instance_key: r.get::<_, i64>(1)? as Key,
        element_instance_key: r.get::<_, i64>(2)? as Key,
        element_id: r.get(3)?,
        kind: incident_kind_from(r.get(4)?),
        state: incident_state_from(r.get(5)?),
        reason: r.get(6)?,
        job_key: r.get::<_, Option<i64>>(7)?.map(|v| v as Key),
        created_at_ms: r.get::<_, i64>(8)?.max(0) as u64,
        process_definition_id: r.get(9)?,
        process_definition_key: r.get(10)?,
    })
}

fn map_variable(r: &rusqlite::Row) -> rusqlite::Result<VariableRow> {
    Ok(VariableRow {
        key: r.get::<_, i64>(0)? as Key,
        instance_key: r.get::<_, i64>(1)? as Key,
        scope_key: r.get::<_, i64>(2)? as Key,
        name: r.get(3)?,
        value: r.get(4)?,
        process_definition_id: r.get(5)?,
        process_definition_key: r.get(6)?,
    })
}

/// Column list for `element_instances` selects, shared by scan and point lookup.
const ELEMENT_INSTANCE_COLS: &str = "element_instance_key, instance_key, process_definition_id, \
     process_definition_key, element_id, element_name, element_type, state, start_date_ms, \
     end_date_ms, scope_key, incident_key, has_incident, tenant_id";

fn map_element_instance(r: &rusqlite::Row) -> rusqlite::Result<ElementInstanceRow> {
    Ok(ElementInstanceRow {
        element_instance_key: r.get::<_, i64>(0)? as Key,
        instance_key: r.get::<_, i64>(1)? as Key,
        process_definition_id: r.get(2)?,
        process_definition_key: r.get(3)?,
        element_id: r.get(4)?,
        element_name: r.get(5)?,
        element_type: r.get(6)?,
        state: element_instance_state_from(r.get(7)?),
        start_date_ms: r.get::<_, i64>(8)? as u64,
        end_date_ms: r.get::<_, Option<i64>>(9)?.map(|v| v as u64),
        scope_key: r.get::<_, i64>(10)? as Key,
        incident_key: r.get::<_, Option<i64>>(11)?.map(|v| v as Key),
        has_incident: r.get::<_, i64>(12)? != 0,
        tenant_id: r.get(13)?,
    })
}

/// Column list for `message_subscriptions` selects.
const MESSAGE_SUBSCRIPTION_COLS: &str = "subscription_key, instance_key, element_instance_key, \
     element_id, message_name, correlation_key, created_at_ms, business_id";

fn map_message_subscription(r: &rusqlite::Row) -> rusqlite::Result<MessageSubscriptionRow> {
    Ok(MessageSubscriptionRow {
        subscription_key: r.get::<_, i64>(0)? as Key,
        instance_key: r.get::<_, i64>(1)? as Key,
        element_instance_key: r.get::<_, i64>(2)? as Key,
        element_id: r.get(3)?,
        message_name: r.get(4)?,
        correlation_key: r.get(5)?,
        created_at_ms: r.get::<_, i64>(6)?.max(0) as u64,
        business_id: r.get(7)?,
    })
}

/// Column list for `correlated_message_subscriptions` selects.
const CORRELATED_MESSAGE_SUBSCRIPTION_COLS: &str = "message_key, subscription_key, instance_key, element_instance_key, element_id, \
     message_name, correlation_key, correlation_time_ms, partition_id, business_id";

fn map_correlated_message_subscription(
    r: &rusqlite::Row,
) -> rusqlite::Result<CorrelatedMessageSubscriptionRow> {
    Ok(CorrelatedMessageSubscriptionRow {
        message_key: r.get::<_, i64>(0)? as Key,
        subscription_key: r.get::<_, i64>(1)? as Key,
        instance_key: r.get::<_, i64>(2)? as Key,
        element_instance_key: r.get::<_, i64>(3)? as Key,
        element_id: r.get(4)?,
        message_name: r.get(5)?,
        correlation_key: r.get(6)?,
        correlation_time_ms: r.get::<_, i64>(7)? as u64,
        partition_id: r.get::<_, i64>(8)? as i32,
        business_id: r.get(9)?,
    })
}

/// Column list for `decision_instances` selects, shared by scan and point lookup.
const DECISION_INSTANCE_COLS: &str = "eval_instance_key, decision_evaluation_key, idx, decision_id, \
     decision_key, decision_name, decision_type, version, decision_requirements_id, \
     decision_requirements_key, root_decision_key, instance_key, element_instance_key, \
     process_definition_key, state, evaluation_failure, evaluation_date_ms, result_json, \
     inputs_json, rules_json, tenant_id, business_id";

fn map_decision_instance(r: &rusqlite::Row) -> rusqlite::Result<DecisionInstanceRow> {
    Ok(DecisionInstanceRow {
        eval_instance_key: r.get(0)?,
        decision_evaluation_key: r.get::<_, i64>(1)? as Key,
        idx: r.get(2)?,
        decision_id: r.get(3)?,
        decision_key: r.get::<_, i64>(4)? as Key,
        decision_name: r.get(5)?,
        decision_type: r.get(6)?,
        version: r.get(7)?,
        decision_requirements_id: r.get(8)?,
        decision_requirements_key: r.get::<_, i64>(9)? as Key,
        root_decision_key: r.get::<_, i64>(10)? as Key,
        instance_key: r.get::<_, i64>(11)? as Key,
        element_instance_key: r.get::<_, i64>(12)? as Key,
        process_definition_key: r.get(13)?,
        state: r.get(14)?,
        evaluation_failure: r.get(15)?,
        evaluation_date_ms: r.get::<_, i64>(16)? as u64,
        result_json: r.get(17)?,
        inputs_json: r.get(18)?,
        rules_json: r.get(19)?,
        tenant_id: r.get(20)?,
        business_id: r.get(21)?,
    })
}

const DECISION_REQUIREMENTS_COLS: &str = "drg_id, drg_key, name, version, resource_name, xml";

fn map_decision_requirements(r: &rusqlite::Row) -> rusqlite::Result<DecisionRequirementsRow> {
    Ok(DecisionRequirementsRow {
        drg_id: r.get(0)?,
        drg_key: r.get::<_, i64>(1)? as Key,
        name: r.get(2)?,
        version: r.get(3)?,
        resource_name: r.get(4)?,
        xml: r.get(5)?,
    })
}

const DECISION_DEFINITION_COLS: &str = "decision_id, decision_key, name, version, \
     decision_requirements_key, decision_requirements_id, decision_requirements_name, \
     decision_requirements_version";

fn map_decision_definition(r: &rusqlite::Row) -> rusqlite::Result<DecisionDefinitionRow> {
    Ok(DecisionDefinitionRow {
        decision_id: r.get(0)?,
        decision_key: r.get::<_, i64>(1)? as Key,
        name: r.get(2)?,
        version: r.get(3)?,
        decision_requirements_key: r.get::<_, i64>(4)? as Key,
        decision_requirements_id: r.get(5)?,
        decision_requirements_name: r.get(6)?,
        decision_requirements_version: r.get(7)?,
    })
}

const FORM_COLS: &str = "form_id, form_key, version, schema, resource_name, tenant_id";

fn map_form(r: &rusqlite::Row) -> rusqlite::Result<FormRow> {
    Ok(FormRow {
        form_id: r.get(0)?,
        form_key: r.get::<_, i64>(1)? as Key,
        version: r.get(2)?,
        schema: r.get(3)?,
        resource_name: r.get(4)?,
        tenant_id: r.get(5)?,
    })
}

const RESOURCE_COLS: &str =
    "resource_key, resource_id, resource_name, version, version_tag, content, tenant_id";

fn map_resource(r: &rusqlite::Row) -> rusqlite::Result<ResourceRow> {
    Ok(ResourceRow {
        resource_key: r.get::<_, i64>(0)? as Key,
        resource_id: r.get(1)?,
        resource_name: r.get(2)?,
        version: r.get(3)?,
        version_tag: r.get(4)?,
        content: r.get(5)?,
        tenant_id: r.get(6)?,
    })
}

const RESOURCE_META_COLS: &str =
    "resource_key, resource_id, resource_name, version, version_tag, tenant_id";

fn map_resource_meta(r: &rusqlite::Row) -> rusqlite::Result<ResourceMetaRow> {
    Ok(ResourceMetaRow {
        resource_key: r.get::<_, i64>(0)? as Key,
        resource_id: r.get(1)?,
        resource_name: r.get(2)?,
        version: r.get(3)?,
        version_tag: r.get(4)?,
        tenant_id: r.get(5)?,
    })
}
/// Serializes an engine [`Value`] to the serialized-JSON string Camunda uses on
/// the wire: strings are JSON-quoted (so a string `myValue` becomes `"myValue"`),
/// numbers and booleans render bare, and lists/objects render as JSON.
fn json_value(value: &Value) -> String {
    crate::value_to_json(value).to_string()
}

/// The byte length beyond which a variable value is truncated in search results
/// (when `truncateValues` is on), and `isTruncated` is flagged. Single source of
/// truth for the gateway's `server` crate and the in-browser `engine-wasm`
/// `TestEngine`, both of which import this rather than redeclaring it, so the two
/// REST surfaces can never drift on the preview length. Mirrors the order of
/// magnitude of Camunda's variable value preview; nano's typical values are far
/// shorter, so it only fires for pathologically large payloads.
pub const VARIABLE_VALUE_PREVIEW_LEN: usize = 8192;

/// Converts an engine [`Value`] into a `serde_json::Value`. Shared by the read
/// model's projection (variable/DMN JSON encoding here) and the gateway's REST
/// result mapping in the `server` crate, which re-exports this as
/// `crate::value_to_json` so both sides encode identically (single source of
/// truth, no drift).
pub fn value_to_json(value: &Value) -> serde_json::Value {
    match value {
        Value::Null => serde_json::Value::Null,
        Value::Bool(b) => serde_json::Value::Bool(*b),
        Value::Int(i) => serde_json::Value::Number((*i).into()),
        Value::Double(d) => serde_json::Number::from_f64(*d)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        Value::Str(s) => serde_json::Value::String(s.clone()),
        Value::List(items) => serde_json::Value::Array(items.iter().map(value_to_json).collect()),
        Value::Map(entries) => serde_json::Value::Object(
            entries
                .iter()
                .map(|(k, v)| (k.clone(), value_to_json(v)))
                .collect(),
        ),
    }
}

/// Converts a `serde_json::Value` (the REST wire form) into an engine [`Value`].
/// The inverse of [`value_to_json`]. Shared by the read model and the gateway's
/// REST mapping (which re-exports it as `crate::json_to_value`) so the two sides
/// use a single, drift-free encoding.
pub fn json_to_value(json: &serde_json::Value) -> Value {
    match json {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(b) => Value::Bool(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::Int(i)
            } else {
                Value::number(n.as_f64().unwrap_or(0.0))
            }
        }
        serde_json::Value::String(s) => Value::Str(s.clone()),
        serde_json::Value::Array(items) => Value::List(items.iter().map(json_to_value).collect()),
        serde_json::Value::Object(entries) => Value::Map(
            entries
                .iter()
                .map(|(k, v)| (k.clone(), json_to_value(v)))
                .collect(),
        ),
    }
}

/// The Zeebe/Camunda REST name for a DMN decision logic type. Shared by the read
/// model's decision-instance projection and the gateway's REST mapping (which
/// re-exports it as `crate::dmn_decision_type_name`).
pub fn dmn_decision_type_name(kind: &nanobpmn_engine_core::dmn::DecisionType) -> &'static str {
    use nanobpmn_engine_core::dmn::DecisionType::*;
    match kind {
        DecisionTable => "DECISION_TABLE",
        LiteralExpression => "LITERAL_EXPRESSION",
        Context => "CONTEXT",
        Invocation => "INVOCATION",
        List => "LIST",
        Relation => "RELATION",
        Unknown => "UNKNOWN",
    }
}

/// The Camunda 8 element-instance `type` an element is exposed as in the read
/// model, derived from the deployed element as the single source of truth.
///
/// This wraps [`ElementKind::type_name`](nanobpmn_engine_core::ElementKind::type_name)
/// and overrides the cases the kind alone cannot disambiguate:
/// `CompensationThrowEvent` and `EscalationThrowEvent` each cover both the
/// `<intermediateThrowEvent>` and `<endEvent>` throw flavours (the engine infers
/// the end-event flavour purely from having no outgoing flow — runtime routing is
/// unaffected). `type_name()` hard-codes both to `INTERMEDIATE_THROW_EVENT`; a
/// terminal (no-outgoing-flow) throw is an end event, so classify it as
/// `END_EVENT` here (#917, #1173). Every other kind passes through unchanged.
fn element_type_name(element: &nanobpmn_engine_core::Element) -> &'static str {
    match element.kind {
        nanobpmn_engine_core::ElementKind::CompensationThrowEvent
        | nanobpmn_engine_core::ElementKind::EscalationThrowEvent { .. }
            if element.outgoing.is_empty() =>
        {
            "END_EVENT"
        }
        _ => element.kind.type_name(),
    }
}

/// Upserts a batch of variables into a single variable scope. For a root-scope
/// write (`VariablesUpdated`) the caller passes `scope_key == instance_key`; for
/// a nested scope (`ScopedVariablesUpdated` — a sub-process, multi-instance body
/// or child) it passes the scope-owning element instance key. Names are sorted
/// so the autoincrement variable keys are assigned deterministically on a rebuild
/// (a `VariablesUpdated`/`ProcessInstanceCreated` event carries an unordered map).
/// An already-known (scope, name) keeps its key and has its value overwritten.
/// Prepared-statement caching for the projection hot path. Plain
/// `Connection::execute` / `query_row` recompile the SQL text on every call; the
/// exporter runs one statement per projected event, so under load that
/// (re)parsing dominated a CPU profile (`sqlite3RunParser` / `sqlite3GetToken` /
/// `yy_reduce` were the exporter's top self-time symbols). Routing the hot
/// statements through `prepare_cached` compiles each SQL string once per
/// connection and reuses the cached plan, keeping only bytecode execution on the
/// per-event path. Semantics are identical — same SQL, same params.
trait CachedSql {
    fn cexecute<P: rusqlite::Params>(&self, sql: &str, params: P) -> rusqlite::Result<usize>;
    fn cquery_row<T, P, F>(&self, sql: &str, params: P, f: F) -> rusqlite::Result<T>
    where
        P: rusqlite::Params,
        F: FnOnce(&rusqlite::Row<'_>) -> rusqlite::Result<T>;
}

impl CachedSql for rusqlite::Connection {
    fn cexecute<P: rusqlite::Params>(&self, sql: &str, params: P) -> rusqlite::Result<usize> {
        self.prepare_cached(sql)?.execute(params)
    }

    fn cquery_row<T, P, F>(&self, sql: &str, params: P, f: F) -> rusqlite::Result<T>
    where
        P: rusqlite::Params,
        F: FnOnce(&rusqlite::Row<'_>) -> rusqlite::Result<T>,
    {
        self.prepare_cached(sql)?.query_row(params, f)
    }
}

fn upsert_variables(
    tx: &rusqlite::Transaction,
    instance_key: Key,
    scope_key: Key,
    variables: &std::collections::HashMap<String, Value>,
) -> rusqlite::Result<()> {
    if variables.is_empty() {
        return Ok(());
    }
    let (def_id, def_key) = instance_def(tx, instance_key);
    let mut entries: Vec<(&String, &Value)> = variables.iter().collect();
    entries.sort_by(|a, b| a.0.cmp(b.0));
    for (name, value) in entries {
        tx.cexecute(
            "INSERT INTO variables (instance_key, scope_key, name, value, \
             process_definition_id, process_definition_key) VALUES (?1, ?2, ?3, ?4, ?5, ?6) \
             ON CONFLICT(scope_key, name) DO UPDATE SET value = excluded.value",
            params![
                instance_key as i64,
                scope_key as i64,
                name,
                json_value(value),
                def_id,
                def_key
            ],
        )?;
    }
    Ok(())
}

/// The process-definition identity (`process_definition_id`,
/// `process_definition_key`) carried by an instance row, used to denormalize
/// jobs and incidents onto their owning definition. Defaults to empty values
/// when the instance is unknown (it always precedes its jobs/incidents in the
/// event order, so this is only a safety net).
fn instance_def(tx: &rusqlite::Transaction, instance_key: Key) -> (String, String) {
    tx.cquery_row(
        "SELECT process_definition_id, process_definition_key FROM process_instances WHERE key = ?1",
        params![instance_key as i64],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )
    .optional()
    .ok()
    .flatten()
    .unwrap_or_default()
}

/// Inserts (or refreshes) an ACTIVE element-instance row, resolving its owning
/// process definition and its `type`/`elementName` from `definition_elements`.
/// `scope` (from `ElementActivated`) is stamped when known; a re-delivery keeps
/// the row's terminal state (only `scope_key` is refreshed).
fn upsert_element_instance(
    tx: &rusqlite::Transaction,
    now_ms: u64,
    instance_key: Key,
    element_instance_key: Key,
    element_id: &str,
    scope: Option<Key>,
) -> rusqlite::Result<()> {
    let (def_id, def_key) = instance_def(tx, instance_key);
    let def_key_int: i64 = def_key.parse().unwrap_or(-1);
    // An ad-hoc sub-process's synthetic inner instance (`<container>#innerInstance`)
    // has no entry in the deployed model, so it is resolved by its id postfix
    // rather than a `definition_elements` lookup — matching Zeebe's
    // `AD_HOC_SUB_PROCESS_INNER_INSTANCE` element type. The postfix is owned by
    // engine-core (the single source of truth shared with the engine that mints
    // these instances).
    let (element_type, element_name): (String, Option<String>) =
        if element_id.ends_with(nanobpmn_engine_core::ADHOC_INNER_INSTANCE_ID_POSTFIX) {
            ("AD_HOC_SUB_PROCESS_INNER_INSTANCE".to_string(), None)
        } else {
            tx.cquery_row(
                "SELECT element_type, element_name FROM definition_elements \
                 WHERE process_definition_key = ?1 AND element_id = ?2",
                params![def_key_int, element_id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()
            .ok()
            .flatten()
            .unwrap_or_else(|| ("UNKNOWN".to_string(), None))
        };
    tx.cexecute(
        "INSERT INTO element_instances (element_instance_key, instance_key, process_definition_id, \
         process_definition_key, element_id, element_name, element_type, state, start_date_ms, \
         end_date_ms, scope_key, incident_key, has_incident) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, NULL, ?10, NULL, 0) \
         ON CONFLICT(element_instance_key) DO UPDATE SET \
         scope_key = CASE WHEN ?11 THEN excluded.scope_key ELSE element_instances.scope_key END",
        params![
            element_instance_key as i64,
            instance_key as i64,
            def_id,
            def_key,
            element_id,
            element_name,
            element_type,
            element_instance_state_code(ElementInstanceState::Active),
            now_ms as i64,
            scope.unwrap_or(0) as i64,
            scope.is_some(),
        ],
    )?;
    Ok(())
}

/// The deployed version of the definition behind an instance (defaults to 1
/// when the instance row is not yet present).
fn instance_version(tx: &rusqlite::Transaction, instance_key: Key) -> i32 {
    tx.cquery_row(
        "SELECT version FROM process_instances WHERE key = ?1",
        params![instance_key as i64],
        |r| r.get(0),
    )
    .optional()
    .ok()
    .flatten()
    .unwrap_or(1)
}

/// Projects the LIVE materialized engine [`State`] (from a boot snapshot) into an
/// empty read model, row-for-row matching what the event projector [`project`]
/// would have produced — the recovery path for a read model that has fallen below
/// the journal compaction floor (issue #732). The engine snapshot is the
/// authoritative capture the compaction invariant guarantees covers everything
/// below the floor, so this rebuilds every operationally-live entity losslessly.
///
/// Deliberately NOT reconstructable here (only ever lived in the read model,
/// already evicted from the engine): terminal-instance audit history and decision
/// evaluation history. Insertion order mirrors the event stream's causal order so
/// the denormalizing `instance_def`/`instance_version`/`definition_elements`
/// lookups resolve: definitions and decisions first, then instances (+ their
/// variables), then per-instance element instances / jobs / incidents / user
/// tasks / message subscriptions.
fn project_engine_state(
    tx: &rusqlite::Transaction,
    state: &nanobpmn_engine_core::State,
    now_ms: u64,
) -> rusqlite::Result<()> {
    // 1) Process definitions + their element metadata. `state.process_versions`
    //    retains EVERY deployed version keyed by process-definition key, while
    //    `state.processes` is only the latest-by-id index over it. Project the
    //    full version-retention map so superseded definitions survive a
    //    compaction-floor recovery — matching the read model's "every version is
    //    searchable / get-by-key resolves superseded versions" semantics. A
    //    pre-retention snapshot deserializes `process_versions` empty
    //    (`serde(default)`); fall back to the latest-by-id index there, where the
    //    latest version per id is the only version that was ever preserved.
    let deployed_defs: Vec<&nanobpmn_engine_core::DeployedProcess> =
        if state.process_versions.is_empty() {
            state.processes.values().collect()
        } else {
            state.process_versions.values().collect()
        };
    for deployed in deployed_defs {
        let def = &deployed.definition;
        tx.cexecute(
            "INSERT INTO process_definitions (process_id, key, version, name, xml, start_form_id) VALUES (?1, ?2, ?3, ?4, ?5, ?6) \
             ON CONFLICT(key) DO UPDATE SET process_id = excluded.process_id, version = excluded.version, name = excluded.name, xml = excluded.xml, start_form_id = excluded.start_form_id",
            params![
                def.id,
                deployed.key as i64,
                deployed.version,
                def.name.as_ref(),
                def.xml,
                def.start_form_id.as_ref()
            ],
        )?;
        for (element_id, element) in &def.elements {
            tx.cexecute(
                "INSERT INTO definition_elements (process_definition_key, element_id, element_type, element_name) \
                 VALUES (?1, ?2, ?3, ?4) \
                 ON CONFLICT(process_definition_key, element_id) DO UPDATE SET \
                 element_type = excluded.element_type, element_name = excluded.element_name",
                params![
                    deployed.key as i64,
                    element_id,
                    element_type_name(element),
                    element.name.as_ref(),
                ],
            )?;
        }
    }

    // 2) Decision requirements graphs (before decisions: the decision row's
    //    denormalized DRG identity is resolved from this table).
    for dep in state.decision_requirements.values() {
        tx.cexecute(
            "INSERT INTO decision_requirements (drg_id, drg_key, name, version, resource_name, xml) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6) \
             ON CONFLICT(drg_id) DO UPDATE SET drg_key = excluded.drg_key, \
             name = excluded.name, version = excluded.version, \
             resource_name = excluded.resource_name, xml = excluded.xml",
            // `resource_name` is not retained in engine state; default to '' as
            // the column does (it only backs a display field).
            params![dep.drg.id, dep.key as i64, dep.drg.name, dep.version, "", dep.drg.xml],
        )?;
    }

    // 3) Decision definitions.
    for dep in state.decisions.values() {
        let (drg_id, drg_name, drg_version): (String, String, i32) = tx
            .cquery_row(
                "SELECT drg_id, name, version FROM decision_requirements WHERE drg_key = ?1",
                params![dep.decision_requirements_key as i64],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .optional()?
            .unwrap_or_default();
        tx.cexecute(
            "INSERT INTO decision_definitions \
             (decision_id, decision_key, name, version, decision_requirements_key, \
              decision_requirements_id, decision_requirements_name, decision_requirements_version) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8) \
             ON CONFLICT(decision_id) DO UPDATE SET decision_key = excluded.decision_key, \
             name = excluded.name, version = excluded.version, \
             decision_requirements_key = excluded.decision_requirements_key, \
             decision_requirements_id = excluded.decision_requirements_id, \
             decision_requirements_name = excluded.decision_requirements_name, \
             decision_requirements_version = excluded.decision_requirements_version",
            params![
                dep.decision_id,
                dep.key as i64,
                dep.decision_name,
                dep.version,
                dep.decision_requirements_key as i64,
                drg_id,
                drg_name,
                drg_version,
            ],
        )?;
    }

    // 4) Forms.
    for dep in state.forms.values() {
        tx.cexecute(
            "INSERT INTO forms (form_key, form_id, version, schema, resource_name, tenant_id) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6) \
             ON CONFLICT(form_key) DO UPDATE SET form_id = excluded.form_id, \
             version = excluded.version, schema = excluded.schema, \
             resource_name = excluded.resource_name, tenant_id = excluded.tenant_id",
            // `tenant_id` is not modeled in engine state; default to '<default>'.
            params![
                dep.key as i64,
                dep.form_id,
                dep.version,
                dep.schema,
                dep.resource_name,
                "<default>"
            ],
        )?;
    }

    // 4b) Generic resources.
    for res in state.resources.values() {
        tx.cexecute(
            "INSERT INTO resources (resource_key, resource_id, resource_name, version, \
             version_tag, content, tenant_id) \
             VALUES (?1, ?2, ?3, ?4, NULL, ?5, ?6) \
             ON CONFLICT(resource_key) DO UPDATE SET resource_id = excluded.resource_id, \
             resource_name = excluded.resource_name, version = excluded.version, \
             version_tag = excluded.version_tag, content = excluded.content, \
             tenant_id = excluded.tenant_id",
            // `tenant_id` is not modeled in engine state; default to '<default>'.
            params![
                res.key as i64,
                res.resource_id,
                res.resource_name,
                res.version,
                res.content,
                "<default>"
            ],
        )?;
    }

    // 5) Process instances (+ their variables). Resolve the deployed identity the
    //    same way the create projection does (latest version on record).
    for inst in state.instances.values() {
        let (def_key, version): (String, i32) = tx
            .cquery_row(
                "SELECT key, version FROM process_definitions WHERE process_id = ?1 \
                 ORDER BY version DESC LIMIT 1",
                params![inst.process_id],
                |r| Ok((r.get::<_, i64>(0)?, r.get::<_, i32>(1)?)),
            )
            .optional()?
            .map(|(k, v)| (k.to_string(), v))
            .unwrap_or_else(|| ("-1".to_string(), 0));
        tx.cexecute(
            "INSERT INTO process_instances (key, process_id, process_definition_id, \
             process_definition_key, version, state, start_date_ms, has_incident, tags, business_id, \
             parent_process_instance_key, parent_element_instance_key, suspended_date_ms) \
             VALUES (?1, ?2, ?2, ?3, ?4, ?5, ?6, 0, ?7, ?8, ?9, ?10, ?11) \
             ON CONFLICT(key) DO NOTHING",
            params![
                inst.key as i64,
                inst.process_id,
                def_key,
                version,
                instance_state_code(inst.state),
                inst.created_at as i64,
                inst.tags.join(","),
                inst.business_id.as_ref(),
                inst.parent_process_instance_key.map(|k| k as i64),
                inst.parent_element_instance_key.map(|k| k as i64),
                inst.suspended_at.map(|t| t as i64),
            ],
        )?;
        // Process-level variables (scope == instance key), then each nested scope.
        // A spilled instance carries no variables in the snapshot (they live in
        // the authoritative var store); those are re-materialized on demand, not
        // here.
        upsert_variables(tx, inst.key, inst.key, inst.variables.as_ref())?;
        for (scope, vars) in &inst.scope_variables {
            upsert_variables(tx, inst.key, *scope, vars)?;
        }
        for agent in inst.agent_instances.values() {
            project_agent_instance(tx, agent)?;
        }
        for (agent_key, history) in &inst.agent_history {
            for record in history {
                project_agent_history_record(tx, record)?;
                if record.commit_status != AgentHistoryCommitStatus::Pending {
                    transition_agent_history(
                        tx,
                        *agent_key,
                        &[record.agent_history_key],
                        record.commit_status,
                    )?;
                }
            }
        }
    }

    // 6) Live element instances (all ACTIVE — the engine only tracks open tokens).
    for inst in state.instances.values() {
        for (eik, element_id) in &inst.active {
            upsert_element_instance(
                tx,
                now_ms,
                inst.key,
                *eik,
                element_id.as_str(),
                inst.scopes.get(eik).copied(),
            )?;
        }
    }

    // 7) Jobs (carry their current state/worker/deadline/kind directly).
    for job in state.jobs.values() {
        let (def_id, def_key) = instance_def(tx, job.instance_key);
        let (kind_code, event_code) = job_kind_codes(&job.kind);
        // Timing (#1344): a snapshotted job is still live (terminal jobs are
        // evicted from engine state), so `end_ms` stays NULL and `last_update_ms`
        // seeds from the creation instant — the best lower bound available without
        // the per-event history, matching `creationTime <= lastUpdateTime`. On a
        // CONFLICT we deliberately do NOT overwrite `created_at_ms` /
        // `last_update_ms` / `end_ms`: an existing read-model row already carries
        // the authoritative event-derived values, which a compaction-floor reseed
        // must not clobber. Seed `created_at_ms` with the SAME batch-observation
        // fallback as `last_update_ms` when the snapshot predates `created_at`
        // (legacy `0`), so a recovered legacy job surfaces a non-null
        // `creationTime` (== `lastUpdateTime`) rather than a null creation time.
        let last_update = if job.created_at != 0 {
            job.created_at
        } else {
            now_ms
        };
        let creation_ms = last_update;
        tx.cexecute(
            "INSERT INTO jobs (key, instance_key, element_instance_key, element_id, job_type, \
             state, retries, worker, deadline_ms, process_definition_id, process_definition_key, \
             job_kind, listener_event_type, lease_token, error_message, error_code, \
             has_failed_with_retries_left, created_at_ms, last_update_ms, business_id) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, \
             ?18, ?19, (SELECT business_id FROM process_instances WHERE key = ?2)) \
             ON CONFLICT(key) DO UPDATE SET state = excluded.state, retries = excluded.retries, \
             worker = excluded.worker, deadline_ms = excluded.deadline_ms, \
             lease_token = excluded.lease_token, error_message = excluded.error_message, \
             error_code = excluded.error_code, \
             has_failed_with_retries_left = excluded.has_failed_with_retries_left",
            params![
                job.key as i64,
                job.instance_key as i64,
                job.element_instance_key as i64,
                job.element_id,
                job.job_type,
                job_state_code(job.state),
                job.retries,
                // Normalize the seeded worker exactly as the event-driven
                // projection does: a pre-normalization snapshot can carry
                // `Some("")` for a job activated with an explicitly-empty worker,
                // which is not an attribution and must seed SQL `NULL`, not `""`.
                // Otherwise a later terminal `COALESCE(?, worker)` (whose own
                // capture is now `None`) would preserve the empty string and pin
                // the row to a bogus empty worker (#1191).
                worker_attribution(job.worker.as_deref()),
                job.deadline.map(|d| d as i64),
                def_id,
                def_key,
                kind_code,
                event_code,
                job.lease_token,
                // Zeebe job error metadata (#1327). Retained in engine state so a
                // compaction-floor rebuild re-seeds them here instead of losing
                // them to the compacted `JobFailed`/`JobErrorThrown` events.
                job.error_message,
                job.error_code,
                i64::from(job.has_failed_with_retries_left),
                creation_ms as i64,
                last_update as i64,
            ],
        )?;
    }

    // 8) Incidents (+ surface their flag on the owning instance/element while active).
    for inc in state.incidents.values() {
        let (def_id, def_key) = instance_def(tx, inc.instance_key);
        tx.cexecute(
            "INSERT INTO incidents (key, instance_key, element_instance_key, element_id, kind, \
             state, reason, job_key, created_at_ms, process_definition_id, process_definition_key) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11) \
             ON CONFLICT(key) DO UPDATE SET state = excluded.state",
            params![
                inc.key as i64,
                inc.instance_key as i64,
                inc.element_instance_key as i64,
                inc.element_id,
                incident_kind_code(inc.kind),
                incident_state_code(inc.state),
                inc.reason,
                inc.job_key.map(|k| k as i64),
                inc.created_at as i64,
                def_id,
                def_key,
            ],
        )?;
        if inc.state == IncidentState::Active {
            tx.cexecute(
                "UPDATE process_instances SET has_incident = 1 WHERE key = ?1",
                params![inc.instance_key as i64],
            )?;
            tx.cexecute(
                "UPDATE element_instances SET has_incident = 1, incident_key = ?2 \
                 WHERE element_instance_key = ?1",
                params![inc.element_instance_key as i64, inc.key as i64],
            )?;
        }
    }

    // 9) User tasks.
    for ut in state.user_tasks.values() {
        let (def_id, def_key) = instance_def(tx, ut.instance_key);
        let version = instance_version(tx, ut.instance_key);
        let groups = serde_json::to_string(&ut.candidate_groups).unwrap_or_else(|_| "[]".into());
        let users = serde_json::to_string(&ut.candidate_users).unwrap_or_else(|_| "[]".into());
        tx.cexecute(
            "INSERT INTO user_tasks (key, instance_key, element_instance_key, element_id, \
             state, assignee, candidate_groups, candidate_users, due_date, follow_up_date, \
             priority, created_at_ms, process_definition_id, process_definition_key, \
             process_definition_version, form_key, external_form_reference, business_id) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, \
             (SELECT business_id FROM process_instances WHERE key = ?2)) \
             ON CONFLICT(key) DO UPDATE SET state = excluded.state",
            params![
                ut.key as i64,
                ut.instance_key as i64,
                ut.element_instance_key as i64,
                ut.element_id,
                user_task_state_code(ut.state),
                ut.assignee.as_ref(),
                groups,
                users,
                ut.due_date.as_ref(),
                ut.follow_up_date.as_ref(),
                ut.priority,
                ut.created_at as i64,
                def_id,
                def_key,
                version,
                ut.form_key.map(|k| k as i64),
                ut.external_form_reference.as_ref(),
            ],
        )?;
    }

    // 10) Open message subscriptions (waiting states). A settled subscription
    //     (Correlated/Canceled) is retained in engine state as an audit trail but
    //     is not a live wait, so the event projector deletes its read-model row;
    //     mirror that by projecting only the open ones.
    for sub in state.message_subscriptions.values() {
        let open = matches!(
            sub.state,
            nanobpmn_engine_core::MessageSubscriptionState::Open
                | nanobpmn_engine_core::MessageSubscriptionState::Opening
        );
        if open && sub.element_instance_key != 0 {
            let non_interrupting = matches!(
                sub.kind,
                nanobpmn_engine_core::MessageSubscriptionKind::NonInterruptingBoundary { .. }
            );
            tx.cexecute(
                "INSERT INTO message_subscriptions (subscription_key, instance_key, \
                 element_instance_key, element_id, message_name, correlation_key, created_at_ms, \
                 non_interrupting, business_id) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, (SELECT business_id FROM process_instances WHERE key = ?2)) \
                 ON CONFLICT(subscription_key) DO UPDATE SET \
                 element_instance_key = excluded.element_instance_key, \
                 element_id = excluded.element_id, \
                 message_name = excluded.message_name, \
                 correlation_key = excluded.correlation_key, \
                 non_interrupting = excluded.non_interrupting",
                params![
                    sub.key as i64,
                    sub.instance_key as i64,
                    sub.element_instance_key as i64,
                    sub.element_id,
                    sub.message_name,
                    sub.correlation_key,
                    now_ms as i64,
                    non_interrupting as i64,
                ],
            )?;
        }
    }

    project_event_waits_from_state(tx, state)
}

/// Projects every open timer / signal / conditional wait held by the live engine
/// `state` into `event_waits` — row-for-row what [`project`] produces from the
/// `*Created` events. Shared by the #732 reseed and the schema-v9 upgrade
/// backfill ([`ReadStore::backfill_pending_event_waits`]); idempotent.
fn project_event_waits_from_state(
    tx: &rusqlite::Transaction,
    state: &nanobpmn_engine_core::State,
) -> rusqlite::Result<()> {
    use nanobpmn_engine_core::{MessageSubscriptionState, TimerKind, TimerState};
    let open = |s: MessageSubscriptionState| {
        matches!(
            s,
            MessageSubscriptionState::Open | MessageSubscriptionState::Opening
        )
    };
    for t in state.timers.values() {
        if t.state == TimerState::Created {
            insert_event_wait(
                tx,
                t.key,
                EventWaitType::Timer,
                t.instance_key,
                t.element_instance_key,
                &t.element_id,
                "",
                Some(t.due_at),
                matches!(t.kind, TimerKind::NonInterruptingBoundary { .. }),
            )?;
        }
    }
    for sub in state.signal_subscriptions.values() {
        if open(sub.state) {
            insert_event_wait(
                tx,
                sub.key,
                EventWaitType::Signal,
                sub.instance_key,
                sub.element_instance_key,
                &sub.element_id,
                &sub.signal_name,
                None,
                is_non_interrupting(&sub.kind),
            )?;
        }
    }
    for sub in state.conditional_subscriptions.values() {
        if open(sub.state) {
            insert_event_wait(
                tx,
                sub.key,
                EventWaitType::Condition,
                sub.instance_key,
                sub.element_instance_key,
                &sub.element_id,
                &sub.condition,
                None,
                is_non_interrupting(&sub.kind),
            )?;
        }
    }
    Ok(())
}

/// An *empty* worker string is not an attribution: a job activated with an
/// explicitly-supplied `""` (e.g. forwarded verbatim by the REST activation
/// handler) carries no worker, so a terminal row must project it to SQL `NULL`
/// — leaving any pre-existing attribution untouched under `COALESCE(?, worker)`
/// — rather than overwriting the row with an empty attribution. The engine's
/// `JobActivated` reducer normalizes this at the source for freshly-emitted
/// events; this guard keeps the projection correct when *replaying* any event
/// persisted before that normalization existed, and when *seeding* from a
/// pre-normalization snapshot in `project_engine_state`.
fn is_non_interrupting(kind: &nanobpmn_engine_core::MessageSubscriptionKind) -> bool {
    matches!(
        kind,
        nanobpmn_engine_core::MessageSubscriptionKind::NonInterruptingBoundary { .. }
    )
}

/// Upserts one open timer / signal / conditional wait row — the single insert
/// shared by the event projector and the engine-state seed/backfill.
#[allow(clippy::too_many_arguments)]
fn insert_event_wait(
    tx: &rusqlite::Transaction,
    wait_key: Key,
    wait_type: EventWaitType,
    instance_key: Key,
    element_instance_key: Key,
    element_id: &str,
    detail: &str,
    due_at_ms: Option<u64>,
    non_interrupting: bool,
) -> rusqlite::Result<()> {
    tx.cexecute(
        "INSERT INTO event_waits (wait_key, wait_type, instance_key, element_instance_key, \
         element_id, detail, due_at_ms, non_interrupting) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8) \
         ON CONFLICT(wait_key) DO NOTHING",
        params![
            wait_key as i64,
            wait_type.as_str(),
            instance_key as i64,
            element_instance_key as i64,
            element_id,
            detail,
            due_at_ms.map(|d| d as i64),
            non_interrupting as i64,
        ],
    )?;
    Ok(())
}

fn worker_attribution(worker: Option<&str>) -> Option<&str> {
    worker.filter(|w| !w.is_empty())
}

/// Applies a single event to the read model and returns its exact contribution
/// to the in-flight instance gauge: `+1` when it genuinely creates a new active
/// instance, `-1` when it genuinely transitions an active instance to terminal,
/// and `0` otherwise — crucially including idempotent re-deliveries, which must
/// not move the gauge (they were the source of the historical `active_backlog`
/// drift where re-delivered creates permanently inflated the counter).
///
/// Only events that surface in a `search*`/`get*` projection are materialized;
/// the rest (element lifecycle, sequence flows, timers, message subscriptions,
/// start subscriptions) carry no queryable read-model state and are ignored.
fn project(tx: &rusqlite::Transaction, event: &Event, now_ms: u64) -> rusqlite::Result<i64> {
    let mut delta: i64 = 0;
    match event {
        Event::ProcessDeployed {
            process_definition_key,
            version,
            process,
            ..
        } => {
            // Retain EVERY deployed version, keyed by processDefinitionKey (one
            // row per version), so getProcessDefinitionXML / the console diagram
            // can serve any version's verbatim BPMN — including versions that a
            // later redeploy has superseded but whose instances are still around
            // (a redeploy no longer overwrites the prior version's XML, which
            // previously blanked the Explorer diagram of every older-version
            // instance). Search returns every retained version and marks the latest
            // per id with `is_latest`. `ON CONFLICT(key)` refreshes idempotently on
            // replay/re-delivery of the same ProcessDeployed event.
            tx.cexecute(
                "INSERT INTO process_definitions (process_id, key, version, name, xml, start_form_id) VALUES (?1, ?2, ?3, ?4, ?5, ?6) \
                 ON CONFLICT(key) DO UPDATE SET process_id = excluded.process_id, version = excluded.version, name = excluded.name, xml = excluded.xml, start_form_id = excluded.start_form_id",
                params![
                    process.id,
                    *process_definition_key as i64,
                    version,
                    process.name.as_ref(),
                    process.xml,
                    process.start_form_id.as_ref()
                ],
            )?;
            // Element metadata (type + BPMN name) keyed by (definition, element
            // id): the per-element lifecycle events carry only an element id, so
            // the element-instance read model resolves `type`/`elementName` here,
            // from the deployed model (the single source of truth).
            for (element_id, element) in &process.elements {
                tx.cexecute(
                    "INSERT INTO definition_elements (process_definition_key, element_id, element_type, element_name) \
                     VALUES (?1, ?2, ?3, ?4) \
                     ON CONFLICT(process_definition_key, element_id) DO UPDATE SET \
                     element_type = excluded.element_type, element_name = excluded.element_name",
                    params![
                        *process_definition_key as i64,
                        element_id,
                        element_type_name(element),
                        element.name.as_ref(),
                    ],
                )?;
            }
        }

        Event::ProcessInstanceCreated {
            instance_key,
            process_id,
            created_at,
            variables,
            tags,
            business_id,
            process_definition_key,
            version,
            parent_process_instance_key,
            parent_element_instance_key,
        } => {
            // Prefer the definition identity the event carries — it pins the
            // instance to the exact version it was created on (a by-key or
            // by-id+version create may target a non-latest version). Fall back to
            // the latest-deployed-so-far lookup for events written before version
            // pinning (`process_definition_key == 0`): during an ordered replay
            // only versions deployed before this create are on record, so the
            // highest version is the one the instance was created on.
            let (def_key, version): (String, i32) = if *process_definition_key != 0 {
                (process_definition_key.to_string(), *version)
            } else {
                tx.query_row(
                    "SELECT key, version FROM process_definitions WHERE process_id = ?1 \
                     ORDER BY version DESC LIMIT 1",
                    params![process_id],
                    |r| Ok((r.get::<_, i64>(0)?, r.get::<_, i32>(1)?)),
                )
                .optional()?
                .map(|(k, v)| (k.to_string(), v))
                .unwrap_or_else(|| ("-1".to_string(), 0))
            };
            // Serialize tags as comma-separated string for storage
            let tags_str = tags.join(",");
            // `DO NOTHING` (not `DO UPDATE`): a create is the first event for a
            // key, so the only conflict is an idempotent re-delivery carrying
            // identical fields — refreshing them would be a no-op. `DO NOTHING`
            // lets the row count distinguish a genuine new instance (1 row) from
            // a re-delivery (0 rows), which is what keeps the in-flight gauge
            // exact under replay.
            let inserted = tx.cexecute(
                "INSERT INTO process_instances (key, process_id, process_definition_id, \
                 process_definition_key, version, state, start_date_ms, has_incident, tags, business_id, \
                 parent_process_instance_key, parent_element_instance_key) \
                 VALUES (?1, ?2, ?2, ?3, ?4, ?5, ?6, 0, ?7, ?8, ?9, ?10) \
                 ON CONFLICT(key) DO NOTHING",
                params![
                    *instance_key as i64,
                    process_id,
                    def_key,
                    version,
                    instance_state_code(ProcessInstanceState::Active),
                    *created_at as i64,
                    tags_str,
                    business_id.as_ref(),
                    parent_process_instance_key.map(|k| k as i64),
                    parent_element_instance_key.map(|k| k as i64),
                ],
            )? == 1;
            if inserted {
                delta = 1;
                // Variables the instance was created with (the process-instance
                // row exists now, so the scope's denormalized definition
                // resolves). Skipped on re-delivery: the row already carries
                // them and the instance may since have been pruned/spilled, so
                // re-upserting could resurrect reclaimed variables.
                upsert_variables(tx, *instance_key, *instance_key, variables)?;
            }
        }

        Event::ProcessInstanceCompleted { instance_key } => {
            // `AND state = 0` (Active): only a genuine Active->terminal transition
            // updates a row (1 change => delta -1); a re-delivered completion
            // finds the row already terminal (0 changes) and must not move the
            // gauge.
            let transitioned = tx.cexecute(
                "UPDATE process_instances SET state = ?2 WHERE key = ?1 AND state = 0",
                params![
                    *instance_key as i64,
                    instance_state_code(ProcessInstanceState::Completed)
                ],
            )? == 1;
            if transitioned {
                delta = -1;
            }
            // A completed instance holds no open message subscriptions; drop any
            // so they stop surfacing as MESSAGE wait states.
            tx.cexecute(
                "DELETE FROM message_subscriptions WHERE instance_key = ?1",
                params![*instance_key as i64],
            )?;
            tx.cexecute(
                "DELETE FROM event_waits WHERE instance_key = ?1",
                params![*instance_key as i64],
            )?;
        }

        Event::ProcessInstanceTerminated { instance_key } => {
            // `AND state = 0`: count a genuine Active->Terminated transition only
            // (see ProcessInstanceCompleted).
            let transitioned = tx.cexecute(
                "UPDATE process_instances SET state = ?2, has_incident = 0 WHERE key = ?1 AND state = 0",
                params![
                    *instance_key as i64,
                    instance_state_code(ProcessInstanceState::Terminated)
                ],
            )? == 1;
            if transitioned {
                delta = -1;
            }
            // Close any incident still active on the terminated instance, so it
            // no longer surfaces as open in incident search.
            tx.cexecute(
                "UPDATE incidents SET state = ?2 WHERE instance_key = ?1 AND state = ?3",
                params![
                    *instance_key as i64,
                    incident_state_code(IncidentState::Resolved),
                    incident_state_code(IncidentState::Active),
                ],
            )?;
            // Every element instance still ACTIVE when the process is terminated
            // transitions to TERMINATED (the engine emits no per-element terminate
            // event — termination is a process-scope event).
            tx.cexecute(
                "UPDATE element_instances SET state = ?2, end_date_ms = ?3, has_incident = 0, \
                 incident_key = NULL WHERE instance_key = ?1 AND state = ?4",
                params![
                    *instance_key as i64,
                    element_instance_state_code(ElementInstanceState::Terminated),
                    now_ms as i64,
                    element_instance_state_code(ElementInstanceState::Active),
                ],
            )?;
            // A terminated instance holds no open message subscriptions.
            tx.cexecute(
                "DELETE FROM message_subscriptions WHERE instance_key = ?1",
                params![*instance_key as i64],
            )?;
            tx.cexecute(
                "DELETE FROM event_waits WHERE instance_key = ?1",
                params![*instance_key as i64],
            )?;
        }

        Event::ProcessInstanceSuspended { instance_key, at } => {
            // Record the most-recent suspension instant. `AND state = 0` guards
            // against suspending a row that has already reached a terminal base
            // state; a suspended instance keeps its `Active` base code (0) and
            // stays in the in-flight gauge, so `delta` is unchanged. The
            // nullable `suspended_date_ms` column is the single source of truth
            // from which the derived `Suspended` state and `suspendedDate` value
            // are both read (see `map_instance`).
            tx.cexecute(
                "UPDATE process_instances SET suspended_date_ms = ?2 WHERE key = ?1 AND state = 0",
                params![*instance_key as i64, *at as i64],
            )?;
        }

        Event::ProcessInstanceResumed { instance_key } => {
            // Clear the suspension record so the instance derives back to
            // `Active` and its `suspendedDate` reverts to null. The base state
            // code was never moved off 0, so the gauge is unchanged.
            tx.cexecute(
                "UPDATE process_instances SET suspended_date_ms = NULL WHERE key = ?1",
                params![*instance_key as i64],
            )?;
        }

        Event::ElementActivating {
            instance_key,
            element_instance_key,
            element_id,
        } => {
            upsert_element_instance(
                tx,
                now_ms,
                *instance_key,
                *element_instance_key,
                element_id,
                None,
            )?;
        }

        Event::ElementActivated {
            instance_key,
            element_instance_key,
            element_id,
            scope,
        } => {
            // `ElementActivating` may have been pruned/spilled or never observed
            // (older journals); upsert so the row exists, and stamp the scope.
            upsert_element_instance(
                tx,
                now_ms,
                *instance_key,
                *element_instance_key,
                element_id,
                Some(*scope),
            )?;
        }

        Event::ElementCompleted {
            element_instance_key,
            ..
        } => {
            // Only a genuine Active->Completed transition stamps an end date; a
            // re-delivery finds the row already terminal and is a no-op.
            tx.cexecute(
                "UPDATE element_instances SET state = ?2, end_date_ms = ?3 \
                 WHERE element_instance_key = ?1 AND state = ?4",
                params![
                    *element_instance_key as i64,
                    element_instance_state_code(ElementInstanceState::Completed),
                    now_ms as i64,
                    element_instance_state_code(ElementInstanceState::Active),
                ],
            )?;
        }

        // --- Timer / signal / conditional waits (TIMER/SIGNAL/CONDITION) -----
        Event::TimerCreated {
            timer_key,
            instance_key,
            element_instance_key,
            element_id,
            due_at,
            kind,
        } => insert_event_wait(
            tx,
            *timer_key,
            EventWaitType::Timer,
            *instance_key,
            *element_instance_key,
            element_id,
            "",
            Some(*due_at),
            matches!(
                kind,
                nanobpmn_engine_core::TimerKind::NonInterruptingBoundary { .. }
            ),
        )?,
        Event::SignalSubscriptionCreated {
            subscription_key,
            instance_key,
            element_instance_key,
            element_id,
            signal_name,
            kind,
        } => insert_event_wait(
            tx,
            *subscription_key,
            EventWaitType::Signal,
            *instance_key,
            *element_instance_key,
            element_id,
            signal_name,
            None,
            is_non_interrupting(kind),
        )?,
        Event::ConditionalSubscriptionCreated {
            subscription_key,
            instance_key,
            element_instance_key,
            element_id,
            condition,
            kind,
            ..
        } => insert_event_wait(
            tx,
            *subscription_key,
            EventWaitType::Condition,
            *instance_key,
            *element_instance_key,
            element_id,
            condition,
            None,
            is_non_interrupting(kind),
        )?,
        // A timer fires once (the engine has no cycle timers), so it always
        // settles; cancellation settles every kind.
        Event::TimerTriggered { timer_key: key, .. }
        | Event::TimerCanceled { timer_key: key, .. }
        | Event::SignalSubscriptionCanceled {
            subscription_key: key,
            ..
        }
        | Event::ConditionalSubscriptionCanceled {
            subscription_key: key,
            ..
        } => {
            tx.cexecute(
                "DELETE FROM event_waits WHERE wait_key = ?1",
                params![*key as i64],
            )?;
        }
        // A non-interrupting boundary signal/condition stays open in the engine
        // (it can fire again); anything else settles on firing.
        Event::SignalCorrelated {
            subscription_key: key,
            ..
        }
        | Event::ConditionalTriggered {
            subscription_key: key,
            ..
        } => {
            tx.cexecute(
                "DELETE FROM event_waits WHERE wait_key = ?1 AND non_interrupting = 0",
                params![*key as i64],
            )?;
        }

        // --- Message subscriptions (MESSAGE wait states) ---------------------
        // Only instance-scoped subscriptions (a running element instance parked
        // on a message catch) are tracked; message-*start* subscriptions carry
        // no element instance and are not element-instance wait states.
        Event::MessageSubscriptionCreated {
            subscription_key,
            instance_key,
            element_instance_key,
            element_id,
            message_name,
            correlation_key,
            kind,
        } => {
            if *element_instance_key != 0 {
                let non_interrupting = matches!(
                    kind,
                    nanobpmn_engine_core::MessageSubscriptionKind::NonInterruptingBoundary { .. }
                );
                tx.cexecute(
                    "INSERT INTO message_subscriptions (subscription_key, instance_key, \
                     element_instance_key, element_id, message_name, correlation_key, created_at_ms, \
                     non_interrupting, business_id) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, (SELECT business_id FROM process_instances WHERE key = ?2)) \
                     ON CONFLICT(subscription_key) DO UPDATE SET \
                     element_instance_key = excluded.element_instance_key, \
                     element_id = excluded.element_id, \
                     message_name = excluded.message_name, \
                     correlation_key = excluded.correlation_key, \
                     non_interrupting = excluded.non_interrupting",
                    params![
                        *subscription_key as i64,
                        *instance_key as i64,
                        *element_instance_key as i64,
                        element_id,
                        message_name,
                        correlation_key,
                        now_ms as i64,
                        non_interrupting as i64,
                    ],
                )?;
            }
        }

        // A message correlated to an open subscription: record it in the
        // correlated (history) table. The open row still holds the message name,
        // correlation key and interrupting flag at this point (none of which are
        // carried on the correlation event), so capture them before deciding
        // whether to drop it. `RemoteMessageCorrelation` is the multi-partition
        // counterpart of `MessageCorrelated` — when the process instance lives on
        // another partition, the message partition settles the canonical
        // subscription with this event instead. In both cases `subscription_key`
        // is the canonical (message-partition) subscription, so its partition is
        // the one that correlated the message.
        Event::MessageCorrelated {
            subscription_key,
            message_key,
            instance_key,
            element_instance_key,
            element_id,
        }
        | Event::RemoteMessageCorrelation {
            subscription_key,
            message_key,
            instance_key,
            element_instance_key,
            element_id,
            ..
        } => {
            // Instance-scoped correlations (a running element instance parked on a
            // catch) are the only ones the open read model tracks; mirror that.
            if *element_instance_key != 0 {
                // Recover the message name / correlation key / interrupting flag
                // from the open row. If the open row is absent (out-of-order replay
                // or partial seeding), skip the history insert entirely rather than
                // record a row with an empty message name / correlation key that
                // would silently corrupt `searchCorrelatedMessageSubscriptions`.
                let captured: Option<(String, String, bool)> = tx
                    .query_row(
                        "SELECT message_name, correlation_key, non_interrupting \
                         FROM message_subscriptions WHERE subscription_key = ?1",
                        params![*subscription_key as i64],
                        |r| Ok((r.get(0)?, r.get(1)?, r.get::<_, i64>(2)? != 0)),
                    )
                    .optional()?;
                if let Some((message_name, correlation_key, non_interrupting)) = captured {
                    tx.cexecute(
                        "INSERT INTO correlated_message_subscriptions (message_key, subscription_key, \
                         instance_key, element_instance_key, element_id, message_name, correlation_key, \
                         correlation_time_ms, partition_id, business_id) \
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, (SELECT business_id FROM process_instances WHERE key = ?3)) \
                         ON CONFLICT(message_key, subscription_key) DO NOTHING",
                        params![
                            *message_key as i64,
                            *subscription_key as i64,
                            *instance_key as i64,
                            *element_instance_key as i64,
                            element_id,
                            message_name,
                            correlation_key,
                            now_ms as i64,
                            // 1-based partition id (Camunda/Zeebe convention), to
                            // match the topology mapping in main.rs.
                            (partition_of(*subscription_key) + 1) as i64,
                        ],
                    )?;
                    // A non-interrupting boundary subscription stays open in the
                    // engine so it can correlate again (each correlation spawns a
                    // parallel token); keep its open read-model row so subsequent
                    // correlations are still recorded. Interrupting boundaries and
                    // intermediate catches settle, so their open row is dropped.
                    if non_interrupting {
                        return Ok(delta);
                    }
                }
            }
            tx.cexecute(
                "DELETE FROM message_subscriptions WHERE subscription_key = ?1",
                params![*subscription_key as i64],
            )?;
        }

        // A cancelled subscription is no longer waiting and did not correlate: drop
        // the open row without recording a correlation.
        Event::MessageSubscriptionCanceled {
            subscription_key, ..
        } => {
            tx.cexecute(
                "DELETE FROM message_subscriptions WHERE subscription_key = ?1",
                params![*subscription_key as i64],
            )?;
        }

        Event::JobCreated {
            job_key,
            instance_key,
            element_instance_key,
            element_id,
            job_type,
            retries,
            created_at,
            ..
        } => {
            let (def_id, def_key) = instance_def(tx, *instance_key);
            // Camunda seeds both `creationTime` and the first `lastUpdateTime`
            // from the CREATED record timestamp. Prefer the engine-carried,
            // replay-stable `created_at`; fall back to the batch observation time
            // for older events that never recorded it (#1344). Seed BOTH columns
            // from the same value so a legacy (created_at == 0) job surfaces a
            // non-null `creationTime` (== `lastUpdateTime`) after recovery rather
            // than a null creation time against a populated last-update time.
            let creation_ms = if *created_at != 0 {
                *created_at
            } else {
                now_ms
            };
            let last_update = creation_ms;
            // The ON CONFLICT path fires only on re-delivery / overlapping-prefix
            // replay — a job's single genuine CREATED already landed — so it must
            // be a replay no-op and must never regress a row that has since
            // advanced (#1344 projection idempotency):
            // - `created_at_ms`: keep the stored value unless THIS event carries a
            //   real (nonzero) `created_at`. A legacy (`created_at == 0`)
            //   re-delivery must NOT re-stamp a fresh batch-time fallback (that
            //   would move `creationTime` every replay); a later real timestamp
            //   still repairs a stale fallback.
            // - state/retries/worker/deadline: preserved once the row has left
            //   `Created` (Activated or any terminal state), so replaying CREATED
            //   after a later event cannot resurrect an advanced/terminal job.
            // - `last_update_ms`: frozen at `end_ms` when set; held once the row
            //   advanced (so an endTime-less terminal Failed/Errored is never
            //   un-frozen by a replayed CREATED — the overlapping-prefix case); and
            //   for a still-`Created` row, re-seeded only from a real `created_at`
            //   (a legacy fallback is held), preserving `lastUpdateTime ==
            //   creationTime` without moving it on a bare re-delivery.
            tx.cexecute(
                "INSERT INTO jobs (key, instance_key, element_instance_key, element_id, job_type, \
                 state, retries, worker, deadline_ms, process_definition_id, process_definition_key, \
                 created_at_ms, last_update_ms, business_id) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, NULL, NULL, ?8, ?9, ?10, ?11, \
                 (SELECT business_id FROM process_instances WHERE key = ?2)) \
                 ON CONFLICT(key) DO UPDATE SET \
                 state = CASE WHEN jobs.state = ?6 THEN excluded.state ELSE jobs.state END, \
                 retries = CASE WHEN jobs.state = ?6 THEN excluded.retries ELSE jobs.retries END, \
                 worker = CASE WHEN jobs.state = ?6 THEN NULL ELSE jobs.worker END, \
                 deadline_ms = CASE WHEN jobs.state = ?6 THEN NULL ELSE jobs.deadline_ms END, \
                 created_at_ms = CASE WHEN ?12 <> 0 THEN excluded.created_at_ms ELSE jobs.created_at_ms END, \
                 last_update_ms = CASE \
                     WHEN jobs.end_ms IS NOT NULL THEN jobs.end_ms \
                     WHEN jobs.state <> ?6 THEN jobs.last_update_ms \
                     WHEN ?12 <> 0 THEN excluded.last_update_ms \
                     ELSE jobs.last_update_ms END",
                params![
                    *job_key as i64,
                    *instance_key as i64,
                    *element_instance_key as i64,
                    element_id,
                    job_type,
                    job_state_code(JobState::Created),
                    *retries,
                    def_id,
                    def_key,
                    creation_ms as i64,
                    last_update as i64,
                    *created_at as i64,
                ],
            )?;
        }

        Event::ExecutionListenerJobCreated {
            job_key,
            instance_key,
            element_instance_key,
            element_id,
            job_type,
            event_type,
            retries,
            created_at,
            ..
        } => {
            let (def_id, def_key) = instance_def(tx, *instance_key);
            let (kind_code, event_code) = job_kind_codes(&JobKind::ExecutionListener {
                event_type: *event_type,
                index: 0,
                scope: 0,
            });
            let last_update = if *created_at != 0 {
                *created_at
            } else {
                now_ms
            };
            let creation_ms = last_update;
            // See `JobCreated`: the ON CONFLICT path is a replay no-op that must
            // not move `created_at_ms`/`last_update_ms` for a legacy re-delivery
            // nor regress an advanced/terminal row (#1344).
            tx.cexecute(
                "INSERT INTO jobs (key, instance_key, element_instance_key, element_id, job_type, \
                 state, retries, worker, deadline_ms, process_definition_id, process_definition_key, \
                 job_kind, listener_event_type, created_at_ms, last_update_ms, business_id) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, NULL, NULL, ?8, ?9, ?10, ?11, ?12, ?13, \
                 (SELECT business_id FROM process_instances WHERE key = ?2)) \
                 ON CONFLICT(key) DO UPDATE SET \
                 state = CASE WHEN jobs.state = ?6 THEN excluded.state ELSE jobs.state END, \
                 retries = CASE WHEN jobs.state = ?6 THEN excluded.retries ELSE jobs.retries END, \
                 worker = CASE WHEN jobs.state = ?6 THEN NULL ELSE jobs.worker END, \
                 deadline_ms = CASE WHEN jobs.state = ?6 THEN NULL ELSE jobs.deadline_ms END, \
                 created_at_ms = CASE WHEN ?14 <> 0 THEN excluded.created_at_ms ELSE jobs.created_at_ms END, \
                 last_update_ms = CASE \
                     WHEN jobs.end_ms IS NOT NULL THEN jobs.end_ms \
                     WHEN jobs.state <> ?6 THEN jobs.last_update_ms \
                     WHEN ?14 <> 0 THEN excluded.last_update_ms \
                     ELSE jobs.last_update_ms END",
                params![
                    *job_key as i64,
                    *instance_key as i64,
                    *element_instance_key as i64,
                    element_id,
                    job_type,
                    job_state_code(JobState::Created),
                    *retries,
                    def_id,
                    def_key,
                    kind_code,
                    event_code,
                    creation_ms as i64,
                    last_update as i64,
                    *created_at as i64,
                ],
            )?;
        }

        Event::JobActivated {
            job_key,
            worker,
            deadline,
            fetch_variables,
            lease_token,
            ..
        } => {
            // Record the declared read-set alongside the activation. Serialized as
            // a JSON array (mirrors `candidate_groups`); empty stays '[]'. Only
            // overwritten when this activation declared a set, so a subsequent
            // fetch-all re-activation of the same job does not erase the last
            // declared provenance.
            //
            // The `worker` binding is normalized through `worker_attribution`: an
            // *empty* activation worker (`""`, e.g. an explicitly-supplied empty
            // string, or replayed from a `JobActivated` written before the engine
            // reducer normalized it) is not an attribution and must land as SQL
            // NULL, not `""`. Otherwise the terminal `COALESCE(?, worker)` on a
            // completion that carries no attribution would preserve that `""`,
            // pinning the completed row to an empty worker (#1191).
            if fetch_variables.is_empty() {
                tx.cexecute(
                    "UPDATE jobs SET state = ?2, worker = ?3, deadline_ms = ?4, lease_token = ?5 WHERE key = ?1",
                    params![
                        *job_key as i64,
                        job_state_code(JobState::Activated),
                        worker_attribution(Some(worker.as_str())),
                        *deadline as i64,
                        lease_token,
                    ],
                )?;
            } else {
                let read_set =
                    serde_json::to_string(fetch_variables).unwrap_or_else(|_| "[]".into());
                tx.cexecute(
                    "UPDATE jobs SET state = ?2, worker = ?3, deadline_ms = ?4, read_set = ?5, lease_token = ?6 \
                     WHERE key = ?1",
                    params![
                        *job_key as i64,
                        job_state_code(JobState::Activated),
                        worker_attribution(Some(worker.as_str())),
                        *deadline as i64,
                        read_set,
                        lease_token,
                    ],
                )?;
            }
        }

        Event::JobLockExpired { job_key, .. } => {
            // Camunda projects the TIMED_OUT record timestamp onto `lastUpdateTime`
            // (not `endTime`). Stamped only on the genuine Activated→Created
            // transition, so a re-delivery is a no-op (#1344). The `end_ms`-guard
            // is redundant with the `state = Activated` guard (a terminal job is
            // never Activated) but is kept for class-uniformity with the other
            // subsequent-event projections; the `state IN (2, 3)` arm is likewise
            // class-uniform (a Failed/Errored job is never Activated either).
            tx.cexecute(
                &format!(
                    "UPDATE jobs SET state = ?2, worker = NULL, deadline_ms = NULL, \
                 last_update_ms = CASE \
                     WHEN end_ms IS NOT NULL THEN end_ms \
                     WHEN {freeze} THEN last_update_ms \
                     ELSE ?4 END \
                 WHERE key = ?1 AND state = ?3",
                    freeze = endtimeless_terminal_job_predicate("state"),
                ),
                params![
                    *job_key as i64,
                    job_state_code(JobState::Created),
                    job_state_code(JobState::Activated),
                    now_ms as i64,
                ],
            )?;
        }

        Event::JobFailed {
            job_key,
            retries,
            worker,
            error_message,
            ..
        } => {
            // Zeebe parity (#1327): every fail records the worker's message and
            // whether retries remain. `COALESCE` keeps the last known message for
            // events serialized before the field existed. `lastUpdateTime` is
            // frozen once the job is terminal (#1344): a re-delivered / replayed
            // FAILED for a job that has already ended must NOT push
            // `lastUpdateTime` past `endTime` (the `lastUpdateTime == endTime`
            // invariant + the "re-delivery doesn't move the times" contract).
            // Frozen at `endTime` when it is set (Completed/Canceled), and held
            // at its current value once the job is in a terminal state that has
            // NO `endTime` (Failed/Errored) — a re-delivered FAILED must not move
            // `lastUpdateTime` off the first terminal transition.
            tx.cexecute(
                &format!(
                    "UPDATE jobs SET error_message = COALESCE(?2, error_message), \
                 has_failed_with_retries_left = ?3, \
                 last_update_ms = CASE \
                     WHEN end_ms IS NOT NULL THEN end_ms \
                     WHEN {freeze} THEN last_update_ms \
                     ELSE ?4 END \
                 WHERE key = ?1",
                    freeze = endtimeless_terminal_job_predicate("state"),
                ),
                params![
                    *job_key as i64,
                    error_message,
                    i64::from(*retries > 0),
                    now_ms as i64
                ],
            )?;
            if *retries > 0 {
                // Back to the activatable pool — drop the last activating worker.
                tx.cexecute(
                    "UPDATE jobs SET state = ?2, retries = ?3, worker = NULL, deadline_ms = NULL \
                     WHERE key = ?1",
                    params![*job_key as i64, job_state_code(JobState::Created), retries],
                )?;
            } else {
                // Terminal, incident-bearing park: set `worker` from the event so
                // the incident (joined by `jobKey`) can attribute the failure even
                // on the leader-local path, where `JobActivated` is never exported
                // and the row's `worker` was therefore still NULL (Zeebe parity).
                // `COALESCE(?4, worker)` keeps any existing *real*
                // attribution for events serialized before the field existed
                // (`worker == NULL`), while `NULLIF(worker, '')` drops a legacy
                // empty-string attribution — a pre-#1191 read-model DB, migrated
                // non-destructively, can still hold `worker = ''` written by the
                // old `JobActivated` projection, and a bare `COALESCE(NULL, '')`
                // would pin the terminal row to that bogus empty worker (#1191).
                tx.cexecute(
                    "UPDATE jobs SET state = ?2, retries = ?3, worker = COALESCE(?4, NULLIF(worker, '')), \
                     deadline_ms = NULL WHERE key = ?1",
                    params![
                        *job_key as i64,
                        job_state_code(JobState::Failed),
                        retries,
                        worker_attribution(worker.as_deref())
                    ],
                )?;
            }
        }

        Event::JobErrorThrown {
            job_key,
            worker,
            error_code,
            error_message,
            ..
        } => {
            // Zeebe parity (#1327): the thrown code and message land on the job;
            // the exporter keys `jobFailedWithRetriesLeft` off the record's
            // (unchanged) retries for ERROR_THROWN as well as FAILED.
            // `lastUpdateTime` freeze (#1344): ERROR_THROWN transitions the job to
            // terminal `Errored` but deliberately leaves `end_ms` NULL (Camunda
            // stamps no `endTime` for an errored job), so the `end_ms`-guard alone
            // never engages here and a re-delivered/replayed ERROR_THROWN would
            // stamp a fresh `now_ms` every time. Freeze at `endTime` when set
            // (Completed/Canceled), and hold the current value once the job is in
            // a terminal `endTime`-less state (Failed/Errored) — the first
            // transition still stamps `now_ms`, a re-delivery is then a no-op.
            tx.cexecute(
                &format!(
                    "UPDATE jobs SET error_code = ?2, error_message = COALESCE(?3, error_message), \
                 has_failed_with_retries_left = (retries > 0), \
                 last_update_ms = CASE \
                     WHEN end_ms IS NOT NULL THEN end_ms \
                     WHEN {freeze} THEN last_update_ms \
                     ELSE ?4 END \
                 WHERE key = ?1",
                    freeze = endtimeless_terminal_job_predicate("state"),
                ),
                params![*job_key as i64, error_code, error_message, now_ms as i64],
            )?;
            // Terminal, incident-bearing transition: set `worker` from the event
            // for attribution (see `JobFailed`); `COALESCE` keeps any existing
            // value for pre-field events, and `NULLIF(worker, '')` drops a legacy
            // empty-string attribution from a pre-#1191 migrated DB.
            tx.cexecute(
                "UPDATE jobs SET state = ?2, worker = COALESCE(?3, NULLIF(worker, '')), deadline_ms = NULL \
                 WHERE key = ?1",
                params![
                    *job_key as i64,
                    job_state_code(JobState::Errored),
                    worker_attribution(worker.as_deref())
                ],
            )?;
        }

        Event::JobCompleted {
            job_key, worker, ..
        } => {
            // Set `worker` from the event so a *successful* completion is
            // attributable to the activating worker on the read-model
            // leader-local path too, where `JobActivated` is never exported and
            // the row's `worker` was therefore still NULL (symmetric with the
            // `JobFailed` / `JobErrorThrown` terminal rows). `COALESCE(?3, worker)`
            // keeps any existing value for events serialized before the field
            // existed (`worker == NULL`), and `NULLIF(worker, '')` drops a legacy
            // empty-string attribution from a pre-#1191 migrated DB. This is what
            // makes a husked agent round (a COMPLETED job that minted no
            // AgentInstance) attributable via the
            // `AgentInstance.jobKey → completed Job.worker` join.
            //
            // Timing (#1344): stamp `lastUpdateTime` and `endTime` from the batch
            // observation time, but only on the first active→terminal transition
            // (`end_ms IS NULL`), so a re-delivery is a no-op — mirroring
            // `ElementCompleted`. The deadline is NOT cleared: Camunda keeps the
            // last projected deadline on a completed job.
            tx.cexecute(
                "UPDATE jobs SET state = ?2, worker = COALESCE(?3, NULLIF(worker, '')), \
                 last_update_ms = CASE WHEN end_ms IS NULL THEN ?4 ELSE last_update_ms END, \
                 end_ms = CASE WHEN end_ms IS NULL THEN ?4 ELSE end_ms END \
                 WHERE key = ?1",
                params![
                    *job_key as i64,
                    job_state_code(JobState::Completed),
                    worker_attribution(worker.as_deref()),
                    now_ms as i64,
                ],
            )?;
        }

        Event::JobCanceled { job_key, .. } => {
            // Camunda stamps `endTime` on CANCELED too (but keeps the deadline).
            // Guard on the first terminal transition so a re-delivery is a no-op;
            // a job that was already FAILED/ERRORED (no `end_ms`) still gets its
            // cancellation `endTime` here (#1344).
            tx.cexecute(
                "UPDATE jobs SET state = ?2, worker = NULL, \
                 last_update_ms = CASE WHEN end_ms IS NULL THEN ?3 ELSE last_update_ms END, \
                 end_ms = CASE WHEN end_ms IS NULL THEN ?3 ELSE end_ms END \
                 WHERE key = ?1",
                params![
                    *job_key as i64,
                    job_state_code(JobState::Canceled),
                    now_ms as i64,
                ],
            )?;
        }

        Event::JobRetriesUpdated {
            job_key, retries, ..
        } => {
            // RETRIES_UPDATED is a projected job event: refresh `lastUpdateTime`
            // (#1344). It never ends the job, so `end_ms` is untouched; the
            // `end_ms`-guard freezes `lastUpdateTime` at `endTime` should this
            // arrive (via re-delivery/replay) for an already-terminal job, and the
            // `state IN (2, 3)` arm holds it for a terminal job with NO `endTime`
            // (Failed/Errored) — a replayed RETRIES_UPDATED must not move it.
            tx.cexecute(
                &format!(
                    "UPDATE jobs SET retries = ?2, \
                 last_update_ms = CASE \
                     WHEN end_ms IS NOT NULL THEN end_ms \
                     WHEN {freeze} THEN last_update_ms \
                     ELSE ?3 END \
                 WHERE key = ?1",
                    freeze = endtimeless_terminal_job_predicate("state"),
                ),
                params![*job_key as i64, retries, now_ms as i64],
            )?;
        }

        Event::JobTimeoutUpdated {
            job_key, deadline, ..
        } => {
            // TIMEOUT_UPDATED is a projected job event: refresh `lastUpdateTime`
            // alongside the extended deadline (#1344). The `end_ms`-guard freezes
            // `lastUpdateTime` at `endTime` for an already-terminal job, and the
            // `state IN (2, 3)` arm holds it for a terminal job with NO `endTime`
            // (Failed/Errored), so a re-delivery/replay cannot push it past the
            // terminal transition.
            tx.cexecute(
                &format!(
                    "UPDATE jobs SET deadline_ms = ?2, \
                 last_update_ms = CASE \
                     WHEN end_ms IS NOT NULL THEN end_ms \
                     WHEN {freeze} THEN last_update_ms \
                     ELSE ?3 END \
                 WHERE key = ?1",
                    freeze = endtimeless_terminal_job_predicate("state"),
                ),
                params![*job_key as i64, *deadline as i64, now_ms as i64],
            )?;
        }

        Event::UserTaskCreated {
            user_task_key,
            instance_key,
            element_instance_key,
            element_id,
            created_at,
            assignee,
            candidate_groups,
            candidate_users,
            due_date,
            follow_up_date,
            priority,
            form_key,
            external_form_reference,
        } => {
            let (def_id, def_key) = instance_def(tx, *instance_key);
            let version = instance_version(tx, *instance_key);
            let groups = serde_json::to_string(candidate_groups).unwrap_or_else(|_| "[]".into());
            let users = serde_json::to_string(candidate_users).unwrap_or_else(|_| "[]".into());
            tx.cexecute(
                "INSERT INTO user_tasks (key, instance_key, element_instance_key, element_id, \
                 state, assignee, candidate_groups, candidate_users, due_date, follow_up_date, \
                 priority, created_at_ms, process_definition_id, process_definition_key, \
                 process_definition_version, form_key, external_form_reference, business_id) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, \
                 (SELECT business_id FROM process_instances WHERE key = ?2)) \
                 ON CONFLICT(key) DO UPDATE SET state = excluded.state",
                params![
                    *user_task_key as i64,
                    *instance_key as i64,
                    *element_instance_key as i64,
                    element_id,
                    user_task_state_code(UserTaskState::Created),
                    assignee,
                    groups,
                    users,
                    due_date,
                    follow_up_date,
                    *priority,
                    *created_at as i64,
                    def_id,
                    def_key,
                    version,
                    form_key.map(|k| k as i64),
                    external_form_reference.as_ref(),
                ],
            )?;
        }

        Event::UserTaskAssigned {
            user_task_key,
            assignee,
            ..
        } => {
            tx.cexecute(
                "UPDATE user_tasks SET assignee = ?2 WHERE key = ?1",
                params![*user_task_key as i64, assignee],
            )?;
        }

        Event::UserTaskUpdated {
            user_task_key,
            candidate_groups,
            candidate_users,
            due_date,
            follow_up_date,
            priority,
            ..
        } => {
            if let Some(groups) = candidate_groups {
                let json = serde_json::to_string(groups).unwrap_or_else(|_| "[]".into());
                tx.cexecute(
                    "UPDATE user_tasks SET candidate_groups = ?2 WHERE key = ?1",
                    params![*user_task_key as i64, json],
                )?;
            }
            if let Some(users) = candidate_users {
                let json = serde_json::to_string(users).unwrap_or_else(|_| "[]".into());
                tx.cexecute(
                    "UPDATE user_tasks SET candidate_users = ?2 WHERE key = ?1",
                    params![*user_task_key as i64, json],
                )?;
            }
            if let Some(due) = due_date {
                tx.cexecute(
                    "UPDATE user_tasks SET due_date = ?2 WHERE key = ?1",
                    params![*user_task_key as i64, due],
                )?;
            }
            if let Some(follow_up) = follow_up_date {
                tx.cexecute(
                    "UPDATE user_tasks SET follow_up_date = ?2 WHERE key = ?1",
                    params![*user_task_key as i64, follow_up],
                )?;
            }
            if let Some(p) = priority {
                tx.cexecute(
                    "UPDATE user_tasks SET priority = ?2 WHERE key = ?1",
                    params![*user_task_key as i64, p],
                )?;
            }
        }

        Event::UserTaskCompleted { user_task_key, .. } => {
            tx.cexecute(
                "UPDATE user_tasks SET state = ?2 WHERE key = ?1",
                params![
                    *user_task_key as i64,
                    user_task_state_code(UserTaskState::Completed)
                ],
            )?;
        }

        Event::UserTaskCanceled { user_task_key, .. } => {
            tx.cexecute(
                "UPDATE user_tasks SET state = ?2 WHERE key = ?1",
                params![
                    *user_task_key as i64,
                    user_task_state_code(UserTaskState::Canceled)
                ],
            )?;
        }

        Event::IncidentRaised {
            incident_key,
            instance_key,
            element_instance_key,
            element_id,
            kind,
            reason,
            job_key,
            created_at,
            redrive: _,
        } => {
            let (def_id, def_key) = instance_def(tx, *instance_key);
            tx.cexecute(
                "INSERT INTO incidents (key, instance_key, element_instance_key, element_id, kind, \
                 state, reason, job_key, created_at_ms, process_definition_id, process_definition_key) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11) \
                 ON CONFLICT(key) DO UPDATE SET state = excluded.state",
                params![
                    *incident_key as i64,
                    *instance_key as i64,
                    *element_instance_key as i64,
                    element_id,
                    incident_kind_code(*kind),
                    incident_state_code(IncidentState::Active),
                    reason,
                    job_key.map(|k| k as i64),
                    *created_at as i64,
                    def_id,
                    def_key,
                ],
            )?;
            tx.cexecute(
                "UPDATE process_instances SET has_incident = 1 WHERE key = ?1",
                params![*instance_key as i64],
            )?;
            // Surface the incident on its element instance too.
            tx.cexecute(
                "UPDATE element_instances SET has_incident = 1, incident_key = ?2 \
                 WHERE element_instance_key = ?1",
                params![*element_instance_key as i64, *incident_key as i64],
            )?;
        }

        Event::IncidentResolved {
            incident_key,
            instance_key,
            job_key,
            resolved_at: _,
            operation_reference: _,
        } => {
            tx.cexecute(
                "UPDATE incidents SET state = ?2 WHERE key = ?1",
                params![
                    *incident_key as i64,
                    incident_state_code(IncidentState::Resolved)
                ],
            )?;
            // `hasIncident` reflects only still-active incidents.
            let active: i64 = tx.cquery_row(
                "SELECT COUNT(*) FROM incidents WHERE instance_key = ?1 AND state = ?2",
                params![
                    *instance_key as i64,
                    incident_state_code(IncidentState::Active)
                ],
                |r| r.get(0),
            )?;
            tx.cexecute(
                "UPDATE process_instances SET has_incident = ?2 WHERE key = ?1",
                params![*instance_key as i64, i64::from(active > 0)],
            )?;
            // Clear the incident flag on the element instance it was raised on
            // (only when this incident is the one currently referenced there).
            tx.cexecute(
                "UPDATE element_instances SET has_incident = 0, incident_key = NULL \
                 WHERE incident_key = ?1",
                params![*incident_key as i64],
            )?;
            // A recoverable job-incident returns its parked job to the pool.
            if let Some(job_key) = job_key {
                tx.cexecute(
                    "UPDATE jobs SET state = ?2, worker = NULL, deadline_ms = NULL WHERE key = ?1",
                    params![*job_key as i64, job_state_code(JobState::Created)],
                )?;
            }
        }

        Event::VariablesUpdated {
            instance_key,
            variables,
        } => {
            upsert_variables(tx, *instance_key, *instance_key, variables)?;
        }

        // A write to a nested variable scope (sub-process, multi-instance body or
        // child): materialize it under its own `scope_key` so `searchVariables`
        // reports the Zeebe-correct `scopeKey`. The scope's local variables are
        // retained after the scope tears down (the read model keeps history, like
        // an instance's variables after it completes).
        Event::ScopedVariablesUpdated {
            instance_key,
            scope_key,
            variables,
        } => {
            upsert_variables(tx, *instance_key, *scope_key, variables)?;
        }

        Event::DecisionRequirementsDeployed {
            decision_requirements_key,
            version,
            drg,
            ..
        } => {
            // Latest version per DRG id (a redeploy replaces), mirroring the
            // engine's `state.decision_requirements`. The resource name is not
            // retained by the engine, so it is synthesized from the DRG id (as the
            // process read model does for BPMN); the raw XML is carried on the DRG.
            let resource_name = format!("{}.dmn", drg.id);
            tx.cexecute(
                "INSERT INTO decision_requirements (drg_id, drg_key, name, version, resource_name, xml) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6) \
                 ON CONFLICT(drg_id) DO UPDATE SET drg_key = excluded.drg_key, \
                 name = excluded.name, version = excluded.version, \
                 resource_name = excluded.resource_name, xml = excluded.xml",
                params![
                    drg.id,
                    *decision_requirements_key as i64,
                    drg.name,
                    version,
                    resource_name,
                    drg.xml,
                ],
            )?;
        }

        Event::DecisionDeployed {
            decision_requirements_key,
            decision_key,
            decision_id,
            decision_name,
            version,
            ..
        } => {
            // Resolve the owning DRG's id/name/version (its
            // DecisionRequirementsDeployed was projected first, in emission order).
            let (drg_id, drg_name, drg_version): (String, String, i32) = tx
                .cquery_row(
                    "SELECT drg_id, name, version FROM decision_requirements WHERE drg_key = ?1",
                    params![*decision_requirements_key as i64],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
                )
                .optional()
                .ok()
                .flatten()
                .unwrap_or_default();
            tx.cexecute(
                "INSERT INTO decision_definitions \
                 (decision_id, decision_key, name, version, decision_requirements_key, \
                  decision_requirements_id, decision_requirements_name, decision_requirements_version) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8) \
                 ON CONFLICT(decision_id) DO UPDATE SET decision_key = excluded.decision_key, \
                 name = excluded.name, version = excluded.version, \
                 decision_requirements_key = excluded.decision_requirements_key, \
                 decision_requirements_id = excluded.decision_requirements_id, \
                 decision_requirements_name = excluded.decision_requirements_name, \
                 decision_requirements_version = excluded.decision_requirements_version",
                params![
                    decision_id,
                    *decision_key as i64,
                    decision_name,
                    version,
                    *decision_requirements_key as i64,
                    drg_id,
                    drg_name,
                    drg_version,
                ],
            )?;
        }

        Event::FormDeployed {
            form_key,
            version,
            form_id,
            resource_name,
            schema,
            ..
        } => {
            // One row per deployed form version, keyed by its unique form_key so
            // GetFormByKey resolves every version. The upsert is idempotent on a
            // journal replay (the same event re-projects identical data).
            tx.cexecute(
                "INSERT INTO forms (form_key, form_id, version, schema, resource_name, tenant_id) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6) \
                 ON CONFLICT(form_key) DO UPDATE SET form_id = excluded.form_id, \
                 version = excluded.version, schema = excluded.schema, \
                 resource_name = excluded.resource_name, tenant_id = excluded.tenant_id",
                params![
                    *form_key as i64,
                    form_id,
                    version,
                    schema,
                    resource_name,
                    "<default>",
                ],
            )?;
        }

        Event::GenericResourceDeployed {
            resource_key,
            version,
            resource_id,
            resource_name,
            content,
            ..
        } => {
            // One row per deployed generic-resource version, keyed by its unique
            // resource_key so GetResourceByKey resolves every version. The upsert
            // is idempotent on a journal replay.
            tx.cexecute(
                "INSERT INTO resources (resource_key, resource_id, resource_name, version, \
                 version_tag, content, tenant_id) \
                 VALUES (?1, ?2, ?3, ?4, NULL, ?5, ?6) \
                 ON CONFLICT(resource_key) DO UPDATE SET resource_id = excluded.resource_id, \
                 resource_name = excluded.resource_name, version = excluded.version, \
                 version_tag = excluded.version_tag, content = excluded.content, \
                 tenant_id = excluded.tenant_id",
                params![
                    *resource_key as i64,
                    resource_id,
                    resource_name,
                    version,
                    content,
                    "<default>",
                ],
            )?;
        }

        Event::DecisionEvaluated {
            instance_key,
            element_instance_key,
            decision_key: root_decision_key,
            evaluated_decisions,
            evaluated_at,
            failure,
            decision_requirements_key,
            decision_requirements_id,
            ..
        } => {
            // The evaluation's own key (#1292): minted per evaluation, so repeat
            // and standalone evaluations never collide. Legacy records derive it
            // canonically (see `Event::decision_evaluation_key`).
            let evaluation_key = event
                .decision_evaluation_key()
                .expect("DecisionEvaluated always identifies its evaluation");
            let last = evaluated_decisions.len();
            // The owning process definition key (join within this shard; the
            // businessRuleTask instance is projected here). Empty when absent.
            let process_definition_key: String = tx
                .cquery_row(
                    "SELECT process_definition_key FROM process_instances WHERE key = ?1",
                    params![*instance_key as i64],
                    |r| r.get(0),
                )
                .optional()
                .ok()
                .flatten()
                .unwrap_or_default();
            // One decision-instance row per evaluated decision (required
            // decisions first, root decision last), indexed 1-based within the
            // evaluation, mirroring Zeebe's decision-instance records. A failed
            // evaluation's trail ends with the decision that failed: that row is
            // FAILED and carries the failure; the others stay EVALUATED (Zeebe
            // exporter parity).
            for (i, ed) in evaluated_decisions.iter().enumerate() {
                let idx = i + 1;
                let eval_instance_key = nanobpmn_engine_core::dmn::decision_evaluation_instance_key(
                    evaluation_key,
                    idx,
                );
                let idx = idx as i64;
                // The engine stamps the exact definition evaluated (within the
                // evaluated DRG version) and the DRG itself; records from before
                // #1292 (`0` / empty) fall back to the latest deployment by id —
                // all such a record can say.
                let (decision_key, version, drg_id, drg_key): (i64, i32, String, i64) = if ed
                    .decision_key
                    != 0
                    && *decision_requirements_key != 0
                {
                    (
                        ed.decision_key as i64,
                        ed.decision_version,
                        decision_requirements_id.clone(),
                        *decision_requirements_key as i64,
                    )
                } else {
                    tx.cquery_row(
                            "SELECT decision_key, version, decision_requirements_id, \
                             decision_requirements_key FROM decision_definitions WHERE decision_id = ?1",
                            params![ed.decision_id],
                            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
                        )
                        .optional()
                        .ok()
                        .flatten()
                        .unwrap_or((*root_decision_key as i64, 1, String::new(), 0))
                };
                let failed = failure.as_ref().filter(|_| i + 1 == last);
                let result_json = serde_json::to_string(&crate::value_to_json(&ed.decision_output))
                    .unwrap_or_else(|_| "null".to_string());
                let inputs_json = serde_json::to_string(
                    &ed.evaluated_inputs
                        .iter()
                        .map(|inp| {
                            serde_json::json!({
                                "inputId": inp.input_id,
                                "inputName": inp.input_name,
                                "inputValue": serde_json::to_string(&crate::value_to_json(&inp.input_value))
                                    .unwrap_or_else(|_| "null".to_string()),
                            })
                        })
                        .collect::<Vec<_>>(),
                )
                .unwrap_or_else(|_| "[]".to_string());
                let rules_json = serde_json::to_string(
                    &ed.matched_rules
                        .iter()
                        .map(|rule| {
                            serde_json::json!({
                                "ruleId": rule.rule_id,
                                "ruleIndex": rule.rule_index,
                                "evaluatedOutputs": rule
                                    .evaluated_outputs
                                    .iter()
                                    .map(|out| {
                                        serde_json::json!({
                                            "outputId": out.output_id,
                                            "outputName": out.output_name,
                                            "outputValue": serde_json::to_string(&crate::value_to_json(&out.output_value))
                                                .unwrap_or_else(|_| "null".to_string()),
                                        })
                                    })
                                    .collect::<Vec<_>>(),
                            })
                        })
                        .collect::<Vec<_>>(),
                )
                .unwrap_or_else(|_| "[]".to_string());
                let decision_type = crate::dmn_decision_type_name(&ed.decision_type);
                tx.cexecute(
                    "INSERT INTO decision_instances \
                     (eval_instance_key, decision_evaluation_key, idx, decision_id, decision_key, \
                      decision_name, decision_type, version, decision_requirements_id, \
                      decision_requirements_key, root_decision_key, instance_key, \
                      element_instance_key, process_definition_key, state, evaluation_failure, \
                      evaluation_date_ms, result_json, inputs_json, rules_json, tenant_id, business_id) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, \
                      ?16, ?17, ?18, ?19, ?20, ?21, (SELECT business_id FROM process_instances WHERE key = ?12)) \
                     ON CONFLICT(eval_instance_key) DO NOTHING",
                    params![
                        eval_instance_key,
                        evaluation_key as i64,
                        idx,
                        ed.decision_id,
                        decision_key,
                        ed.decision_name,
                        decision_type,
                        version,
                        drg_id,
                        drg_key,
                        *root_decision_key as i64,
                        *instance_key as i64,
                        *element_instance_key as i64,
                        process_definition_key,
                        if failed.is_some() { "FAILED" } else { "EVALUATED" },
                        failed.map(|f| f.message.clone()),
                        *evaluated_at as i64,
                        result_json,
                        inputs_json,
                        rules_json,
                        "<default>",
                    ],
                )?;
            }
        }

        Event::DecisionInstanceDeleted {
            decision_evaluation_key,
            instance_key,
        } => {
            // Retract every decision-instance row of this evaluation (one per
            // evaluated decision). Idempotent: a replay or a broadcast to a shard
            // that never held the rows deletes nothing. Because this is journaled
            // on the owning instance's partition (same shard as the originating
            // DecisionEvaluated), the deletion survives replay/rebuild.
            //
            // Scoped to the owning instance (`0` for a standalone evaluation).
            // Records written before #1292 carry the evaluation's root decision
            // *definition* key rather than an evaluation key, so the second arm
            // retracts that instance's rows of that decision — whether they were
            // projected under the legacy definition-keyed scheme or re-projected
            // under the per-evaluation scheme. Evaluation keys and definition
            // keys come from one key space, so the arms never cross-match.
            tx.cexecute(
                "DELETE FROM decision_instances WHERE instance_key = ?2 \
                 AND (decision_evaluation_key = ?1 OR root_decision_key = ?1)",
                params![*decision_evaluation_key as i64, *instance_key as i64],
            )?;
        }

        Event::ProcessInstanceMigrated {
            instance_key,
            target_process_id,
            target_process_definition_key,
            element_mappings,
        } => {
            // Re-home the read model onto the target definition, mirroring the
            // engine applier (`state::apply`): the instance's definition
            // identity moves, and every LIVE runtime row's `element_id` is
            // remapped by its ORIGINAL id (never chained). Completed/terminal
            // history rows keep the definition + element id they ran under.
            let ik = *instance_key as i64;
            let target_key_str = target_process_definition_key.to_string();
            let target_key_i = *target_process_definition_key as i64;
            let target_version: i32 = tx
                .query_row(
                    "SELECT version FROM process_definitions WHERE key = ?1",
                    params![target_key_i],
                    |r| r.get::<_, i32>(0),
                )
                .optional()?
                .ok_or_else(|| {
                    // The engine validated the target definition is deployed
                    // before emitting this event, so a missing row is read-model
                    // corruption — fail loudly rather than writing version=0.
                    rusqlite::Error::SqliteFailure(
                        rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_ERROR),
                        Some(format!(
                            "migration projection: target process definition key {target_key_i} \
                             missing from process_definitions"
                        )),
                    )
                })?;

            // 1) Re-point the instance row itself.
            tx.cexecute(
                "UPDATE process_instances SET process_id = ?2, process_definition_id = ?2, \
                 process_definition_key = ?3, version = ?4 WHERE key = ?1",
                params![ik, target_process_id, target_key_str, target_version],
            )?;

            // 2) Remap live runtime element ids in a single logical pass. A
            //    control-char (`SOH`) temp namespace — illegal in a BPMN NCName
            //    id — makes the two SQL passes equivalent to the engine's
            //    remap-by-original-id map even when a target id equals another
            //    mapping's source (loops / swaps). Job types are preserved (the
            //    worker keeps its lease), matching the applier.
            let active_el = element_instance_state_code(ElementInstanceState::Active);
            let tmp = |target: &str| format!("\u{1}mig:{target}");

            // Phase A — stamp matched live rows with a collision-free temp id.
            for (source_id, target_id) in element_mappings {
                let t = tmp(target_id);
                tx.cexecute(
                    "UPDATE element_instances SET element_id = ?3 \
                     WHERE instance_key = ?1 AND element_id = ?2 AND state = ?4",
                    params![ik, source_id, t, active_el],
                )?;
                // No state filter on jobs / user_tasks / incidents: the engine
                // applier re-points `element_id` on *every* such row the instance
                // owns — it loops all of `state.jobs`, `state.user_tasks` and
                // `state.incidents` filtering only by `instance_key`, and never
                // removes terminal rows — so a job that is live-but-parked
                // (`Failed` with retries=0) or terminal (`Errored`/`Completed`/
                // `Canceled`), a user task that is `Completed`/`Canceled`, and a
                // `Resolved` incident are all remapped there too. Filtering to a
                // "live" state here (`Created` user tasks / `Active` incidents)
                // would be a narrower, divergent notion of "live" than the single
                // source of truth (the engine) and would silently leave terminal
                // rows pointing at stale source element ids (and, for those tables
                // re-homed in Phase B, stale definition identity) — and adding a
                // new state would silently widen that drift. Mirror the applier
                // exactly and remap by original element id alone. (Only the
                // `element_instances` remap keeps a state filter, because the
                // applier likewise remaps only *active* element instances — the
                // ids in `instance.active`.)
                tx.cexecute(
                    "UPDATE jobs SET element_id = ?3 \
                     WHERE instance_key = ?1 AND element_id = ?2",
                    params![ik, source_id, t],
                )?;
                tx.cexecute(
                    "UPDATE user_tasks SET element_id = ?3 \
                     WHERE instance_key = ?1 AND element_id = ?2",
                    params![ik, source_id, t],
                )?;
                tx.cexecute(
                    "UPDATE incidents SET element_id = ?3 \
                     WHERE instance_key = ?1 AND element_id = ?2",
                    params![ik, source_id, t],
                )?;
                // Message subscriptions in the read model are all live (rows are
                // dropped on correlation/cancel/termination), and carry no
                // definition-identity columns, so remap by original element id
                // with no state filter. A migrated instance waiting on a message
                // catch event must re-home its subscription alongside the token.
                tx.cexecute(
                    "UPDATE message_subscriptions SET element_id = ?3 \
                     WHERE instance_key = ?1 AND element_id = ?2",
                    params![ik, source_id, t],
                )?;
            }

            // Phase B — resolve temp ids to the target id + re-home the
            //    definition identity (once per distinct target).
            let mut resolved: std::collections::HashSet<&str> = std::collections::HashSet::new();
            for (_source_id, target_id) in element_mappings {
                if !resolved.insert(target_id.as_str()) {
                    continue;
                }
                let t = tmp(target_id);
                let (t_name, t_type): (Option<String>, String) = tx
                    .query_row(
                        "SELECT element_name, element_type FROM definition_elements \
                         WHERE process_definition_key = ?1 AND element_id = ?2",
                        params![target_key_i, target_id],
                        |r| Ok((r.get::<_, Option<String>>(0)?, r.get::<_, String>(1)?)),
                    )
                    .optional()?
                    .ok_or_else(|| {
                        // The engine validated every mapped target element exists
                        // before emitting this event, so missing metadata is read
                        // model corruption — fail loudly rather than overwriting
                        // `element_type` with an empty string.
                        rusqlite::Error::SqliteFailure(
                            rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_ERROR),
                            Some(format!(
                                "migration projection: target element '{target_id}' missing from \
                                 definition_elements for process definition key {target_key_i}"
                            )),
                        )
                    })?;
                tx.cexecute(
                    "UPDATE element_instances SET element_id = ?2, element_name = ?3, \
                     element_type = ?4, process_definition_id = ?5, process_definition_key = ?6 \
                     WHERE instance_key = ?1 AND element_id = ?7",
                    params![
                        ik,
                        target_id,
                        t_name,
                        t_type,
                        target_process_id,
                        target_key_str,
                        t
                    ],
                )?;
                tx.cexecute(
                    "UPDATE jobs SET element_id = ?2, process_definition_id = ?3, \
                     process_definition_key = ?4 WHERE instance_key = ?1 AND element_id = ?5",
                    params![ik, target_id, target_process_id, target_key_str, t],
                )?;
                tx.cexecute(
                    "UPDATE user_tasks SET element_id = ?2, process_definition_id = ?3, \
                     process_definition_key = ?4, process_definition_version = ?5 \
                     WHERE instance_key = ?1 AND element_id = ?6",
                    params![
                        ik,
                        target_id,
                        target_process_id,
                        target_key_str,
                        target_version,
                        t
                    ],
                )?;
                tx.cexecute(
                    "UPDATE incidents SET element_id = ?2, process_definition_id = ?3, \
                     process_definition_key = ?4 WHERE instance_key = ?1 AND element_id = ?5",
                    params![ik, target_id, target_process_id, target_key_str, t],
                )?;
                // Resolve the temp-stamped message subscriptions (they carry no
                // definition identity, so only the element id moves).
                tx.cexecute(
                    "UPDATE message_subscriptions SET element_id = ?2 \
                     WHERE instance_key = ?1 AND element_id = ?3",
                    params![ik, target_id, t],
                )?;
            }

            // Variables carry the definition identity but no element id, so
            // re-home them wholesale for the instance.
            tx.cexecute(
                "UPDATE variables SET process_definition_id = ?2, process_definition_key = ?3 \
                 WHERE instance_key = ?1",
                params![ik, target_process_id, target_key_str],
            )?;
        }

        Event::AgentInstanceCreated {
            instance_key: _,
            agent_instance,
        } => {
            project_agent_instance(tx, agent_instance)?;
        }

        Event::AgentInstanceUpdated {
            instance_key: _,
            agent_instance,
        }
        | Event::AgentInstanceCompleted {
            instance_key: _,
            agent_instance,
        } => {
            project_agent_instance(tx, agent_instance)?;
        }

        Event::AgentHistoryCreated {
            instance_key: _,
            record,
        } => {
            project_agent_history_record(tx, record)?;
        }

        Event::AgentHistoryCommitted {
            instance_key: _,
            agent_instance_key,
            agent_history_keys,
        } => {
            transition_agent_history(
                tx,
                *agent_instance_key,
                agent_history_keys,
                AgentHistoryCommitStatus::Committed,
            )?;
        }

        Event::AgentHistoryDiscarded {
            instance_key: _,
            agent_instance_key,
            agent_history_keys,
        } => {
            transition_agent_history(
                tx,
                *agent_instance_key,
                agent_history_keys,
                AgentHistoryCommitStatus::Discarded,
            )?;
        }

        Event::ProcessInstanceBusinessIdAssigned {
            instance_key,
            business_id,
        } => {
            // Camunda 8.10 business-id assignment on job completion: the row's
            // `business_id` is the single source every derived `businessId`
            // (instance, and artifacts created afterwards) reads from.
            tx.cexecute(
                "UPDATE process_instances SET business_id = ?2 WHERE key = ?1",
                params![*instance_key as i64, business_id],
            )?;
        }

        // Events with no queryable read-model projection. Listed explicitly
        // (no `_` catch-all) so a NEW `Event` variant fails to compile here
        // until someone decides whether the read model must project it — a
        // wildcard silently dropped new variants from every query surface.
        Event::AdHocActivated { .. }
        | Event::AdHocCompleted { .. }
        | Event::AdHocCompletionConditionFulfilled { .. }
        | Event::AdHocIterated { .. }
        | Event::AdHocToolActivated { .. }
        | Event::AdHocToolCompleted { .. }
        | Event::AgentHistoryDeduplicated { .. }
        | Event::CompensationHandlerCompleted { .. }
        | Event::CompensationSubscriptionCreated { .. }
        | Event::CompensationTriggered { .. }
        | Event::DeploymentCreated { .. }
        | Event::ElementCompleting { .. }
        | Event::MessagePublished { .. }
        | Event::MessageStartSubscriptionCreated { .. }
        | Event::MessageSubscriptionClosing { .. }
        | Event::MessageSubscriptionOpening { .. }
        | Event::MultiInstanceActivated { .. }
        | Event::MultiInstanceChildActivated { .. }
        | Event::MultiInstanceChildCompleted { .. }
        | Event::MultiInstanceCompleted { .. }
        | Event::ParallelJoinFired { .. }
        | Event::ParallelJoinOpened { .. }
        | Event::ParallelJoinReset { .. }
        | Event::ParallelJoinTokenArrived { .. }
        | Event::ProcessInstanceTerminating { .. }
        | Event::ProcessStartTimerArmed { .. }
        | Event::ProcessStartTimerFired { .. }
        | Event::ScopedCompensationCleared { .. }
        | Event::SequenceFlowTaken { .. }
        | Event::SignalBroadcast { .. }
        | Event::StartInstanceDispatched { .. }
        | Event::TaskListenerJobCreated { .. }
        | Event::UserTaskCorrectionsApplied { .. }
        | Event::UserTaskTransitionDeferred { .. }
        | Event::UserTaskTransitionResolved { .. }
        | Event::VariableScopeCreated { .. }
        | Event::VariableScopeDestroyed { .. } => {}
    }
    Ok(delta)
}

// ---------------------------------------------------------------------------
// AgentInstance / AgentHistory read model (Camunda 8.10 parity, slice S4)
// ---------------------------------------------------------------------------
//
// The engine is the system-of-record for AgentInstance state; here we project
// its `AgentInstanceCreated` / `AgentHistory{Created,Committed,Discarded}` record
// streams into two denormalized tables so the gateway REST layer (S5) can search
// them by filter and sort. Both projections are idempotent/replay-safe: the
// instance is a full-record UPSERT (a re-delivered `CREATED` — or a future
// `UPDATED`/`COMPLETED` reusing [`project_agent_instance`] — refreshes the same
// row), and a history turn is an append-only insert whose only post-insert
// mutation is the PENDING -> COMMITTED / PENDING -> DISCARDED commit-status
// transition (immutable once it leaves PENDING).

/// The `SELECT` column list for [`AgentInstanceRow`], single-sourced so the
/// full-scan search and the by-key lookup can never drift.
const AGENT_INSTANCE_COLS: &str = "agent_instance_key, agent_definition_key, element_instance_key, \
     element_id, process_instance_key, root_process_instance_key, process_definition_key, \
     process_definition_id, process_definition_version, tenant_id, status, agent_type, model, \
     provider, system_prompt, max_tokens, max_model_calls, max_tool_calls, input_tokens, \
     output_tokens, reasoning_token_count, cache_creation_token_count, cache_read_token_count, \
     model_calls, tool_calls, job_key, tools_json, creation_date_ms, last_updated_date_ms, \
     completion_date_ms, process_definition_version_tag, element_instance_keys_json, CAST(job_lease AS TEXT), system_prompt_json";

/// The `SELECT` column list for [`AgentHistoryRow`], single-sourced.
const AGENT_HISTORY_COLS: &str = "agent_history_key, agent_instance_key, element_instance_key, \
     process_instance_key, root_process_instance_key, process_definition_key, \
     process_definition_id, tenant_id, job_key, loop_iteration, role, produced_at_ms, \
     content_json, system_prompt, tool_calls_json, input_tokens, output_tokens, \
     reasoning_token_count, cache_creation_token_count, cache_read_token_count, duration_ms, \
     history_item_id, tools_json, model, provider, is_duplicate, commit_status, \
     CAST(job_lease AS TEXT), limits_json, metrics_json, system_prompt_json";

/// Full-record UPSERT of an [`AgentInstance`] into the `agent_instances` table.
/// Keyed by `agent_instance_key`; on conflict the mutable state (status, metrics,
/// tools, job key, last-updated / completion timestamps) is refreshed while the
/// creation identity is preserved — so a re-delivered `CREATED` is idempotent and
/// a later `UPDATED`/`COMPLETED` event can reuse this same projection unchanged.
fn project_agent_instance(tx: &rusqlite::Transaction, ai: &AgentInstance) -> rusqlite::Result<()> {
    let tools_json = serde_json::to_string(&ai.tools).unwrap_or_else(|_| "[]".to_string());
    let element_instance_keys_json =
        serde_json::to_string(&ai.element_instance_keys).unwrap_or_else(|_| "[]".to_string());
    // `0` in the engine means "not completed yet"; store it as SQL NULL so the
    // completionDate filter/sort distinguishes live from completed instances.
    let completion: Option<i64> = if ai.completed_at == 0 {
        None
    } else {
        Some(ai.completed_at as i64)
    };
    tx.cexecute(
        "INSERT INTO agent_instances (\
             agent_instance_key, agent_definition_key, element_instance_key, element_id, \
             process_instance_key, root_process_instance_key, process_definition_key, \
             process_definition_id, process_definition_version, tenant_id, status, agent_type, \
             model, provider, system_prompt, max_tokens, max_model_calls, max_tool_calls, \
             input_tokens, output_tokens, reasoning_token_count, cache_creation_token_count, \
             cache_read_token_count, model_calls, tool_calls, job_key, tools_json, \
             creation_date_ms, last_updated_date_ms, completion_date_ms, \
             process_definition_version_tag, element_instance_keys_json, job_lease, system_prompt_json) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, \
             ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25, ?26, ?27, ?28, ?29, ?30, ?31, ?32, ?33, ?34) \
         ON CONFLICT(agent_instance_key) DO UPDATE SET \
             status = excluded.status, model = excluded.model, provider = excluded.provider, \
             system_prompt = excluded.system_prompt, system_prompt_json = excluded.system_prompt_json, max_tokens = excluded.max_tokens, \
             max_model_calls = excluded.max_model_calls, max_tool_calls = excluded.max_tool_calls, \
             input_tokens = excluded.input_tokens, output_tokens = excluded.output_tokens, \
             reasoning_token_count = excluded.reasoning_token_count, \
             cache_creation_token_count = excluded.cache_creation_token_count, \
             cache_read_token_count = excluded.cache_read_token_count, \
             model_calls = excluded.model_calls, tool_calls = excluded.tool_calls, \
             job_key = excluded.job_key, tools_json = excluded.tools_json, \
             last_updated_date_ms = excluded.last_updated_date_ms, \
             completion_date_ms = excluded.completion_date_ms, \
             process_definition_version_tag = excluded.process_definition_version_tag, \
             element_instance_keys_json = excluded.element_instance_keys_json, \
             element_instance_key = excluded.element_instance_key, job_lease = excluded.job_lease",
        params![
            ai.agent_instance_key as i64,
            ai.agent_definition_key as i64,
            ai.element_instance_key as i64,
            ai.element_id,
            ai.process_instance_key as i64,
            ai.root_process_instance_key as i64,
            ai.process_definition_key as i64,
            ai.bpmn_process_id,
            ai.process_definition_version,
            ai.tenant_id,
            ai.status.as_str(),
            ai.agent_type.as_str(),
            ai.definition.model.as_ref(),
            ai.definition.provider.as_ref(),
            rusqlite::types::Null,
            ai.limits.max_tokens,
            ai.limits.max_model_calls,
            ai.limits.max_tool_calls,
            ai.metrics.input_tokens,
            ai.metrics.output_tokens,
            ai.metrics.reasoning_token_count,
            ai.metrics.cache_creation_token_count,
            ai.metrics.cache_read_token_count,
            ai.metrics.model_calls,
            ai.metrics.tool_calls,
            ai.job_key as i64,
            tools_json,
            ai.created_at as i64,
            ai.last_updated_at as i64,
            completion,
            ai.process_definition_version_tag.as_ref(),
            element_instance_keys_json,
            ai.job_lease,
            ai.definition.system_prompt.as_ref().map(serde_json::to_string).transpose()
                .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?,
        ],
    )?;
    Ok(())
}

/// Append-only insert of one materialised [`AgentHistoryRecord`] turn into the
/// `agent_history` table. `ON CONFLICT(agent_history_key) DO NOTHING`: a turn is
/// immutable once recorded, and a later commit/discard transition (applied by
/// [`transition_agent_history`]) must survive a re-delivered `CREATED`.
fn project_agent_history_record(
    tx: &rusqlite::Transaction,
    r: &AgentHistoryRecord,
) -> rusqlite::Result<()> {
    let content_json = serde_json::to_string(&r.content).unwrap_or_else(|_| "[]".to_string());
    let tool_calls_json = serde_json::to_string(&r.tool_calls).unwrap_or_else(|_| "[]".to_string());
    let tools_json = serde_json::to_string(&r.tools).unwrap_or_else(|_| "[]".to_string());
    tx.cexecute(
        "INSERT INTO agent_history (\
             agent_history_key, agent_instance_key, element_instance_key, process_instance_key, \
             root_process_instance_key, process_definition_key, process_definition_id, tenant_id, \
             job_key, loop_iteration, role, produced_at_ms, content_json, system_prompt, \
             tool_calls_json, input_tokens, output_tokens, reasoning_token_count, \
             cache_creation_token_count, cache_read_token_count, duration_ms, history_item_id, \
             tools_json, model, provider, is_duplicate, commit_status, job_lease, limits_json, metrics_json, system_prompt_json) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, \
             ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25, ?26, ?27, ?28, ?29, ?30, ?31) \
         ON CONFLICT(agent_history_key) DO NOTHING",
        params![
            r.agent_history_key as i64,
            r.agent_instance_key as i64,
            r.element_instance_key as i64,
            r.process_instance_key as i64,
            r.root_process_instance_key as i64,
            r.process_definition_key as i64,
            r.bpmn_process_id,
            r.tenant_id,
            r.job_key as i64,
            r.loop_iteration,
            r.role.as_str(),
            r.produced_at as i64,
            content_json,
            rusqlite::types::Null,
            tool_calls_json,
            r.metrics.as_ref().and_then(|m| m.input_tokens),
            r.metrics.as_ref().and_then(|m| m.output_tokens),
            r.metrics.as_ref().and_then(|m| m.reasoning_token_count),
            r.metrics.as_ref().and_then(|m| m.cache_creation_token_count),
            r.metrics.as_ref().and_then(|m| m.cache_read_token_count),
            r.metrics.as_ref().and_then(|m| m.duration_ms),
            r.history_item_id.as_ref(),
            tools_json,
            r.model.as_ref(),
            r.provider.as_ref(),
            i64::from(r.is_duplicate),
            r.commit_status.as_str(),
            r.job_lease,
            r.limits.as_ref().map(serde_json::to_string).transpose().unwrap(),
            serde_json::to_string(&r.metrics).unwrap(),
            r.system_prompt.as_ref().map(serde_json::to_string).transpose()
                .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?,
        ],
    )?;
    Ok(())
}

/// Apply a PENDING -> `target` commit-status transition to the named turns of an
/// agent instance. `AND commit_status = 'PENDING'` enforces immutability: only a
/// still-pending turn transitions, so a re-delivered COMMITTED/DISCARDED event is
/// a no-op and a committed turn can never be discarded (or vice versa).
fn transition_agent_history(
    tx: &rusqlite::Transaction,
    agent_instance_key: Key,
    agent_history_keys: &[Key],
    target: AgentHistoryCommitStatus,
) -> rusqlite::Result<()> {
    if agent_history_keys.is_empty() {
        return Ok(());
    }
    let placeholders = vec!["?"; agent_history_keys.len()].join(", ");
    let sql = format!(
        "UPDATE agent_history SET commit_status = ?1 \
         WHERE agent_instance_key = ?2 AND commit_status = 'PENDING' \
           AND agent_history_key IN ({placeholders})",
    );
    let mut values: Vec<rusqlite::types::Value> = Vec::with_capacity(agent_history_keys.len() + 2);
    values.push(rusqlite::types::Value::Text(target.as_str().to_string()));
    values.push(rusqlite::types::Value::Integer(agent_instance_key as i64));
    for k in agent_history_keys {
        values.push(rusqlite::types::Value::Integer(*k as i64));
    }
    tx.execute(&sql, rusqlite::params_from_iter(values))?;
    Ok(())
}

/// Sort direction for the agent search surfaces.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SortOrder {
    Asc,
    Desc,
}

impl SortOrder {
    /// The SQL keyword — a fixed string literal, never user data (no injection).
    fn keyword(self) -> &'static str {
        match self {
            SortOrder::Asc => "ASC",
            SortOrder::Desc => "DESC",
        }
    }
}

/// A sortable AgentInstance field (the 8.10 `AgentInstanceSearchQuerySortRequest`
/// keys). Each maps to a fixed column name — never a user-supplied string — so
/// splicing it into `ORDER BY` carries no injection surface.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AgentInstanceSortField {
    AgentInstanceKey,
    AgentDefinitionKey,
    Status,
    ElementId,
    ProcessInstanceKey,
    RootProcessInstanceKey,
    ProcessDefinitionKey,
    TenantId,
    CreationDate,
    LastUpdatedDate,
    CompletionDate,
}

impl AgentInstanceSortField {
    fn column(self) -> &'static str {
        match self {
            AgentInstanceSortField::AgentInstanceKey => "agent_instance_key",
            AgentInstanceSortField::AgentDefinitionKey => "agent_definition_key",
            AgentInstanceSortField::Status => "status",
            AgentInstanceSortField::ElementId => "element_id",
            AgentInstanceSortField::ProcessInstanceKey => "process_instance_key",
            AgentInstanceSortField::RootProcessInstanceKey => "root_process_instance_key",
            AgentInstanceSortField::ProcessDefinitionKey => "process_definition_key",
            AgentInstanceSortField::TenantId => "tenant_id",
            AgentInstanceSortField::CreationDate => "creation_date_ms",
            AgentInstanceSortField::LastUpdatedDate => "last_updated_date_ms",
            AgentInstanceSortField::CompletionDate => "completion_date_ms",
        }
    }
}

/// Filter for [`ReadStore::agent_instances`]. Every dimension is optional; a
/// `None` field imposes no constraint. Mirrors the 8.10 filterable fields.
#[derive(Clone, Debug, Default)]
pub struct AgentInstanceFilter {
    pub agent_instance_key: Option<Key>,
    pub agent_definition_key: Option<Key>,
    pub status: Option<AgentInstanceStatus>,
    pub element_id: Option<String>,
    pub process_instance_key: Option<Key>,
    pub root_process_instance_key: Option<Key>,
    pub process_definition_key: Option<Key>,
    pub tenant_id: Option<String>,
    pub process_definition_id: Option<String>,
    pub process_definition_version: Option<i32>,
    pub process_definition_version_tag: Option<String>,
    pub element_instance_keys: Vec<Key>,
    pub creation_date_ms: Option<u64>,
    pub last_updated_date_ms: Option<u64>,
    pub completion_date_ms: Option<u64>,
}

impl AgentInstanceFilter {
    /// Builds the `WHERE …` clause (empty when unfiltered) plus the bound values.
    /// Values are always **bound parameters**, never interpolated, so no
    /// user-supplied string reaches the SQL text.
    fn where_clause(&self) -> (String, Vec<rusqlite::types::Value>) {
        use rusqlite::types::Value;
        let mut clauses: Vec<String> = Vec::new();
        let mut values: Vec<Value> = Vec::new();
        let push_int = |clauses: &mut Vec<String>, values: &mut Vec<Value>, col: &str, v: i64| {
            clauses.push(format!("{col} = ?{}", values.len() + 1));
            values.push(Value::Integer(v));
        };
        if let Some(v) = self.agent_instance_key {
            push_int(&mut clauses, &mut values, "agent_instance_key", v as i64);
        }
        if let Some(v) = self.agent_definition_key {
            push_int(&mut clauses, &mut values, "agent_definition_key", v as i64);
        }
        if let Some(v) = self.process_instance_key {
            push_int(&mut clauses, &mut values, "process_instance_key", v as i64);
        }
        if let Some(v) = self.root_process_instance_key {
            push_int(
                &mut clauses,
                &mut values,
                "root_process_instance_key",
                v as i64,
            );
        }
        if let Some(v) = self.process_definition_key {
            push_int(
                &mut clauses,
                &mut values,
                "process_definition_key",
                v as i64,
            );
        }
        if let Some(status) = self.status {
            clauses.push(format!("status = ?{}", values.len() + 1));
            values.push(Value::Text(status.as_str().to_string()));
        }
        if let Some(element_id) = &self.element_id {
            clauses.push(format!("element_id = ?{}", values.len() + 1));
            values.push(Value::Text(element_id.clone()));
        }
        if let Some(tenant_id) = &self.tenant_id {
            clauses.push(format!("tenant_id = ?{}", values.len() + 1));
            values.push(Value::Text(tenant_id.clone()));
        }
        for (col, value) in [
            ("process_definition_id", &self.process_definition_id),
            (
                "process_definition_version_tag",
                &self.process_definition_version_tag,
            ),
        ] {
            if let Some(value) = value {
                clauses.push(format!("{col} = ?{}", values.len() + 1));
                values.push(Value::Text(value.clone()));
            }
        }
        for (col, value) in [
            (
                "process_definition_version",
                self.process_definition_version.map(i64::from),
            ),
            ("creation_date_ms", self.creation_date_ms.map(|v| v as i64)),
            (
                "last_updated_date_ms",
                self.last_updated_date_ms.map(|v| v as i64),
            ),
            (
                "completion_date_ms",
                self.completion_date_ms.map(|v| v as i64),
            ),
        ] {
            if let Some(value) = value {
                push_int(&mut clauses, &mut values, col, value);
            }
        }
        for key in &self.element_instance_keys {
            clauses.push(format!(
                "EXISTS (SELECT 1 FROM json_each(element_instance_keys_json) WHERE value = ?{})",
                values.len() + 1,
            ));
            values.push(Value::Integer(*key as i64));
        }
        let where_sql = if clauses.is_empty() {
            String::new()
        } else {
            format!(" WHERE {}", clauses.join(" AND "))
        };
        (where_sql, values)
    }
}

/// A sortable AgentHistory field (the 8.10 history-search sort keys:
/// `producedAt`, `historyItemKey`, `loopIteration`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AgentHistorySortField {
    ProducedAt,
    HistoryItemKey,
    LoopIteration,
}

impl AgentHistorySortField {
    fn column(self) -> &'static str {
        match self {
            AgentHistorySortField::ProducedAt => "produced_at_ms",
            AgentHistorySortField::HistoryItemKey => "agent_history_key",
            AgentHistorySortField::LoopIteration => "loop_iteration",
        }
    }
}

/// Filter for [`ReadStore::agent_history`]. `commit_status` carries the derived
/// commit-status filter: **`None` means the default — COMMITTED only** — so
/// PENDING/DISCARDED turns surface only when a caller asks for them explicitly.
#[derive(Clone, Debug, Default)]
pub struct AgentHistoryFilter {
    pub agent_instance_key: Option<Key>,
    pub process_instance_key: Option<Key>,
    pub history_item_key: Option<Key>,
    pub element_instance_key: Option<Key>,
    pub job_key: Option<Key>,
    pub role: Option<AgentHistoryRole>,
    pub loop_iteration: Option<i32>,
    pub produced_at_ms: Option<u64>,
    /// The commit statuses to include. `None` (the default) restricts the result
    /// to `COMMITTED`; `Some(list)` returns exactly the listed statuses (an empty
    /// list is treated as the COMMITTED default rather than "match nothing").
    pub commit_status: Option<Vec<AgentHistoryCommitStatus>>,
}

impl AgentHistoryFilter {
    fn where_clause(&self) -> (String, Vec<rusqlite::types::Value>) {
        use rusqlite::types::Value;
        let mut clauses: Vec<String> = Vec::new();
        let mut values: Vec<Value> = Vec::new();
        if let Some(v) = self.agent_instance_key {
            clauses.push(format!("agent_instance_key = ?{}", values.len() + 1));
            values.push(Value::Integer(v as i64));
        }
        if let Some(v) = self.process_instance_key {
            clauses.push(format!("process_instance_key = ?{}", values.len() + 1));
            values.push(Value::Integer(v as i64));
        }
        for (col, value) in [
            ("agent_history_key", self.history_item_key.map(|v| v as i64)),
            (
                "element_instance_key",
                self.element_instance_key.map(|v| v as i64),
            ),
            ("job_key", self.job_key.map(|v| v as i64)),
            ("loop_iteration", self.loop_iteration.map(i64::from)),
            ("produced_at_ms", self.produced_at_ms.map(|v| v as i64)),
        ] {
            if let Some(value) = value {
                clauses.push(format!("{col} = ?{}", values.len() + 1));
                values.push(Value::Integer(value));
            }
        }
        if let Some(role) = self.role {
            clauses.push(format!("role = ?{}", values.len() + 1));
            values.push(Value::Text(role.as_str().into()));
        }
        // Default filter = COMMITTED (an omitted or empty commit_status).
        let statuses: Vec<AgentHistoryCommitStatus> = match &self.commit_status {
            Some(list) if !list.is_empty() => list.clone(),
            _ => vec![AgentHistoryCommitStatus::Committed],
        };
        let placeholders: Vec<String> = statuses
            .iter()
            .map(|s| {
                values.push(Value::Text(s.as_str().to_string()));
                format!("?{}", values.len())
            })
            .collect();
        clauses.push(format!("commit_status IN ({})", placeholders.join(", ")));
        (format!(" WHERE {}", clauses.join(" AND ")), values)
    }
}

/// A projected AgentInstance row (the read-model shape the 8.10
/// `/v2/agent-instances` REST layer serves).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AgentInstanceRow {
    pub agent_instance_key: Key,
    pub agent_definition_key: Key,
    pub element_instance_key: Key,
    pub element_id: String,
    pub process_instance_key: Key,
    pub root_process_instance_key: Key,
    pub process_definition_key: Key,
    pub process_definition_id: String,
    pub process_definition_version: i32,
    pub tenant_id: String,
    pub status: AgentInstanceStatus,
    pub agent_type: String,
    pub model: Option<String>,
    pub provider: Option<String>,
    pub system_prompt: Option<Vec<AgentHistoryContent>>,
    pub max_tokens: i64,
    pub max_model_calls: i64,
    pub max_tool_calls: i64,
    pub input_tokens: i64,
    pub output_tokens: i64,
    pub reasoning_token_count: i64,
    pub cache_creation_token_count: i64,
    pub cache_read_token_count: i64,
    pub model_calls: i64,
    pub tool_calls: i64,
    pub job_key: Key,
    /// The tools available to the agent, as a JSON array string (as projected).
    pub tools_json: String,
    pub creation_date_ms: u64,
    pub last_updated_date_ms: u64,
    /// The completion instant (ms), `None` until the instance is COMPLETED.
    pub completion_date_ms: Option<u64>,
    /// The process definition version tag, if any.
    pub process_definition_version_tag: Option<String>,
    /// Every element instance associated with this agent instance (the owning
    /// `element_instance_key` is always the first). Projected as a JSON array.
    pub element_instance_keys: Vec<Key>,
    pub job_lease: String,
}

/// A projected AgentHistory turn row.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AgentHistoryRow {
    pub agent_history_key: Key,
    pub agent_instance_key: Key,
    pub element_instance_key: Key,
    pub process_instance_key: Key,
    pub root_process_instance_key: Key,
    pub process_definition_key: Key,
    pub process_definition_id: String,
    pub tenant_id: String,
    pub job_key: Key,
    pub loop_iteration: i32,
    pub role: AgentHistoryRole,
    pub produced_at_ms: u64,
    pub content_json: String,
    pub system_prompt: Option<Vec<AgentHistoryContent>>,
    pub tool_calls_json: String,
    pub input_tokens: Option<i64>,
    pub output_tokens: Option<i64>,
    pub reasoning_token_count: Option<i64>,
    pub cache_creation_token_count: Option<i64>,
    pub cache_read_token_count: Option<i64>,
    pub duration_ms: Option<i64>,
    pub history_item_id: Option<String>,
    pub tools_json: String,
    pub model: Option<String>,
    pub provider: Option<String>,
    pub is_duplicate: bool,
    pub commit_status: AgentHistoryCommitStatus,
    pub job_lease: String,
    pub limits_json: Option<String>,
    pub metrics_json: Option<String>,
}

/// Parse a stored `AgentInstanceStatus` label back to the enum. An unrecognised
/// value (only possible if a record predates a status) degrades to
/// `INITIALIZING` rather than erroring the read.
fn agent_status_from_label(s: &str) -> AgentInstanceStatus {
    match s {
        "TOOL_DISCOVERY" => AgentInstanceStatus::ToolDiscovery,
        "THINKING" => AgentInstanceStatus::Thinking,
        "TOOL_CALLING" => AgentInstanceStatus::ToolCalling,
        "IDLE" => AgentInstanceStatus::Idle,
        "COMPLETED" => AgentInstanceStatus::Completed,
        _ => AgentInstanceStatus::Initializing,
    }
}

fn agent_role_from_label(s: &str) -> AgentHistoryRole {
    match s {
        "ASSISTANT" => AgentHistoryRole::Assistant,
        "TOOL_RESULT" => AgentHistoryRole::ToolResult,
        "CONFIGURATION" => AgentHistoryRole::Configuration,
        _ => AgentHistoryRole::User,
    }
}

fn commit_status_from_label(s: &str) -> AgentHistoryCommitStatus {
    match s {
        "PENDING" => AgentHistoryCommitStatus::Pending,
        "DISCARDED" => AgentHistoryCommitStatus::Discarded,
        _ => AgentHistoryCommitStatus::Committed,
    }
}

fn map_agent_instance(r: &rusqlite::Row) -> rusqlite::Result<AgentInstanceRow> {
    Ok(AgentInstanceRow {
        agent_instance_key: r.get::<_, i64>(0)? as Key,
        agent_definition_key: r.get::<_, i64>(1)? as Key,
        element_instance_key: r.get::<_, i64>(2)? as Key,
        element_id: r.get(3)?,
        process_instance_key: r.get::<_, i64>(4)? as Key,
        root_process_instance_key: r.get::<_, i64>(5)? as Key,
        process_definition_key: r.get::<_, i64>(6)? as Key,
        process_definition_id: r.get(7)?,
        process_definition_version: r.get(8)?,
        tenant_id: r.get(9)?,
        status: agent_status_from_label(&r.get::<_, String>(10)?),
        agent_type: r.get(11)?,
        model: r.get(12)?,
        provider: r.get(13)?,
        system_prompt: read_agent_prompt(r, 14, 33)?,
        max_tokens: r.get(15)?,
        max_model_calls: r.get(16)?,
        max_tool_calls: r.get(17)?,
        input_tokens: r.get(18)?,
        output_tokens: r.get(19)?,
        reasoning_token_count: r.get(20)?,
        cache_creation_token_count: r.get(21)?,
        cache_read_token_count: r.get(22)?,
        model_calls: r.get(23)?,
        tool_calls: r.get(24)?,
        job_key: r.get::<_, i64>(25)? as Key,
        tools_json: r.get(26)?,
        creation_date_ms: r.get::<_, i64>(27)? as u64,
        last_updated_date_ms: r.get::<_, i64>(28)? as u64,
        completion_date_ms: r.get::<_, Option<i64>>(29)?.map(|v| v as u64),
        process_definition_version_tag: r.get(30)?,
        element_instance_keys: {
            let json: String = r.get(31)?;
            serde_json::from_str::<Vec<i64>>(&json)
                .map(|v| v.into_iter().map(|k| k as Key).collect())
                .unwrap_or_default()
        },
        job_lease: r.get(32)?,
    })
}

fn map_agent_history(r: &rusqlite::Row) -> rusqlite::Result<AgentHistoryRow> {
    Ok(AgentHistoryRow {
        agent_history_key: r.get::<_, i64>(0)? as Key,
        agent_instance_key: r.get::<_, i64>(1)? as Key,
        element_instance_key: r.get::<_, i64>(2)? as Key,
        process_instance_key: r.get::<_, i64>(3)? as Key,
        root_process_instance_key: r.get::<_, i64>(4)? as Key,
        process_definition_key: r.get::<_, i64>(5)? as Key,
        process_definition_id: r.get(6)?,
        tenant_id: r.get(7)?,
        job_key: r.get::<_, i64>(8)? as Key,
        loop_iteration: r.get(9)?,
        role: agent_role_from_label(&r.get::<_, String>(10)?),
        produced_at_ms: r.get::<_, i64>(11)? as u64,
        content_json: r.get(12)?,
        system_prompt: read_agent_prompt(r, 13, 30)?,
        tool_calls_json: r.get(14)?,
        input_tokens: r.get(15)?,
        output_tokens: r.get(16)?,
        reasoning_token_count: r.get(17)?,
        cache_creation_token_count: r.get(18)?,
        cache_read_token_count: r.get(19)?,
        duration_ms: r.get(20)?,
        history_item_id: r.get(21)?,
        tools_json: r.get(22)?,
        model: r.get(23)?,
        provider: r.get(24)?,
        is_duplicate: r.get::<_, i64>(25)? != 0,
        commit_status: commit_status_from_label(&r.get::<_, String>(26)?),
        job_lease: r.get(27)?,
        limits_json: r.get(28)?,
        metrics_json: match r.get::<_, Option<String>>(29)? {
            Some(json) => Some(json),
            None => Some(
                serde_json::to_string(&nanobpmn_engine_core::AgentHistoryMetrics {
                    input_tokens: r.get(15)?,
                    output_tokens: r.get(16)?,
                    reasoning_token_count: r.get(17)?,
                    cache_creation_token_count: r.get(18)?,
                    cache_read_token_count: r.get(19)?,
                    duration_ms: r.get(20)?,
                })
                .map_err(|e| {
                    rusqlite::Error::FromSqlConversionFailure(
                        29,
                        rusqlite::types::Type::Text,
                        Box::new(e),
                    )
                })?,
            ),
        },
    })
}

fn read_agent_prompt(
    row: &rusqlite::Row,
    legacy_column: usize,
    canonical_column: usize,
) -> rusqlite::Result<Option<Vec<AgentHistoryContent>>> {
    if let Some(json) = row.get::<_, Option<String>>(canonical_column)? {
        return serde_json::from_str(&json).map(Some).map_err(|e| {
            rusqlite::Error::FromSqlConversionFailure(
                canonical_column,
                rusqlite::types::Type::Text,
                Box::new(e),
            )
        });
    }
    // The old column is explicitly plain text, even when it looks like JSON.
    Ok(row.get::<_, Option<String>>(legacy_column)?.map(|text| {
        vec![AgentHistoryContent {
            content_type: AgentHistoryContentType::Text,
            text: Some(text),
            document_reference: None,
            object: None,
        }]
    }))
}

impl ReadStore {
    /// Search AgentInstance rows by `filter`, ordered by `sort` (field +
    /// direction), or by `agent_instance_key ASC` when `sort` is `None`. The
    /// order field maps to a fixed column and the direction to a fixed keyword,
    /// so the only interpolation is compile-time-known text; all filter values
    /// are bound parameters.
    pub fn agent_instances(
        &self,
        filter: &AgentInstanceFilter,
        sort: Option<(AgentInstanceSortField, SortOrder)>,
    ) -> Vec<AgentInstanceRow> {
        self.try_agent_instances(filter, sort)
            .expect("query agent_instances")
    }

    /// Search agents without hiding malformed persisted projections.
    pub fn try_agent_instances(
        &self,
        filter: &AgentInstanceFilter,
        sort: Option<(AgentInstanceSortField, SortOrder)>,
    ) -> rusqlite::Result<Vec<AgentInstanceRow>> {
        let conn = self.conn.lock().expect("read store poisoned");
        let (where_sql, values) = filter.where_clause();
        let (col, dir) = match sort {
            Some((field, order)) => (field.column(), order.keyword()),
            None => ("agent_instance_key", "ASC"),
        };
        let sql = format!(
            "SELECT {AGENT_INSTANCE_COLS} FROM agent_instances{where_sql} \
             ORDER BY {col} {dir}, agent_instance_key ASC"
        );
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(rusqlite::params_from_iter(values), map_agent_instance)?;
        rows.collect()
    }

    /// A single AgentInstance by its dedicated key.
    pub fn agent_instance(&self, key: Key) -> Option<AgentInstanceRow> {
        self.try_agent_instance(key).expect("query agent_instance")
    }

    /// Look up an agent, distinguishing absence from corrupt persisted data.
    pub fn try_agent_instance(&self, key: Key) -> rusqlite::Result<Option<AgentInstanceRow>> {
        let conn = self.conn.lock().expect("read store poisoned");
        conn.query_row(
            &format!(
                "SELECT {AGENT_INSTANCE_COLS} FROM agent_instances WHERE agent_instance_key = ?1"
            ),
            params![key as i64],
            map_agent_instance,
        )
        .optional()
    }

    /// Search AgentHistory turns by `filter`, ordered by `sort`, or by the
    /// engine's canonical `(loop_iteration, produced_at_ms, agent_history_key)`
    /// order when `sort` is `None`. With no `commit_status` in the filter only
    /// COMMITTED turns are returned (see [`AgentHistoryFilter`]).
    pub fn agent_history(
        &self,
        filter: &AgentHistoryFilter,
        sort: Option<(AgentHistorySortField, SortOrder)>,
    ) -> Vec<AgentHistoryRow> {
        self.try_agent_history(filter, sort)
            .expect("query agent_history")
    }

    /// Search history without silently dropping malformed rows.
    pub fn try_agent_history(
        &self,
        filter: &AgentHistoryFilter,
        sort: Option<(AgentHistorySortField, SortOrder)>,
    ) -> rusqlite::Result<Vec<AgentHistoryRow>> {
        let conn = self.conn.lock().expect("read store poisoned");
        let (where_sql, values) = filter.where_clause();
        let order_sql = match sort {
            Some((field, order)) => {
                format!(
                    "{} {}, agent_history_key ASC",
                    field.column(),
                    order.keyword()
                )
            }
            None => "loop_iteration ASC, produced_at_ms ASC, agent_history_key ASC".to_string(),
        };
        let sql = format!(
            "SELECT {AGENT_HISTORY_COLS} FROM agent_history{where_sql} ORDER BY {order_sql}"
        );
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(rusqlite::params_from_iter(values), map_agent_history)?;
        rows.collect()
    }
}

#[cfg(test)]
mod writability_tests {
    use std::sync::atomic::{AtomicU32, Ordering};

    use super::ReadStore;

    static COUNTER: AtomicU32 = AtomicU32::new(0);

    fn scratch_db() -> std::path::PathBuf {
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "nanobpm-readstore-{}-{}.sqlite",
            std::process::id(),
            n
        ))
    }

    #[test]
    fn open_succeeds_on_a_writable_db() {
        let path = scratch_db();
        ReadStore::open(Some(&path)).expect("fresh writable db opens");
        // Re-open (schema already matches) still validates writability.
        ReadStore::open(Some(&path)).expect("existing writable db re-opens");
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn file_backed_store_runs_in_wal_mode() {
        // The read model is a derived projection rebuilt from the journal, so it
        // runs WAL + synchronous=NORMAL to keep the exporter's per-commit fsync off
        // the projection hot path. Lock that in so a future change can't silently
        // revert to the default (DELETE journal + synchronous=FULL) durability.
        let path = scratch_db();
        let store = ReadStore::open(Some(&path)).expect("open file-backed db");
        let (mode, sync): (String, i64) = {
            let conn = store.conn.lock().unwrap();
            let mode = conn
                .query_row("PRAGMA journal_mode", [], |r| r.get::<_, String>(0))
                .unwrap();
            let sync = conn
                .query_row("PRAGMA synchronous", [], |r| r.get::<_, i64>(0))
                .unwrap();
            (mode, sync)
        };
        assert_eq!(
            mode.to_lowercase(),
            "wal",
            "read store must use WAL journal"
        );
        assert_eq!(sync, 1, "read store must use synchronous=NORMAL (1)");
        drop(store);
        std::fs::remove_file(&path).ok();
        std::fs::remove_file(path.with_extension("sqlite-wal")).ok();
        std::fs::remove_file(path.with_extension("sqlite-shm")).ok();
    }

    #[test]
    fn archive_attaches_with_power_safe_full_synchronous() {
        // Defect-class guard (issue #831): the terminal-audit archive is the
        // durable source of truth for completed history and — unlike the derived
        // read model — is NOT rebuildable from the journal once an instance is
        // evicted below the snapshot floor. It must therefore NOT silently inherit
        // the read model's throughput-tuned synchronous=NORMAL (which can drop a
        // recently-archived completion on power/OS loss). `attach_archive` must
        // give the attached archive its own power-safe FULL profile regardless of
        // the main connection's NORMAL.
        let path = scratch_db();
        let store = ReadStore::open(Some(&path)).expect("open file-backed db (creates archive)");
        let archive = ReadStore::terminal_archive_path(&path);
        let archive_sync: i64 = {
            let conn = store.conn.lock().unwrap();
            let main_sync = conn
                .query_row("PRAGMA synchronous", [], |r| r.get::<_, i64>(0))
                .unwrap();
            assert_eq!(
                main_sync, 1,
                "precondition: main read model runs NORMAL (1)"
            );
            super::attach_archive(&conn, &archive).expect("attach archive");
            let s = conn
                .query_row("PRAGMA terminal_archive.synchronous", [], |r| {
                    r.get::<_, i64>(0)
                })
                .unwrap();
            let _ = conn.execute_batch("DETACH DATABASE terminal_archive");
            s
        };
        assert_eq!(
            archive_sync, 2,
            "archive must attach with power-safe synchronous=FULL (2), not the read model's NORMAL (1)"
        );
        drop(store);
        let cleanup = |p: &std::path::Path| {
            std::fs::remove_file(p).ok();
            std::fs::remove_file(p.with_extension("sqlite-wal")).ok();
            std::fs::remove_file(p.with_extension("sqlite-shm")).ok();
        };
        cleanup(&path);
        cleanup(&archive);
    }

    #[cfg(unix)]
    #[test]
    fn open_fails_fast_on_a_readonly_db() {
        use std::os::unix::fs::PermissionsExt;

        let path = scratch_db();
        // Create a healthy db with the current schema, then drop the handle.
        ReadStore::open(Some(&path)).expect("seed db");
        // Make the file itself read-only: re-open finds a matching schema (so it
        // writes nothing during open) and must fail on the writability probe.
        let mut perms = std::fs::metadata(&path).unwrap().permissions();
        perms.set_mode(0o444);
        std::fs::set_permissions(&path, perms).unwrap();

        let err = match ReadStore::open(Some(&path)) {
            Ok(_) => panic!("read-only db must fail fast"),
            Err(e) => e,
        };
        assert!(
            err.to_string().to_lowercase().contains("readonly"),
            "expected a readonly error, got: {err}"
        );

        // Restore perms so cleanup can remove the file.
        let mut perms = std::fs::metadata(&path).unwrap().permissions();
        perms.set_mode(0o644);
        std::fs::set_permissions(&path, perms).unwrap();
        std::fs::remove_file(&path).ok();
    }

    // --- schema drift guards (the "derive, don't duplicate" invariant) ---------

    #[test]
    fn fnv1a_is_deterministic_and_content_sensitive() {
        use super::fnv1a_64;
        // Known FNV-1a/64 vectors (offset basis for empty input; a canonical
        // "hello" vector) pin the algorithm so a refactor can't silently change
        // the fingerprint of every existing database.
        assert_eq!(fnv1a_64(b"") as u64, 0xcbf2_9ce4_8422_2325);
        assert_eq!(fnv1a_64(b"hello") as u64, 0xa430_d846_80aa_bd0b);
        // Any change to the hashed bytes must change the digest (this is the
        // property `ensure_schema` relies on to notice a SCHEMA edit).
        assert_ne!(fnv1a_64(b"jobs(a,b)"), fnv1a_64(b"jobs(a,b,c)"));
    }

    #[test]
    fn schema_edit_requires_version_bump() {
        // CI drift guard (issue #831): the fingerprint is no longer a runtime wipe
        // trigger, but it still pins SCHEMA to a recorded value. If you edit
        // SCHEMA, this fails until you bump SCHEMA_VERSION and update
        // SCHEMA_FINGERPRINT — the "fingerprint changed => a migration was added"
        // invariant, enforced in the test suite instead of by dropping tables.
        assert_eq!(
            super::schema_fingerprint(),
            super::SCHEMA_FINGERPRINT,
            "SCHEMA changed: bump SCHEMA_VERSION (currently {}) and set \
             SCHEMA_FINGERPRINT = {}",
            super::SCHEMA_VERSION,
            super::schema_fingerprint(),
        );
    }

    #[test]
    fn additive_schema_change_preserves_rows() {
        // The core issue #831 guarantee: an additive SCHEMA change across an
        // upgrade preserves every existing read-model row (and does not reset the
        // exported_position below the compaction floor). Seed rows under a vN
        // schema, then reopen after an additive (ADD COLUMN + new TABLE) change and
        // assert nothing was dropped.
        let path = scratch_db();
        {
            let store = ReadStore::open(Some(&path)).expect("fresh open");
            let conn = store.conn.lock().unwrap();
            conn.execute(
                "INSERT INTO process_instances \
                 (key, process_id, process_definition_id, process_definition_key, \
                  version, state, start_date_ms, has_incident, tags, business_id) \
                 VALUES (7, 'p', 'p', '1', 1, 1, 0, 0, '[]', NULL)",
                [],
            )
            .unwrap();
            conn.execute("UPDATE meta SET v = 500 WHERE k = 'exported_position'", [])
                .unwrap();
        }

        // Simulate the vN+1 binary: additively evolve the live database exactly as
        // `reconcile_to_schema` would for an additive SCHEMA edit, and clear the
        // stored `schema_version` so the next open actually takes the migration
        // (reconcile) branch of `ensure_schema_on` rather than the "already
        // current" fast path — otherwise this test never exercises the invariant
        // it claims to (that an additive reconcile preserves `exported_position`
        // while stamping the new version).
        {
            let conn = rusqlite::Connection::open(&path).unwrap();
            conn.execute_batch(
                "ALTER TABLE process_instances ADD COLUMN priority INTEGER NOT NULL DEFAULT 50;
                 CREATE TABLE new_feature (id INTEGER PRIMARY KEY, note TEXT);
                 DELETE FROM meta WHERE k = 'schema_version';",
            )
            .unwrap();
        }

        // Reopen with the current binary: the additive columns/tables are kept and
        // the seeded row + cursor survive (no destructive wipe), and the reconcile
        // path re-stamps the current schema version.
        let store = ReadStore::open(Some(&path)).expect("additive reopen self-heals");
        let (count, stamped_version): (i64, i64) = {
            let conn = store.conn.lock().unwrap();
            let count = conn
                .query_row("SELECT COUNT(*) FROM process_instances", [], |r| r.get(0))
                .unwrap();
            let version = conn
                .query_row("SELECT v FROM meta WHERE k = 'schema_version'", [], |r| {
                    r.get(0)
                })
                .unwrap();
            (count, version)
        };
        assert_eq!(count, 1, "additive migration must preserve existing rows");
        assert_eq!(
            stamped_version,
            super::SCHEMA_VERSION,
            "additive reconcile must stamp the current schema version"
        );
        assert_eq!(
            store.exported_position(),
            500,
            "additive migration must NOT reset the exported_position (compaction floor)"
        );
        std::fs::remove_file(&path).ok();
        std::fs::remove_file(path.with_extension("sqlite-wal")).ok();
        std::fs::remove_file(path.with_extension("sqlite-shm")).ok();
    }

    #[test]
    fn ensure_schema_migrates_additively_without_dropping() {
        // Guard: a database missing a purely-additive column (a genuine prior-nano
        // read model) is brought up to SCHEMA by ADD COLUMN — never by dropping the
        // table (which is what silently destroyed completed history, issue #831).
        // We reproduce a jobs table lacking only the additive `listener_event_type`
        // column (which carries a DEFAULT) and assert its row survives.
        let path = scratch_db();
        {
            let store = ReadStore::open(Some(&path)).expect("fresh open");
            let conn = store.conn.lock().unwrap();
            conn.execute(
                "INSERT INTO jobs \
                 (key, instance_key, element_instance_key, element_id, job_type, \
                  state, retries, process_definition_id, process_definition_key) \
                 VALUES (11, 1, 1, 'e', 't', 0, 3, 'p', '1')",
                [],
            )
            .unwrap();
        }
        // Regress the schema to before an additive column existed, and clear the
        // version stamp so the next open runs the migration path.
        {
            let conn = rusqlite::Connection::open(&path).unwrap();
            conn.execute_batch(
                "ALTER TABLE jobs DROP COLUMN listener_event_type;
                 DELETE FROM meta WHERE k = 'schema_version';",
            )
            .unwrap();
        }
        let store = ReadStore::open(Some(&path)).expect("additive migration on open");
        let cols: Vec<String> = {
            let conn = store.conn.lock().unwrap();
            let mut stmt = conn.prepare("PRAGMA table_info(jobs)").unwrap();
            stmt.query_map([], |r| r.get::<_, String>(1))
                .unwrap()
                .collect::<rusqlite::Result<Vec<_>>>()
                .unwrap()
        };
        assert!(
            cols.iter().any(|c| c == "listener_event_type"),
            "migrated jobs table must regain listener_event_type, got {cols:?}"
        );
        let count: i64 = {
            let conn = store.conn.lock().unwrap();
            conn.query_row("SELECT COUNT(*) FROM jobs", [], |r| r.get(0))
                .unwrap()
        };
        assert_eq!(count, 1, "additive migration must preserve the jobs row");
        std::fs::remove_file(&path).ok();
        std::fs::remove_file(path.with_extension("sqlite-wal")).ok();
        std::fs::remove_file(path.with_extension("sqlite-shm")).ok();
    }

    #[test]
    fn schema_fingerprint_is_stable_within_a_build() {
        // Derived purely from SCHEMA, so it is constant across calls and never
        // hand-maintained.
        assert_eq!(super::schema_fingerprint(), super::schema_fingerprint());
    }

    #[test]
    fn incompatible_schema_is_rebuilt() {
        // Reproduces the production incident: a database left behind by an older
        // build whose `jobs` table lacks the `job_kind`/`listener_event_type`
        // columns *and* several NOT NULL columns that cannot be back-filled
        // additively. Such a genuinely-incompatible schema falls back to a
        // destructive rebuild (which #732 reprojection then recovers live data
        // for), NOT a half-shaped table that panics on the first query.
        let path = scratch_db();
        {
            let conn = rusqlite::Connection::open(&path).unwrap();
            conn.execute_batch(
                "CREATE TABLE meta (k TEXT PRIMARY KEY, v INTEGER NOT NULL);
                 INSERT INTO meta (k, v) VALUES ('schema_fingerprint', 1);
                 INSERT INTO meta (k, v) VALUES ('exported_position', 42);
                 CREATE TABLE jobs (
                     key INTEGER PRIMARY KEY,
                     job_type TEXT NOT NULL
                 );
                 INSERT INTO jobs (key, job_type) VALUES (1, 'stale');",
            )
            .unwrap();
        }

        let store = ReadStore::open(Some(&path)).expect("incompatible schema self-heals on open");

        // The rebuilt `jobs` table carries the current columns...
        let cols: Vec<String> = {
            let conn = store.conn.lock().unwrap();
            let mut stmt = conn.prepare("PRAGMA table_info(jobs)").unwrap();
            stmt.query_map([], |r| r.get::<_, String>(1))
                .unwrap()
                .collect::<rusqlite::Result<Vec<_>>>()
                .unwrap()
        };
        assert!(
            cols.iter().any(|c| c == "job_kind"),
            "rebuilt jobs table must have job_kind, got {cols:?}"
        );
        assert!(cols.iter().any(|c| c == "listener_event_type"));

        // ...the reads that used to panic on the missing column now succeed...
        assert_eq!(store.active_instance_count(), 0);
        // ...the stale projection state was reset for a clean journal re-replay...
        assert_eq!(store.exported_position(), 0);
        // ...and the stored version now equals the current one, so a second open is
        // a no-op (no rebuild).
        drop(store);
        let store2 = ReadStore::open(Some(&path)).unwrap();
        let stored: Option<i64> = {
            let conn = store2.conn.lock().unwrap();
            conn.query_row("SELECT v FROM meta WHERE k = 'schema_version'", [], |r| {
                r.get(0)
            })
            .ok()
        };
        assert_eq!(stored, Some(super::SCHEMA_VERSION));
        std::fs::remove_file(&path).ok();
        std::fs::remove_file(path.with_extension("sqlite-wal")).ok();
        std::fs::remove_file(path.with_extension("sqlite-shm")).ok();
    }

    #[test]
    fn matching_version_does_not_rebuild() {
        // When the stored version already matches, open must NOT drop the
        // projection: durable derived state (e.g. exported_position) has to
        // survive a restart, or every boot would needlessly re-replay the journal.
        let path = scratch_db();
        let store = ReadStore::open(Some(&path)).unwrap();
        {
            let conn = store.conn.lock().unwrap();
            conn.execute("UPDATE meta SET v = 99 WHERE k = 'exported_position'", [])
                .unwrap();
        }
        drop(store);

        let store2 = ReadStore::open(Some(&path)).unwrap();
        assert_eq!(
            store2.exported_position(),
            99,
            "a schema that already matches must not be rebuilt"
        );
        std::fs::remove_file(&path).ok();
        std::fs::remove_file(path.with_extension("sqlite-wal")).ok();
        std::fs::remove_file(path.with_extension("sqlite-shm")).ok();
    }
}

#[cfg(test)]
mod definition_xml_tests {
    use nanobpmn_engine_core::{Event, ProcessBuilder, ProcessDefinition};

    use super::{ReadStore, RootResolver};

    fn deployed_event(key: u64, xml: &str) -> Event {
        deployed_event_versioned("p", key, 1, xml)
    }

    fn deployed_event_versioned(process_id: &str, key: u64, version: i32, xml: &str) -> Event {
        let mut def: ProcessDefinition = ProcessBuilder::new(process_id)
            .start_event("s")
            .end_event("e")
            .connect("s", "e")
            .build()
            .unwrap();
        def.xml = xml.to_string();
        Event::ProcessDeployed {
            deployment_key: 1,
            process_definition_key: key,
            version,
            process: def,
        }
    }

    #[test]
    fn projects_and_serves_the_deployment_xml_by_key() {
        let store = ReadStore::open(None).unwrap();
        let xml = "<bpmn:definitions>…verbatim…</bpmn:definitions>";
        let event = deployed_event(42, xml);
        store.export(&[&event]).unwrap();

        assert_eq!(store.process_definition_xml(42).as_deref(), Some(xml));
        // Unknown key has no XML.
        assert_eq!(store.process_definition_xml(999), None);
    }

    #[test]
    fn programmatic_definition_has_empty_xml() {
        let store = ReadStore::open(None).unwrap();
        let event = deployed_event(7, "");
        store.export(&[&event]).unwrap();
        // Present but empty — the handler maps this to a 204, not a 404.
        assert_eq!(store.process_definition_xml(7).as_deref(), Some(""));
    }

    #[test]
    fn redeploy_retains_every_version_xml_by_key() {
        let store = ReadStore::open(None).unwrap();
        // Deploy v1 (key 6) then a new version v2 (key 297) of the same process id.
        let v1 = deployed_event_versioned("p", 6, 1, "<xml>v1</xml>");
        let v2 = deployed_event_versioned("p", 297, 2, "<xml>v2</xml>");
        store.export(&[&v1]).unwrap();
        store.export(&[&v2]).unwrap();

        // Both versions' XML remain servable by key: an older-version instance's
        // Explorer diagram survives a redeploy (the bug this fixes).
        assert_eq!(
            store.process_definition_xml(6).as_deref(),
            Some("<xml>v1</xml>")
        );
        assert_eq!(
            store.process_definition_xml(297).as_deref(),
            Some("<xml>v2</xml>")
        );

        // Search now surfaces every version (Camunda parity), with `is_latest`
        // marking the highest version per id.
        let mut defs = store.process_definitions();
        defs.sort_by_key(|d| d.version);
        assert_eq!(defs.len(), 2);
        assert_eq!(defs[0].version, 1);
        assert_eq!(defs[0].key, 6);
        assert!(!defs[0].is_latest);
        assert_eq!(defs[1].version, 2);
        assert_eq!(defs[1].key, 297);
        assert!(defs[1].is_latest);

        // Get-by-key resolves any version, including the superseded one.
        let v1_row = store.process_definition_by_key(6).expect("v1 by key");
        assert_eq!(v1_row.version, 1);
        assert!(!v1_row.is_latest);
        assert!(store.process_definition_by_key(999).is_none());
    }

    #[test]
    fn seed_from_engine_state_projects_every_retained_version() {
        use nanobpmn_engine_core::{DeployedProcess, State};

        // A below-compaction-floor recovery rebuilds the read model from the live
        // engine snapshot. `State::process_versions` retains EVERY deployed
        // version; the projection must surface all of them (not just the
        // latest-by-id index in `state.processes`), or a superseded definition
        // would silently vanish from search / get-by-key after recovery.
        let mk = |key: u64, version: i32, xml: &str| {
            let mut def: ProcessDefinition = ProcessBuilder::new("p")
                .start_event("s")
                .end_event("e")
                .connect("s", "e")
                .build()
                .unwrap();
            def.xml = xml.to_string();
            DeployedProcess {
                key,
                version,
                definition: def,
            }
        };

        let mut state = State::default();
        let v1 = mk(6, 1, "<xml>v1</xml>");
        let v2 = mk(297, 2, "<xml>v2</xml>");
        state.process_versions.insert(6, v1.clone());
        state.process_versions.insert(297, v2.clone());
        // `processes` is the latest-by-id index — v2 only. If the projection read
        // this instead of `process_versions`, v1 would be dropped.
        state.processes.insert("p".to_string(), v2.clone());

        let store = ReadStore::open(None).unwrap();
        store.seed_from_engine_state(&state).unwrap();

        let mut defs = store.process_definitions();
        defs.sort_by_key(|d| d.version);
        assert_eq!(defs.len(), 2, "both retained versions must be projected");
        assert_eq!(
            (defs[0].version, defs[0].key, defs[0].is_latest),
            (1, 6, false)
        );
        assert_eq!(
            (defs[1].version, defs[1].key, defs[1].is_latest),
            (2, 297, true)
        );
        // The superseded version resolves by key after recovery.
        assert_eq!(
            store.process_definition_xml(6).as_deref(),
            Some("<xml>v1</xml>")
        );
        assert_eq!(
            store.process_definition_by_key(6).map(|d| d.version),
            Some(1)
        );
    }

    #[test]
    fn seed_from_engine_state_falls_back_to_latest_index_for_legacy_snapshots() {
        use nanobpmn_engine_core::{DeployedProcess, State};

        // A pre-retention snapshot deserializes `process_versions` empty
        // (`serde(default)`); the projection must fall back to the latest-by-id
        // index so the latest definition still recovers.
        let mut def: ProcessDefinition = ProcessBuilder::new("p")
            .start_event("s")
            .end_event("e")
            .connect("s", "e")
            .build()
            .unwrap();
        def.xml = "<xml>latest</xml>".to_string();

        let mut state = State::default();
        state.processes.insert(
            "p".to_string(),
            DeployedProcess {
                key: 42,
                version: 3,
                definition: def,
            },
        );
        assert!(state.process_versions.is_empty());

        let store = ReadStore::open(None).unwrap();
        store.seed_from_engine_state(&state).unwrap();

        let defs = store.process_definitions();
        assert_eq!(defs.len(), 1);
        assert_eq!(defs[0].key, 42);
        assert_eq!(defs[0].version, 3);
        assert!(defs[0].is_latest);
        assert_eq!(
            store.process_definition_xml(42).as_deref(),
            Some("<xml>latest</xml>")
        );
    }

    #[test]
    fn export_inflight_delta_is_exact_under_idempotent_redelivery() {
        let store = ReadStore::open(None).unwrap();

        // A genuine create contributes +1.
        let created = created_event(1);
        assert_eq!(store.export(&[&created]).unwrap().inflight_delta, 1);
        // Re-delivering the same create must NOT move the gauge (this is the
        // historical drift: idempotent projection double-counted raw events).
        let out = store.export(&[&created]).unwrap();
        assert_eq!(out.inflight_delta, 0);
        assert!(out.terminal_keys.is_empty());

        // A genuine completion contributes -1 and reports the terminal key once.
        let done = Event::ProcessInstanceCompleted { instance_key: 1 };
        let out = store.export(&[&done]).unwrap();
        assert_eq!(out.inflight_delta, -1);
        assert_eq!(out.terminal_keys, vec![1]);
        // Re-delivering the completion (or a late create re-delivery) is inert.
        let out = store.export(&[&done]).unwrap();
        assert_eq!(out.inflight_delta, 0);
        assert!(out.terminal_keys.is_empty());
        assert_eq!(store.export(&[&created]).unwrap().inflight_delta, 0);

        // Net gauge over the whole life is zero, and the row is terminal.
        assert_eq!(store.active_instance_count(), 0);
    }

    #[test]
    fn reconcile_orphaned_active_retires_rows_absent_from_the_live_set() {
        use nanobpmn_engine_core::ProcessInstanceState;
        let store = ReadStore::open(None).unwrap();
        // Three creates, none completed: all Active.
        for k in [10u64, 11, 12] {
            store.export(&[&created_event(k)]).unwrap();
        }
        assert_eq!(store.active_instance_count(), 3);

        // Engine truth: only 11 is genuinely live (e.g. cold-spilled). 10 and 12
        // are orphans — their CREATE was projected but the terminal event never
        // was — so the engine holds no such instance.
        let mut live = std::collections::HashSet::new();
        live.insert(11u64);

        let reconciled = store.reconcile_orphaned_active(&live);
        assert_eq!(reconciled, 2, "10 and 12 are retired; 11 is live");
        assert_eq!(store.active_instance_count(), 1);
        // The live instance is untouched and still Active; the orphans are now
        // Completed (not deleted — the row is preserved for queries).
        assert_eq!(
            store.process_instance(11).map(|r| r.state),
            Some(ProcessInstanceState::Active)
        );
        assert_eq!(
            store.process_instance(10).map(|r| r.state),
            Some(ProcessInstanceState::Completed)
        );

        // Idempotent: a second sweep with the same live set retires nothing.
        assert_eq!(store.reconcile_orphaned_active(&live), 0);

        // A late genuine completion re-delivery for an already-reconciled orphan
        // is inert (the `WHERE state = 0` guard), so the gauge never double-counts.
        let done = Event::ProcessInstanceCompleted { instance_key: 10 };
        assert_eq!(store.export(&[&done]).unwrap().inflight_delta, 0);
        assert_eq!(store.active_instance_count(), 1);
    }

    #[test]
    fn suspend_and_resume_drive_suspended_state_and_suspended_date() {
        use nanobpmn_engine_core::ProcessInstanceState;
        let store = ReadStore::open(None).unwrap();

        // A live instance starts Active with no suspension timestamp.
        assert_eq!(
            store.export(&[&created_event(7)]).unwrap().inflight_delta,
            1
        );
        let row = store.process_instance(7).unwrap();
        assert_eq!(row.state, ProcessInstanceState::Active);
        assert_eq!(row.suspended_date_ms, None);

        // Suspending records the instant and derives the Suspended state, but
        // leaves it counted as in-flight (gauge unchanged).
        let out = store
            .export(&[&Event::ProcessInstanceSuspended {
                instance_key: 7,
                at: 1_700_000_000_000,
            }])
            .unwrap();
        assert_eq!(out.inflight_delta, 0);
        let row = store.process_instance(7).unwrap();
        assert_eq!(row.state, ProcessInstanceState::Suspended);
        assert_eq!(row.suspended_date_ms, Some(1_700_000_000_000));
        assert_eq!(store.active_instance_count(), 1);

        // Resuming clears the timestamp and reverts to Active.
        let out = store
            .export(&[&Event::ProcessInstanceResumed { instance_key: 7 }])
            .unwrap();
        assert_eq!(out.inflight_delta, 0);
        let row = store.process_instance(7).unwrap();
        assert_eq!(row.state, ProcessInstanceState::Active);
        assert_eq!(row.suspended_date_ms, None);

        // A completion after resume still terminates cleanly (gauge -1).
        let out = store
            .export(&[&Event::ProcessInstanceCompleted { instance_key: 7 }])
            .unwrap();
        assert_eq!(out.inflight_delta, -1);
        assert_eq!(
            store.process_instance(7).map(|r| r.state),
            Some(ProcessInstanceState::Completed)
        );
    }

    #[test]
    fn business_id_assignment_projects_onto_the_instance_row() {
        let store = ReadStore::open(None).unwrap();
        store.export(&[&created_event(7)]).unwrap();
        assert_eq!(store.process_instance(7).unwrap().business_id, None);
        let out = store
            .export(&[&Event::ProcessInstanceBusinessIdAssigned {
                instance_key: 7,
                business_id: "order-9".into(),
            }])
            .unwrap();
        assert_eq!(out.inflight_delta, 0);
        assert_eq!(
            store.process_instance(7).unwrap().business_id.as_deref(),
            Some("order-9")
        );
    }

    #[test]
    fn timer_signal_and_conditional_waits_project_open_rows_until_they_settle() {
        use nanobpmn_engine_core::{Key, MessageSubscriptionKind as SubKind, TimerKind};

        use super::EventWaitType;
        let store = ReadStore::open(None).unwrap();
        store.export(&[&created_event(7)]).unwrap();
        let waits = |store: &ReadStore| {
            let mut w: Vec<(Key, EventWaitType)> = store
                .event_waits()
                .into_iter()
                .map(|w| (w.wait_key, w.wait_type))
                .collect();
            w.sort_by_key(|(k, _)| *k);
            w
        };
        let ni = SubKind::NonInterruptingBoundary {
            boundary_element_id: "b".into(),
        };
        store
            .export(&[
                &Event::TimerCreated {
                    timer_key: 10,
                    instance_key: 7,
                    element_instance_key: 20,
                    element_id: "wait".into(),
                    due_at: 5_000,
                    kind: TimerKind::IntermediateCatch,
                },
                &Event::SignalSubscriptionCreated {
                    subscription_key: 11,
                    instance_key: 7,
                    element_instance_key: 21,
                    element_id: "sig".into(),
                    signal_name: "go".into(),
                    kind: SubKind::IntermediateCatch,
                },
                &Event::ConditionalSubscriptionCreated {
                    subscription_key: 12,
                    instance_key: 7,
                    element_instance_key: 22,
                    element_id: "cond".into(),
                    condition: "= x > 1".into(),
                    referenced_vars: vec!["x".into()],
                    kind: ni.clone(),
                },
                &Event::SignalSubscriptionCreated {
                    subscription_key: 13,
                    instance_key: 7,
                    element_instance_key: 23,
                    element_id: "sig-ni".into(),
                    signal_name: "tick".into(),
                    kind: ni,
                },
            ])
            .unwrap();
        assert_eq!(
            waits(&store),
            vec![
                (10, EventWaitType::Timer),
                (11, EventWaitType::Signal),
                (12, EventWaitType::Condition),
                (13, EventWaitType::Signal),
            ]
        );
        let rows = store.event_waits();
        let timer = rows.iter().find(|w| w.wait_key == 10).unwrap();
        assert_eq!(
            (timer.due_at_ms, timer.element_instance_key),
            (Some(5_000), 20)
        );
        let cond = rows.iter().find(|w| w.wait_key == 12).unwrap();
        assert_eq!(cond.detail, "= x > 1");

        // Firing settles a timer / interrupting signal; a non-interrupting
        // conditional or signal boundary stays open (it can fire again).
        store
            .export(&[
                &Event::TimerTriggered {
                    timer_key: 10,
                    instance_key: 7,
                    element_instance_key: 20,
                    element_id: "wait".into(),
                },
                &Event::SignalCorrelated {
                    subscription_key: 11,
                    signal_key: 99,
                    instance_key: 7,
                    element_instance_key: 21,
                    element_id: "sig".into(),
                },
                &Event::ConditionalTriggered {
                    subscription_key: 12,
                    instance_key: 7,
                    element_instance_key: 22,
                    element_id: "cond".into(),
                },
                &Event::SignalCorrelated {
                    subscription_key: 13,
                    signal_key: 99,
                    instance_key: 7,
                    element_instance_key: 23,
                    element_id: "sig-ni".into(),
                },
            ])
            .unwrap();
        assert_eq!(
            waits(&store),
            vec![(12, EventWaitType::Condition), (13, EventWaitType::Signal)]
        );

        // Cancellation settles; instance termination clears whatever remains.
        store
            .export(&[&Event::ConditionalSubscriptionCanceled {
                subscription_key: 12,
                instance_key: 7,
                element_instance_key: 22,
                element_id: "cond".into(),
            }])
            .unwrap();
        assert_eq!(waits(&store), vec![(13, EventWaitType::Signal)]);
        store
            .export(&[&Event::ProcessInstanceTerminated { instance_key: 7 }])
            .unwrap();
        assert!(waits(&store).is_empty());
    }

    #[test]
    fn upgrading_from_before_event_waits_backfills_live_waits_from_the_engine_state() {
        use nanobpmn_engine_core::{
            ConditionalSubscription, MessageSubscriptionKind, MessageSubscriptionState, State,
            Timer, TimerKind, TimerState,
        };

        use super::EventWaitType;
        let path = std::env::temp_dir().join(format!(
            "nanobpm-event-waits-backfill-{}-{}.sqlite",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        // A fresh store never needs the backfill.
        let store = ReadStore::open(Some(&path)).unwrap();
        assert!(
            !store
                .backfill_pending_event_waits(&State::default())
                .unwrap()
        );
        // Wind it back to a v8 store (no `event_waits` table).
        {
            let conn = store.conn.lock().unwrap();
            conn.execute_batch(
                "DROP TABLE event_waits; UPDATE meta SET v = 8 WHERE k = 'schema_version';",
            )
            .unwrap();
        }
        drop(store);

        let store = ReadStore::open(Some(&path)).unwrap();
        let mut state = State::default();
        let timer = |key, state| Timer {
            key,
            instance_key: 7,
            element_instance_key: 20,
            element_id: "wait".into(),
            due_at: 5_000,
            state,
            kind: TimerKind::IntermediateCatch,
        };
        state.timers.insert(10, timer(10, TimerState::Created));
        state.timers.insert(11, timer(11, TimerState::Triggered));
        state.conditional_subscriptions.insert(
            12,
            ConditionalSubscription {
                key: 12,
                instance_key: 7,
                element_instance_key: 22,
                element_id: "cond".into(),
                condition: "= x > 1".into(),
                referenced_vars: vec!["x".into()],
                state: MessageSubscriptionState::Open,
                kind: MessageSubscriptionKind::IntermediateCatch,
            },
        );
        assert!(store.backfill_pending_event_waits(&state).unwrap());
        let mut got: Vec<_> = store
            .event_waits()
            .into_iter()
            .map(|w| (w.wait_key, w.wait_type, w.due_at_ms))
            .collect();
        got.sort_by_key(|w| w.0);
        assert_eq!(
            got,
            vec![
                (10, EventWaitType::Timer, Some(5_000)),
                (12, EventWaitType::Condition, None),
            ]
        );
        // The flag is cleared: the backfill runs once.
        assert!(!store.backfill_pending_event_waits(&state).unwrap());
        drop(store);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn export_inflight_delta_sums_a_mixed_batch() {
        let store = ReadStore::open(None).unwrap();
        // Two creates + one completion in one batch => net +1.
        let c2 = created_event(2);
        let c3 = created_event(3);
        let done2 = Event::ProcessInstanceCompleted { instance_key: 2 };
        let out = store.export(&[&c2, &c3, &done2]).unwrap();
        assert_eq!(out.inflight_delta, 1);
        assert_eq!(out.terminal_keys, vec![2]);
        assert_eq!(store.active_instance_count(), 1);
    }

    #[test]
    fn preexisting_terminal_history_is_backfilled_into_the_archive() {
        // History that completed BEFORE the archive existed (the merlin.local data)
        // is captured once on the first boot carrying this fix, so it too survives a
        // later reprojection — not only instances that complete afterwards.
        let n = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "nanobpm-backfill-{}-{}.sqlite",
            std::process::id(),
            n
        ));
        let cleanup = |p: &std::path::Path| {
            let _ = std::fs::remove_file(p);
            let _ = std::fs::remove_file(p.with_extension("sqlite-wal"));
            let _ = std::fs::remove_file(p.with_extension("sqlite-shm"));
        };
        cleanup(&path);

        // A read model that already holds a terminal instance which was NEVER
        // captured (inserted directly, as if it completed under an older binary),
        // and whose backfill has not run.
        {
            let store = ReadStore::open(Some(&path)).unwrap();
            let conn = store.conn.lock().unwrap();
            conn.execute(
                "INSERT INTO process_instances \
                 (key, process_id, process_definition_id, process_definition_key, \
                  version, state, start_date_ms, has_incident, tags, business_id) \
                 VALUES (77, 'legacy', 'legacy', '1', 1, 1, 0, 0, '[]', NULL)",
                [],
            )
            .unwrap();
            conn.execute(
                "DELETE FROM meta WHERE k = 'terminal_archive_backfilled'",
                [],
            )
            .unwrap();
        }

        // Reopen: the one-time backfill copies the pre-existing terminal instance
        // into the durable archive.
        let store = ReadStore::open(Some(&path)).unwrap();
        // Wipe (reprojection stand-in) and replay: the legacy history is restored.
        store.reset().unwrap();
        assert!(store.process_instance(77).is_none());
        let restored = store.replay_terminal_archive().unwrap();
        assert_eq!(
            restored, 1,
            "the pre-existing terminal instance was backfilled"
        );
        assert_eq!(
            store.process_instance(77).map(|r| r.state),
            Some(nanobpmn_engine_core::ProcessInstanceState::Completed),
            "legacy completed history survives reprojection after backfill (issue #831)"
        );

        drop(store);
        cleanup(&path);
        let archive = path.with_file_name(format!(
            "{}.terminal-archive.sqlite",
            path.file_stem().unwrap().to_string_lossy()
        ));
        cleanup(&archive);
    }

    #[test]
    fn terminal_history_survives_reprojection_via_durable_archive() {
        // The issue #831 durability guarantee: a terminal instance is copied to the
        // durable terminal-audit archive as it completes, so that after a below-floor
        // snapshot reprojection wipes the read model (which the engine snapshot can
        // only refill with *live* instances), the completed history is restored from
        // the archive. Reproduces the merlin.local loss and asserts it no longer
        // occurs.
        let n = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "nanobpm-archive-{}-{}.sqlite",
            std::process::id(),
            n
        ));
        let cleanup = |p: &std::path::Path| {
            let _ = std::fs::remove_file(p);
            let _ = std::fs::remove_file(p.with_extension("sqlite-wal"));
            let _ = std::fs::remove_file(p.with_extension("sqlite-shm"));
        };
        cleanup(&path);

        let store = ReadStore::open(Some(&path)).unwrap();
        // Create and complete an instance: the completion is a genuine terminal
        // transition, so `export` archives it durably.
        let created = created_event(42);
        let done = Event::ProcessInstanceCompleted { instance_key: 42 };
        let out = store.export(&[&created, &done]).unwrap();
        assert_eq!(out.terminal_keys, vec![42]);
        assert_eq!(
            store.process_instance(42).map(|r| r.state),
            Some(nanobpmn_engine_core::ProcessInstanceState::Completed),
            "instance completed and present before the wipe"
        );

        // Simulate the below-floor reprojection: the read model is reset (wiped),
        // and the engine snapshot holds only live instances — so the terminal
        // instance is NOT reprojected and would be lost without the archive.
        store.reset().unwrap();
        assert!(
            store.process_instance(42).is_none(),
            "reset wipes the read model (stands in for the reprojection)"
        );

        // Replaying the durable archive restores the completed history.
        let restored = store.replay_terminal_archive().unwrap();
        assert_eq!(
            restored, 1,
            "one terminal instance restored from the archive"
        );
        assert_eq!(
            store.process_instance(42).map(|r| r.state),
            Some(nanobpmn_engine_core::ProcessInstanceState::Completed),
            "terminal/completed history is queryable again after reprojection (issue #831)"
        );

        drop(store);
        cleanup(&path);
        let archive = path.with_file_name(format!(
            "{}.terminal-archive.sqlite",
            path.file_stem().unwrap().to_string_lossy()
        ));
        cleanup(&archive);
    }

    fn created_event(instance_key: super::Key) -> Event {
        Event::ProcessInstanceCreated {
            instance_key,
            process_id: "p".to_string(),
            variables: std::collections::HashMap::new(),
            created_at: 0,
            tags: Vec::new(),
            business_id: None,
            process_definition_key: 0,
            version: 0,
            parent_process_instance_key: None,
            parent_element_instance_key: None,
        }
    }

    /// A create event that pins the instance to an explicit definition
    /// (`key`/`version`) — the on-the-wire shape a by-key or by-id+version
    /// create produces.
    fn created_event_pinned(
        instance_key: super::Key,
        process_id: &str,
        process_definition_key: super::Key,
        version: i32,
    ) -> Event {
        Event::ProcessInstanceCreated {
            instance_key,
            process_id: process_id.to_string(),
            variables: std::collections::HashMap::new(),
            created_at: 0,
            tags: Vec::new(),
            business_id: None,
            process_definition_key,
            version,
            parent_process_instance_key: None,
            parent_element_instance_key: None,
        }
    }

    /// A create event carrying the call-activity parent linkage (issue #977):
    /// the calling instance's key and the spawning call-activity element instance
    /// key, so a hierarchy can be seeded event-first.
    fn created_event_with_parent(
        instance_key: super::Key,
        parent_pi: super::Key,
        parent_ei: super::Key,
    ) -> Event {
        Event::ProcessInstanceCreated {
            instance_key,
            process_id: "p".to_string(),
            variables: std::collections::HashMap::new(),
            created_at: 0,
            tags: Vec::new(),
            business_id: None,
            process_definition_key: 0,
            version: 0,
            parent_process_instance_key: Some(parent_pi),
            parent_element_instance_key: Some(parent_ei),
        }
    }

    /// `ReadStore::root_process_instance_key` walks the `parentProcessInstanceKey`
    /// chain to the top-level ancestor within a single (partition-co-located)
    /// store: a top-level instance self-roots and every call-activity descendant
    /// — however deeply nested — roots to the top-level instance, never its own
    /// key. This is the walk the `engine-wasm` `TestEngine` read model relies on.
    #[test]
    fn root_process_instance_key_walks_a_co_located_hierarchy_to_the_top() {
        let store = ReadStore::open(None).unwrap();
        // top (no parent) <- child (via EI 111) <- grandchild (via EI 222).
        store
            .export(&[
                &created_event(10),
                &created_event_with_parent(20, 10, 111),
                &created_event_with_parent(30, 20, 222),
            ])
            .unwrap();

        assert_eq!(store.root_process_instance_key(10), 10, "top self-roots");
        assert_eq!(
            store.root_process_instance_key(20),
            10,
            "direct child roots to the top"
        );
        assert_eq!(
            store.root_process_instance_key(30),
            10,
            "nested descendant roots to the top, not its own or its parent's key"
        );
    }

    /// Best-effort boundaries plus the cycle guard: an unknown key self-roots
    /// (nothing to walk); a known child whose parent row is absent roots to that
    /// furthest observable ancestor key; and a corrupt `A → B → A` parent cycle
    /// terminates at the re-encountered ring member instead of looping forever
    /// (issue #977, the lasso guard).
    #[test]
    fn root_process_instance_key_is_best_effort_and_cycle_safe() {
        let store = ReadStore::open(None).unwrap();
        // `child` (2000) names a parent (7777) that was never projected/pruned.
        store
            .export(&[&created_event_with_parent(2000, 7777, 111)])
            .unwrap();
        // A lasso cycle: A(40) -> B(41) -> A(40) -> …
        store
            .export(&[
                &created_event_with_parent(40, 41, 333),
                &created_event_with_parent(41, 40, 444),
            ])
            .unwrap();

        // Unknown key: nothing to walk, roots to itself.
        assert_eq!(store.root_process_instance_key(9999), 9999);
        // Known child, absent parent: the furthest known ancestor is the parent
        // key, so that is the reported root (not the child).
        assert_eq!(store.root_process_instance_key(2000), 7777);
        // The cycle terminates at a ring member rather than hanging; the
        // guarantee under test is termination, not a value.
        let cycle_root = store.root_process_instance_key(40);
        assert!(
            cycle_root == 40 || cycle_root == 41,
            "a parent cycle terminates at a ring member, got {cycle_root}"
        );
    }

    /// A [`RootResolver`] walks each parent chain **once** and memoises every key
    /// it touches, so projecting a page of co-located descendants is
    /// `O(distinct keys)` point lookups, not `O(rows × chain-depth)` (issue #977
    /// review: the naive per-row walk repeated shared-ancestor lookups).
    #[test]
    fn root_resolver_memoises_the_whole_walked_chain() {
        let store = ReadStore::open(None).unwrap();
        // top(10) <- child(20) <- grandchild(30) <- great-grandchild(40).
        store
            .export(&[
                &created_event(10),
                &created_event_with_parent(20, 10, 111),
                &created_event_with_parent(30, 20, 222),
                &created_event_with_parent(40, 30, 333),
            ])
            .unwrap();

        let lookups = std::cell::Cell::new(0usize);
        let resolver = RootResolver::new(|k| {
            lookups.set(lookups.get() + 1);
            store.process_instance(k)
        });

        // Resolving the deepest descendant seeds every ancestor on the chain.
        assert_eq!(resolver.root_process_instance_key(40), 10);
        let after_deep = lookups.get();
        assert_eq!(
            after_deep, 4,
            "one walk looks up each of the four chain keys exactly once"
        );

        // Every other chain member now resolves from cache — no further lookups.
        assert_eq!(resolver.root_process_instance_key(30), 10);
        assert_eq!(resolver.root_process_instance_key(20), 10);
        assert_eq!(resolver.root_process_instance_key(10), 10);
        assert_eq!(resolver.root_process_instance_key(40), 10);
        assert_eq!(
            lookups.get(),
            after_deep,
            "cached ancestors trigger no re-walk"
        );
    }

    /// `root_of_row` resolves an already-loaded row identically to
    /// `root_process_instance_key(row.key)` but skips the redundant lookup of the
    /// row's own key — a top-level row self-roots with **zero** lookups, and a
    /// child row does one fewer lookup than the by-key walk. Guards the
    /// read-amplification fix (issue #977 review) and the parity of the two entry
    /// points (a top-level row, a co-located child chain, and a corrupt cycle).
    #[test]
    fn root_of_row_matches_the_by_key_walk_without_the_self_lookup() {
        let store = ReadStore::open(None).unwrap();
        // top(10) <- child(20) <- grandchild(30); plus a lasso A(40)->B(41)->A.
        store
            .export(&[
                &created_event(10),
                &created_event_with_parent(20, 10, 111),
                &created_event_with_parent(30, 20, 222),
                &created_event_with_parent(40, 41, 333),
                &created_event_with_parent(41, 40, 444),
            ])
            .unwrap();

        // Top-level row self-roots with NO lookup at all.
        let lookups = std::cell::Cell::new(0usize);
        let resolver = RootResolver::new(|k| {
            lookups.set(lookups.get() + 1);
            store.process_instance(k)
        });
        let top = store.process_instance(10).unwrap();
        assert_eq!(resolver.root_of_row(&top), 10);
        assert_eq!(lookups.get(), 0, "a top-level row needs no point lookup");

        // A child row: one fewer lookup than the by-key walk (its own key is not
        // re-read), same resolved root.
        let by_key = RootResolver::new(|k| store.process_instance(k));
        let child = store.process_instance(30).unwrap();
        let lookups2 = std::cell::Cell::new(0usize);
        let from_row = RootResolver::new(|k| {
            lookups2.set(lookups2.get() + 1);
            store.process_instance(k)
        });
        assert_eq!(
            from_row.root_of_row(&child),
            by_key.root_process_instance_key(30)
        );
        assert_eq!(from_row.root_of_row(&child), 10);
        assert_eq!(
            lookups2.get(),
            2,
            "walking from key 30's parent looks up only 20 and 10, not 30 itself"
        );

        // Cycle parity: `root_of_row` terminates at the same ring member as the
        // by-key walk (best-effort, termination is the guarantee).
        let cyc = RootResolver::new(|k| store.process_instance(k));
        let row_a = store.process_instance(40).unwrap();
        assert_eq!(
            cyc.root_of_row(&row_a),
            by_key.root_process_instance_key(40),
            "a corrupt cycle terminates identically for both entry points"
        );
    }

    #[test]
    fn read_model_reports_each_instance_its_pinned_version() {
        // Two versions of the same process id are deployed; one instance is
        // created against the *older* version (by-key) and one against the
        // latest. The read model must report the version each instance was
        // actually created on — not merely the newest deployed.
        let store = ReadStore::open(None).unwrap();
        let v1 = deployed_event_versioned("order", 6, 1, "<xml>v1</xml>");
        let v2 = deployed_event_versioned("order", 297, 2, "<xml>v2</xml>");
        store.export(&[&v1, &v2]).unwrap();

        // Instance A pins v1 (key 6); instance B pins v2 (key 297).
        let a = created_event_pinned(1000, "order", 6, 1);
        let b = created_event_pinned(1001, "order", 297, 2);
        store.export(&[&a, &b]).unwrap();

        let row_a = store.process_instance(1000).expect("instance A present");
        assert_eq!(row_a.version, 1, "A reports the version it was created on");
        assert_eq!(row_a.process_definition_key, "6");

        let row_b = store.process_instance(1001).expect("instance B present");
        assert_eq!(row_b.version, 2, "B reports the latest version");
        assert_eq!(row_b.process_definition_key, "297");
    }

    #[test]
    fn prune_terminal_instances_caps_history_keeping_active_and_newest() {
        let store = ReadStore::open(None).unwrap();
        // 5 terminal instances (keys 1..=5) + 2 active (keys 100, 101).
        for k in 1..=5u64 {
            let created = created_event(k);
            let done = Event::ProcessInstanceCompleted { instance_key: k };
            store.export(&[&created, &done]).unwrap();
        }
        for k in [100u64, 101] {
            let created = created_event(k);
            store.export(&[&created]).unwrap();
        }

        // max_keep == 0 disables pruning.
        assert_eq!(store.prune_terminal_instances(0, 0).unwrap(), 0);
        assert!(store.process_instance(1).is_some());
        // COUNT(*) over all instances: 5 terminal + 2 active = 7.
        assert_eq!(store.instance_count(), 7);

        // Batched: with a delete cap of 1, only the oldest terminal beyond the
        // cap (key 1) is evicted this sweep; keys 2,3 remain until later sweeps.
        assert_eq!(store.prune_terminal_instances(2, 1).unwrap(), 1);
        assert!(store.process_instance(1).is_none());
        assert!(store.process_instance(2).is_some());
        assert!(store.process_instance(3).is_some());
        assert_eq!(store.instance_count(), 6);

        // Unbounded (max_delete == 0): evict the remaining overflow (keys 2, 3),
        // keeping the 2 newest terminal instances (keys 4, 5).
        let evicted = store.prune_terminal_instances(2, 0).unwrap();
        assert_eq!(evicted, 2);
        assert!(store.process_instance(2).is_none());
        assert!(store.process_instance(3).is_none());
        assert!(store.process_instance(4).is_some());
        assert!(store.process_instance(5).is_some());
        // Active instances are never evicted.
        assert!(store.process_instance(100).is_some());
        assert!(store.process_instance(101).is_some());
        assert_eq!(store.active_instance_count(), 2);
        // 2 newest terminal (4,5) + 2 active (100,101) = 4.
        assert_eq!(store.instance_count(), 4);

        // Re-pruning at the same cap is a no-op (nothing beyond the cap).
        assert_eq!(store.prune_terminal_instances(2, 0).unwrap(), 0);
    }

    #[test]
    fn adaptive_prune_once_evicts_oldest_terminal_in_bounded_batches() {
        // File-backed so the pruner can open its own second connection.
        let path = std::env::temp_dir().join(format!(
            "nanobpm-pruner-{}-{}.sqlite",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let store = ReadStore::open(Some(&path)).unwrap();
        // 5 terminal (keys 1..=5) + 2 active (100, 101).
        for k in 1..=5u64 {
            let created = created_event(k);
            let done = Event::ProcessInstanceCompleted { instance_key: k };
            store.export(&[&created, &done]).unwrap();
        }
        for k in [100u64, 101] {
            store.export(&[&created_event(k)]).unwrap();
        }
        let mut conn = store.prune_connection().unwrap().expect("file-backed");

        // Under budget (huge high-water): a cheap no-op, evicts nothing.
        assert_eq!(
            store
                .adaptive_prune_once(&mut conn, u64::MAX, u64::MAX, 4096, 4096)
                .unwrap(),
            0
        );
        assert_eq!(store.instance_count(), 7);

        // Over budget (high=low=1 forces eviction), capped at 2 deletes this wake:
        // the OLDEST two terminal (keys 1, 2) go first.
        assert_eq!(
            store.adaptive_prune_once(&mut conn, 1, 1, 4096, 2).unwrap(),
            2
        );
        assert!(store.process_instance(1).is_none());
        assert!(store.process_instance(2).is_none());
        assert!(store.process_instance(3).is_some());
        assert_eq!(store.instance_count(), 5);

        // Next wake with a generous cap drains the remaining terminal (3,4,5)…
        assert_eq!(
            store
                .adaptive_prune_once(&mut conn, 1, 1, 4096, 4096)
                .unwrap(),
            3
        );
        // …but never the active instances.
        assert!(store.process_instance(100).is_some());
        assert!(store.process_instance(101).is_some());
        assert_eq!(store.active_instance_count(), 2);
        assert_eq!(store.instance_count(), 2);

        // Nothing terminal left: a no-op even while "over budget".
        assert_eq!(
            store
                .adaptive_prune_once(&mut conn, 1, 1, 4096, 4096)
                .unwrap(),
            0
        );

        drop(conn);
        drop(store);
        std::fs::remove_file(&path).ok();
        std::fs::remove_file(path.with_extension("sqlite-wal")).ok();
        std::fs::remove_file(path.with_extension("sqlite-shm")).ok();
    }

    #[test]
    fn wal_checkpoint_is_size_gated() {
        // The size-gate is what turns the former ~5/s TRUNCATE storm (a dominant
        // read-model write amplifier) into an occasional file-space reclaim: below
        // the threshold `maybe_checkpoint_wal` must be a no-op; at/above it must
        // TRUNCATE the WAL back into the main DB (shrinking the -wal sidecar).
        let path = std::env::temp_dir().join(format!(
            "nanobpm-ckptgate-{}-{}.sqlite",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let store = ReadStore::open(Some(&path)).unwrap();
        // Write enough committed rows to grow the WAL sidecar past zero. The raised
        // autocheckpoint (48 MiB) will not have truncated it for this small volume.
        for k in 1..=200u64 {
            store.export(&[&created_event(k)]).unwrap();
        }
        let conn = store.prune_connection().unwrap().expect("file-backed");
        let wal_before = store.wal_len_bytes();
        assert!(wal_before > 0, "WAL should hold uncheckpointed frames");

        // Threshold above the current WAL size → gated off, no checkpoint, WAL unchanged.
        assert!(!store.checkpoint_wal_if_larger_than(&conn, wal_before + 1));
        assert_eq!(
            store.wal_len_bytes(),
            wal_before,
            "no-op must not shrink WAL"
        );

        // Threshold at/below the WAL size → checkpoint runs and TRUNCATE shrinks it.
        assert!(store.checkpoint_wal_if_larger_than(&conn, 1));
        assert!(
            store.wal_len_bytes() < wal_before,
            "TRUNCATE checkpoint must reclaim WAL file space"
        );

        drop(conn);
        drop(store);
        std::fs::remove_file(&path).ok();
        std::fs::remove_file(path.with_extension("sqlite-wal")).ok();
        std::fs::remove_file(path.with_extension("sqlite-shm")).ok();
    }

    #[test]
    fn export_waits_on_the_shared_write_lock_instead_of_erroring() {
        // Regression for #97: the exporter and the adaptive pruner are two
        // independent WAL writers. With only SQLite's `busy_timeout` a long
        // pruner write would surface to the exporter as `database is locked`
        // (dropped/retried batch). The shared in-process `write_lock` must turn
        // that into a clean wait: while the lock is held, `export` blocks and
        // then succeeds — it never errors.
        let path = std::env::temp_dir().join(format!(
            "nanobpm-writelock-{}-{}.sqlite",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let store = std::sync::Arc::new(ReadStore::open(Some(&path)).unwrap());

        // Hold the write lock, mimicking a pruner mid delete+checkpoint.
        let guard = store.write_lock.lock().expect("write lock");

        let done = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let handle = {
            let store = store.clone();
            let done = done.clone();
            std::thread::spawn(move || {
                let created = created_event(1);
                // Blocks on `write_lock` until the main thread releases it; must
                // return Ok (no `database is locked`).
                store.export(&[&created]).expect("export must not error");
                done.store(true, std::sync::atomic::Ordering::SeqCst);
            })
        };

        // While the lock is held the export cannot have completed.
        std::thread::sleep(std::time::Duration::from_millis(150));
        assert!(
            !done.load(std::sync::atomic::Ordering::SeqCst),
            "export completed while the write lock was held — it did not serialize"
        );

        // Release; the export now proceeds and commits.
        drop(guard);
        handle.join().expect("export thread panicked");
        assert!(done.load(std::sync::atomic::Ordering::SeqCst));
        assert!(store.process_instance(1).is_some());

        drop(store);
        std::fs::remove_file(&path).ok();
        std::fs::remove_file(path.with_extension("sqlite-wal")).ok();
        std::fs::remove_file(path.with_extension("sqlite-shm")).ok();
    }

    #[test]
    fn reset_waits_on_the_shared_write_lock_instead_of_erroring() {
        // Regression guard: `reset` runs destructive DDL (drop-all + recreate)
        // and is one more independent WAL writer alongside the exporter and the
        // adaptive pruner. Like `export`/`advance_exported` it must serialize on
        // the shared in-process `write_lock`, so a `reset` racing a pruner mid
        // delete+checkpoint blocks cleanly instead of tripping
        // `database is locked`.
        let path = std::env::temp_dir().join(format!(
            "nanobpm-resetlock-{}-{}.sqlite",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let store = std::sync::Arc::new(ReadStore::open(Some(&path)).unwrap());

        let guard = store.write_lock.lock().expect("write lock");

        let done = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let handle = {
            let store = store.clone();
            let done = done.clone();
            std::thread::spawn(move || {
                store.reset().expect("reset must not error");
                done.store(true, std::sync::atomic::Ordering::SeqCst);
            })
        };

        std::thread::sleep(std::time::Duration::from_millis(150));
        assert!(
            !done.load(std::sync::atomic::Ordering::SeqCst),
            "reset completed while the write lock was held — it did not serialize"
        );

        drop(guard);
        handle.join().expect("reset thread panicked");
        assert!(done.load(std::sync::atomic::Ordering::SeqCst));

        drop(store);
        std::fs::remove_file(&path).ok();
        std::fs::remove_file(path.with_extension("sqlite-wal")).ok();
        std::fs::remove_file(path.with_extension("sqlite-shm")).ok();
    }

    #[test]
    fn terminal_state_predicate_is_derived_from_the_canonical_state_codes() {
        use nanobpmn_engine_core::ProcessInstanceState;

        use super::{
            TERMINAL_INSTANCE_STATE_CODES, instance_state_code, instance_state_from,
            terminal_state_predicate,
        };

        // Drift guard (issue #831): every terminal-selection query (eviction,
        // adaptive pruning, terminal-archive copy/backfill) must build its
        // `state IN (...)` predicate from `TERMINAL_INSTANCE_STATE_CODES`, which
        // is itself derived from `instance_state_code`. If the enum-to-int codes
        // ever change, this predicate follows automatically instead of a magic
        // `state IN (1, 2)` literal silently drifting out of sync.
        assert_eq!(
            TERMINAL_INSTANCE_STATE_CODES,
            [
                instance_state_code(ProcessInstanceState::Completed),
                instance_state_code(ProcessInstanceState::Terminated),
            ]
        );
        // The two codes must map back to genuinely terminal states, never to a
        // live (`Active`) or transient (`Terminating`) one.
        for code in TERMINAL_INSTANCE_STATE_CODES {
            assert!(matches!(
                instance_state_from(code),
                ProcessInstanceState::Completed | ProcessInstanceState::Terminated
            ));
        }
        let [completed, terminated] = TERMINAL_INSTANCE_STATE_CODES;
        assert_eq!(
            terminal_state_predicate("state"),
            format!("state IN ({completed}, {terminated})")
        );
    }

    #[test]
    fn endtimeless_terminal_job_predicate_is_derived_from_the_canonical_job_codes() {
        use nanobpmn_engine_core::JobState;

        use super::{
            ENDTIMELESS_TERMINAL_JOB_STATE_CODES, endtimeless_terminal_job_predicate,
            job_state_code, job_state_from,
        };

        // Drift guard: the `lastUpdateTime`-freeze arm (#1344) in every
        // subsequent-event job projection (JobLockExpired, JobFailed,
        // JobErrorThrown, JobRetriesUpdated, JobTimeoutUpdated) builds its
        // `state IN (...)` predicate from `ENDTIMELESS_TERMINAL_JOB_STATE_CODES`,
        // itself derived from `job_state_code`. If the enum-to-int codes are ever
        // renumbered the SQL follows automatically instead of a magic
        // `state IN (2, 3)` literal silently freezing the wrong states.
        assert_eq!(
            ENDTIMELESS_TERMINAL_JOB_STATE_CODES,
            [
                job_state_code(JobState::Failed),
                job_state_code(JobState::Errored),
            ]
        );
        // The two codes must map back to the genuinely terminal, `endTime`-less
        // states, never to a live or completed/canceled (`endTime`-bearing) one.
        for code in ENDTIMELESS_TERMINAL_JOB_STATE_CODES {
            assert!(matches!(
                job_state_from(code),
                JobState::Failed | JobState::Errored
            ));
        }
        let [failed, errored] = ENDTIMELESS_TERMINAL_JOB_STATE_CODES;
        assert_eq!(
            endtimeless_terminal_job_predicate("state"),
            format!("state IN ({failed}, {errored})")
        );
    }

    #[test]
    fn process_instances_page_returns_newest_first_bounded_pages() {
        use super::InstanceFilter;
        let store = ReadStore::open(None).unwrap();
        // Keys 1..=5, created oldest→newest; keys are monotonic so newest = key 5.
        for k in 1..=5u64 {
            store.export(&[&created_event(k)]).unwrap();
        }

        assert_eq!(store.process_instance_count(&InstanceFilter::default()), 5);

        // First page: the 2 newest, descending by key.
        let page0 = store.process_instances_page(2, 0, &InstanceFilter::default());
        assert_eq!(page0.iter().map(|r| r.key).collect::<Vec<_>>(), vec![5, 4]);

        // Second page picks up where the first left off.
        let page1 = store.process_instances_page(2, 2, &InstanceFilter::default());
        assert_eq!(page1.iter().map(|r| r.key).collect::<Vec<_>>(), vec![3, 2]);

        // Final partial page.
        let page2 = store.process_instances_page(2, 4, &InstanceFilter::default());
        assert_eq!(page2.iter().map(|r| r.key).collect::<Vec<_>>(), vec![1]);

        // Offset past the end yields nothing.
        assert!(
            store
                .process_instances_page(2, 6, &InstanceFilter::default())
                .is_empty()
        );
    }

    /// A `ProcessInstanceCreated` with an explicit key so filter tests can wire
    /// up several instances and then transition/incident a subset.
    #[test]
    fn process_instances_page_and_count_apply_state_and_incident_filter() {
        use nanobpmn_engine_core::ProcessInstanceState;

        use super::InstanceFilter;
        let store = ReadStore::open(None).unwrap();
        // Five Active instances, keys 1..=5.
        for k in 1..=5u64 {
            store.export(&[&created_event(k)]).unwrap();
        }
        // Complete key 2 and key 4 → Completed.
        store
            .export(&[&Event::ProcessInstanceCompleted { instance_key: 2 }])
            .unwrap();
        store
            .export(&[&Event::ProcessInstanceCompleted { instance_key: 4 }])
            .unwrap();
        // Terminate key 1 → Terminated.
        store
            .export(&[&Event::ProcessInstanceTerminated { instance_key: 1 }])
            .unwrap();
        // Raise an incident on (still Active) key 5.
        store
            .export(&[&Event::IncidentRaised {
                incident_key: 900,
                instance_key: 5,
                element_instance_key: 5001,
                element_id: "t".to_string(),
                kind: nanobpmn_engine_core::IncidentKind::JobNoRetries,
                reason: "boom".to_string(),
                job_key: Some(42),
                created_at: 5,
                redrive: None,
            }])
            .unwrap();
        // Final states: 1=Terminated, 2=Completed, 3=Active, 4=Completed,
        // 5=Active(+incident).

        let active = InstanceFilter {
            state: Some(ProcessInstanceState::Active),
            has_incident: None,
        };
        let completed = InstanceFilter {
            state: Some(ProcessInstanceState::Completed),
            has_incident: None,
        };
        let terminated = InstanceFilter {
            state: Some(ProcessInstanceState::Terminated),
            has_incident: None,
        };
        let has_incident = InstanceFilter {
            state: None,
            has_incident: Some(true),
        };
        let active_incident = InstanceFilter {
            state: Some(ProcessInstanceState::Active),
            has_incident: Some(true),
        };

        // Filtered page returns only matching rows, newest-first, bounded.
        let page = store.process_instances_page(50, 0, &active);
        assert_eq!(page.iter().map(|r| r.key).collect::<Vec<_>>(), vec![5, 3]);
        // Filtered count matches the number of filtered rows (pager-desync guard).
        assert_eq!(store.process_instance_count(&active), 2);

        assert_eq!(
            store
                .process_instances_page(50, 0, &completed)
                .iter()
                .map(|r| r.key)
                .collect::<Vec<_>>(),
            vec![4, 2]
        );
        assert_eq!(store.process_instance_count(&completed), 2);

        assert_eq!(
            store
                .process_instances_page(50, 0, &terminated)
                .iter()
                .map(|r| r.key)
                .collect::<Vec<_>>(),
            vec![1]
        );
        assert_eq!(store.process_instance_count(&terminated), 1);

        // Has-incident on its own.
        assert_eq!(
            store
                .process_instances_page(50, 0, &has_incident)
                .iter()
                .map(|r| r.key)
                .collect::<Vec<_>>(),
            vec![5]
        );
        assert_eq!(store.process_instance_count(&has_incident), 1);

        // state + has_incident combine with AND.
        assert_eq!(
            store
                .process_instances_page(50, 0, &active_incident)
                .iter()
                .map(|r| r.key)
                .collect::<Vec<_>>(),
            vec![5]
        );
        assert_eq!(store.process_instance_count(&active_incident), 1);
        // Completed + has_incident: no completed instance carries an incident.
        let completed_incident = InstanceFilter {
            state: Some(ProcessInstanceState::Completed),
            has_incident: Some(true),
        };
        assert!(
            store
                .process_instances_page(50, 0, &completed_incident)
                .is_empty()
        );
        assert_eq!(store.process_instance_count(&completed_incident), 0);

        // Filtered paging is still bounded/offset-correct.
        let terminal_page = store.process_instances_page(1, 1, &completed);
        assert_eq!(
            terminal_page.iter().map(|r| r.key).collect::<Vec<_>>(),
            vec![2]
        );

        // Empty/None filter == unfiltered result (regression guard).
        let unfiltered = store.process_instances_page(50, 0, &InstanceFilter::default());
        assert_eq!(
            unfiltered.iter().map(|r| r.key).collect::<Vec<_>>(),
            vec![5, 4, 3, 2, 1]
        );
        assert_eq!(store.process_instance_count(&InstanceFilter::default()), 5);
    }

    #[test]
    fn state_filter_disambiguates_active_from_suspended_via_suspended_date() {
        use nanobpmn_engine_core::ProcessInstanceState;

        use super::InstanceFilter;
        let store = ReadStore::open(None).unwrap();
        // Three Active instances, keys 1..=3.
        for k in 1..=3u64 {
            store.export(&[&created_event(k)]).unwrap();
        }
        // Suspend key 2 → base state code stays 0 (Active), but the nullable
        // `suspended_date_ms` column disambiguates it as Suspended.
        store
            .export(&[&Event::ProcessInstanceSuspended {
                instance_key: 2,
                at: 1_700_000_000_000,
            }])
            .unwrap();

        let active = InstanceFilter {
            state: Some(ProcessInstanceState::Active),
            has_incident: None,
        };
        let suspended = InstanceFilter {
            state: Some(ProcessInstanceState::Suspended),
            has_incident: None,
        };

        // SQL page + count filter for Active must exclude the suspended row…
        assert_eq!(
            store
                .process_instances_page(50, 0, &active)
                .iter()
                .map(|r| r.key)
                .collect::<Vec<_>>(),
            vec![3, 1]
        );
        assert_eq!(store.process_instance_count(&active), 2);
        // …and the Suspended filter must return only the suspended row (not the
        // active ones that share base code 0).
        assert_eq!(
            store
                .process_instances_page(50, 0, &suspended)
                .iter()
                .map(|r| r.key)
                .collect::<Vec<_>>(),
            vec![2]
        );
        assert_eq!(store.process_instance_count(&suspended), 1);

        // Page + count agree with the in-memory `matches` predicate used by the
        // multi-shard merge, so a sharded node filters identically (pager-desync
        // guard for the shared derivation).
        for (filter, expect) in [(&active, [3u64, 1].as_slice()), (&suspended, &[2])] {
            let all = store.process_instances_page(50, 0, &InstanceFilter::default());
            let merged: Vec<u64> = all
                .into_iter()
                .filter(|r| filter.matches(r))
                .map(|r| r.key)
                .collect();
            assert_eq!(merged, expect);
        }

        // Resuming key 2 clears the suspension: it returns to the Active set.
        store
            .export(&[&Event::ProcessInstanceResumed { instance_key: 2 }])
            .unwrap();
        assert_eq!(store.process_instance_count(&active), 3);
        assert_eq!(store.process_instance_count(&suspended), 0);
    }

    #[test]
    fn scoped_variables_are_projected_under_their_own_scope_key() {
        use nanobpmn_engine_core::Value;

        let store = ReadStore::open(None).unwrap();
        let instance_key: super::Key = 1;
        let scope_key: super::Key = 50; // a sub-process / MI-child element instance
        store.export(&[&created_event(instance_key)]).unwrap();

        // A root-scope write and a nested-scope write of the SAME name.
        store
            .export(&[
                &Event::VariablesUpdated {
                    instance_key,
                    variables: std::collections::HashMap::from([(
                        "amount".to_string(),
                        Value::Int(10),
                    )]),
                },
                &Event::ScopedVariablesUpdated {
                    instance_key,
                    scope_key,
                    variables: std::collections::HashMap::from([
                        ("amount".to_string(), Value::Int(20)),
                        ("item".to_string(), Value::Int(7)),
                    ]),
                },
            ])
            .unwrap();

        let mut rows = store.instance_variables(instance_key);
        rows.sort_by_key(|a| (a.scope_key, a.name.clone()));
        // Three distinct rows: root `amount`, scoped `amount`, scoped `item` —
        // the same name coexists across scopes because UNIQUE is (scope_key, name).
        assert_eq!(rows.len(), 3);

        let root_amount = rows
            .iter()
            .find(|r| r.scope_key == instance_key && r.name == "amount")
            .expect("root amount");
        assert_eq!(root_amount.value, "10");

        let scoped_amount = rows
            .iter()
            .find(|r| r.scope_key == scope_key && r.name == "amount")
            .expect("scoped amount");
        assert_eq!(scoped_amount.value, "20");
        assert_eq!(scoped_amount.instance_key, instance_key);

        let scoped_item = rows
            .iter()
            .find(|r| r.scope_key == scope_key && r.name == "item")
            .expect("scoped item");
        assert_eq!(scoped_item.value, "7");

        // Re-writing the nested scope updates in place (keeps its key/scope).
        let before = scoped_amount.key;
        store
            .export(&[&Event::ScopedVariablesUpdated {
                instance_key,
                scope_key,
                variables: std::collections::HashMap::from([(
                    "amount".to_string(),
                    Value::Int(99),
                )]),
            }])
            .unwrap();
        let after = store
            .instance_variables(instance_key)
            .into_iter()
            .find(|r| r.scope_key == scope_key && r.name == "amount")
            .unwrap();
        assert_eq!(after.key, before, "upsert keeps the variable key");
        assert_eq!(after.value, "99");
    }
}

#[cfg(test)]
mod decision_deletion_tests {
    use nanobpmn_engine_core::dmn::{DecisionType, EvaluatedDecision};
    use nanobpmn_engine_core::{Event, Value};

    use super::ReadStore;

    /// Root decision definition key every [`evaluated_event`] evaluates.
    const ROOT_DECISION_KEY: u64 = 9_000;

    /// A DecisionEvaluated for process instance `instance_key` (`0` =
    /// standalone) with its own minted evaluation key `eval_key`, carrying `n`
    /// evaluated decisions stamped with their definition key/version (so it
    /// projects `n` decision-instance rows).
    fn evaluated_event(instance_key: u64, eval_key: u64, n: usize) -> Event {
        let evaluated_decisions = (0..n)
            .map(|i| EvaluatedDecision {
                decision_id: format!("d{i}"),
                decision_name: format!("Decision {i}"),
                decision_type: DecisionType::DecisionTable,
                decision_output: Value::Int(i as i64),
                evaluated_inputs: Vec::new(),
                matched_rules: Vec::new(),
                decision_key: ROOT_DECISION_KEY + i as u64,
                decision_version: 1,
            })
            .collect();
        Event::DecisionEvaluated {
            instance_key,
            element_instance_key: if instance_key == 0 {
                0
            } else {
                instance_key + 1
            },
            element_id: if instance_key == 0 {
                String::new()
            } else {
                "brt".to_string()
            },
            decision_key: ROOT_DECISION_KEY,
            decision_id: "d0".to_string(),
            decision_output: Value::Int(0),
            evaluated_decisions,
            evaluated_at: 123,
            decision_evaluation_key: eval_key,
            failure: None,
            decision_requirements_key: 77,
            decision_requirements_id: "drg".to_string(),
        }
    }

    /// The same evaluation as a journal written before #1292 records it: no
    /// evaluation key, no DRG, unstamped decisions.
    fn legacy_evaluated_event(instance_key: u64, n: usize) -> Event {
        let Event::DecisionEvaluated {
            instance_key,
            element_instance_key,
            element_id,
            decision_key,
            decision_id,
            decision_output,
            mut evaluated_decisions,
            evaluated_at,
            ..
        } = evaluated_event(instance_key, 0, n)
        else {
            unreachable!()
        };
        for d in &mut evaluated_decisions {
            d.decision_key = 0;
            d.decision_version = 0;
        }
        Event::DecisionEvaluated {
            instance_key,
            element_instance_key,
            element_id,
            decision_key,
            decision_id,
            decision_output,
            evaluated_decisions,
            evaluated_at,
            decision_evaluation_key: 0,
            failure: None,
            decision_requirements_key: 0,
            decision_requirements_id: String::new(),
        }
    }

    /// #1292: two evaluations of the same decision in the same instance are
    /// two decision instances (they used to share the definition-keyed rows).
    #[test]
    fn repeat_evaluations_of_one_decision_are_distinct_decision_instances() {
        let store = ReadStore::open(None).unwrap();
        store
            .export(&[&evaluated_event(5, 100, 2), &evaluated_event(5, 200, 2)])
            .unwrap();
        let first = store.decision_instances_by_evaluation_key(100);
        let second = store.decision_instances_by_evaluation_key(200);
        assert_eq!((first.len(), second.len()), (2, 2));
        let keys: Vec<_> = first
            .iter()
            .chain(&second)
            .map(|r| r.eval_instance_key.as_str())
            .collect();
        assert_eq!(keys, ["100-1", "100-2", "200-1", "200-2"]);
        assert!(
            first
                .iter()
                .chain(&second)
                .all(|r| r.root_decision_key == ROOT_DECISION_KEY && r.state == "EVALUATED")
        );
    }

    /// Rows report the definition the engine stamped (the evaluated DRG
    /// version), never the latest deployment of the id.
    #[test]
    fn rows_report_the_stamped_definition_and_drg() {
        let store = ReadStore::open(None).unwrap();
        store.export(&[&evaluated_event(5, 100, 2)]).unwrap();
        let rows = store.decision_instances_by_evaluation_key(100);
        let got: Vec<_> = rows
            .iter()
            .map(|r| {
                (
                    r.decision_key,
                    r.version,
                    r.decision_requirements_key,
                    r.decision_requirements_id.as_str(),
                )
            })
            .collect();
        assert_eq!(
            got,
            [
                (ROOT_DECISION_KEY, 1, 77, "drg"),
                (ROOT_DECISION_KEY + 1, 1, 77, "drg")
            ]
        );
    }

    /// A failed evaluation's trail ends with the decision that failed: only
    /// that row is FAILED and carries the failure (Zeebe exporter parity).
    #[test]
    fn failed_evaluation_marks_only_the_last_row_failed() {
        let store = ReadStore::open(None).unwrap();
        let Event::DecisionEvaluated {
            instance_key,
            element_instance_key,
            element_id,
            decision_key,
            decision_id,
            decision_output,
            evaluated_decisions,
            evaluated_at,
            decision_evaluation_key,
            decision_requirements_key,
            decision_requirements_id,
            ..
        } = evaluated_event(5, 100, 2)
        else {
            unreachable!()
        };
        store
            .export(&[&Event::DecisionEvaluated {
                instance_key,
                element_instance_key,
                element_id,
                decision_key,
                decision_id,
                decision_output,
                evaluated_decisions,
                evaluated_at,
                decision_evaluation_key,
                failure: Some(nanobpmn_engine_core::dmn::EvaluationFailure {
                    message: "boom".to_string(),
                    failed_decision_id: "d1".to_string(),
                }),
                decision_requirements_key,
                decision_requirements_id,
            }])
            .unwrap();
        let rows = store.decision_instances_by_evaluation_key(100);
        let got: Vec<_> = rows
            .iter()
            .map(|r| (r.state.as_str(), r.evaluation_failure.as_deref()))
            .collect();
        assert_eq!(got, [("EVALUATED", None), ("FAILED", Some("boom"))]);
    }

    /// A standalone evaluation (no process instance) is a decision instance too.
    #[test]
    fn standalone_evaluation_projects_instance_less_rows() {
        let store = ReadStore::open(None).unwrap();
        store.export(&[&evaluated_event(0, 300, 1)]).unwrap();
        let rows = store.decision_instances_by_evaluation_key(300);
        assert_eq!(rows.len(), 1);
        assert_eq!((rows[0].instance_key, rows[0].element_instance_key), (0, 0));
        assert_eq!(rows[0].process_definition_key, "");
        assert_eq!(rows[0].business_id, None);
        // ...and it is deletable through its own evaluation key.
        store
            .export(&[&Event::DecisionInstanceDeleted {
                instance_key: 0,
                decision_evaluation_key: 300,
            }])
            .unwrap();
        assert!(store.decision_instances_by_evaluation_key(300).is_empty());
    }

    /// Pre-#1292 journals: an evaluation is identified by its element instance
    /// key, and a legacy deletion (which named the root decision *definition*
    /// key) retracts only the owning instance's rows of that decision.
    #[test]
    fn legacy_records_key_by_element_instance_and_delete_per_instance() {
        let store = ReadStore::open(None).unwrap();
        store
            .export(&[&legacy_evaluated_event(5, 2), &legacy_evaluated_event(9, 1)])
            .unwrap();
        // Element instance keys are instance_key + 1.
        assert_eq!(store.decision_instances_by_evaluation_key(6).len(), 2);
        assert!(store.decision_instance("6-1").is_some());
        assert_eq!(store.decision_instances_by_evaluation_key(10).len(), 1);
        store
            .export(&[&Event::DecisionInstanceDeleted {
                instance_key: 5,
                decision_evaluation_key: ROOT_DECISION_KEY,
            }])
            .unwrap();
        assert!(store.decision_instances_by_evaluation_key(6).is_empty());
        assert_eq!(
            store.decision_instances_by_evaluation_key(10).len(),
            1,
            "a legacy deletion stays within its instance"
        );
    }

    /// Every artifact snapshots the owning instance's `businessId` when it is
    /// created: a later assignment must NOT retroactively enrich artifacts that
    /// already existed (Camunda 8.10 contract, #1295 review).
    #[test]
    fn artifacts_snapshot_the_business_id_current_at_their_creation() {
        use nanobpmn_engine_core::MessageSubscriptionKind;
        let store = ReadStore::open(None).unwrap();
        let job = |job_key| Event::JobCreated {
            job_key,
            instance_key: 7,
            element_instance_key: 70,
            element_id: "task".into(),
            job_type: "t".into(),
            created_at: 0,
            priority: 0,
            retries: 3,
        };
        let task = |user_task_key| Event::UserTaskCreated {
            user_task_key,
            instance_key: 7,
            element_instance_key: 71,
            element_id: "ut".into(),
            created_at: 0,
            assignee: None,
            candidate_groups: Vec::new(),
            candidate_users: Vec::new(),
            due_date: None,
            follow_up_date: None,
            priority: 50,
            form_key: None,
            external_form_reference: None,
        };
        let sub = |subscription_key| Event::MessageSubscriptionCreated {
            subscription_key,
            instance_key: 7,
            element_instance_key: 72,
            element_id: "catch".into(),
            message_name: "m".into(),
            correlation_key: "k".into(),
            kind: MessageSubscriptionKind::IntermediateCatch,
        };
        let correlate = |subscription_key, message_key| Event::MessageCorrelated {
            subscription_key,
            message_key,
            instance_key: 7,
            element_instance_key: 72,
            element_id: "catch".into(),
        };
        let created = Event::ProcessInstanceCreated {
            instance_key: 7,
            process_id: "p".into(),
            variables: std::collections::HashMap::new(),
            created_at: 0,
            tags: Vec::new(),
            business_id: None,
            process_definition_key: 0,
            version: 0,
            parent_process_instance_key: None,
            parent_element_instance_key: None,
        };
        // Before the assignment: one of each artifact, plus a correlation.
        store
            .export(&[
                &created,
                &job(1),
                &task(2),
                &sub(3),
                &sub(4),
                &correlate(3, 30),
                &evaluated_event(7, 100, 1),
            ])
            .unwrap();
        store
            .export(&[&Event::ProcessInstanceBusinessIdAssigned {
                instance_key: 7,
                business_id: "order-9".into(),
            }])
            .unwrap();
        // After it: one more of each.
        store
            .export(&[
                &job(11),
                &task(12),
                &sub(13),
                &correlate(4, 40),
                &evaluated_event(7, 200, 1),
            ])
            .unwrap();

        let bid = |v: Option<String>| v;
        let order = Some("order-9".to_string());
        let jobs = store.jobs();
        let job_bid = |k| {
            bid(jobs
                .iter()
                .find(|j| j.key == k)
                .unwrap()
                .business_id
                .clone())
        };
        assert_eq!((job_bid(1), job_bid(11)), (None, order.clone()));
        let tasks = store.user_tasks();
        let task_bid = |k| {
            bid(tasks
                .iter()
                .find(|t| t.key == k)
                .unwrap()
                .business_id
                .clone())
        };
        assert_eq!((task_bid(2), task_bid(12)), (None, order.clone()));
        let subs = store.message_subscriptions();
        let sub_bid = |k| {
            bid(subs
                .iter()
                .find(|s| s.subscription_key == k)
                .unwrap()
                .business_id
                .clone())
        };
        // Sub 4 was opened before the assignment (it correlated after it).
        assert_eq!((sub_bid(13), subs.len()), (order.clone(), 1));
        let corr = store.correlated_message_subscriptions();
        let corr_bid = |k| {
            bid(corr
                .iter()
                .find(|c| c.message_key == k)
                .unwrap()
                .business_id
                .clone())
        };
        // A correlation record is created when the message correlates.
        assert_eq!((corr_bid(30), corr_bid(40)), (None, order.clone()));
        let dec_bid = |k| {
            store.decision_instances_by_evaluation_key(k)[0]
                .business_id
                .clone()
        };
        assert_eq!((dec_bid(100), dec_bid(200)), (None, order));
    }

    #[test]
    fn decision_instance_deleted_retracts_all_rows_of_the_evaluation() {
        let store = ReadStore::open(None).unwrap();
        // Two evaluations: eval_key 100 (2 decisions) and eval_key 200 (1 decision).
        store.export(&[&evaluated_event(5, 100, 2)]).unwrap();
        store.export(&[&evaluated_event(9, 200, 1)]).unwrap();

        assert_eq!(store.decision_instances_by_evaluation_key(100).len(), 2);
        assert!(store.decision_instance("100-1").is_some());
        assert!(store.decision_instance("100-2").is_some());
        assert_eq!(store.decision_instances_by_evaluation_key(200).len(), 1);

        // Delete evaluation 100: both of its rows go, evaluation 200 is untouched.
        store
            .export(&[&Event::DecisionInstanceDeleted {
                instance_key: 5,
                decision_evaluation_key: 100,
            }])
            .unwrap();

        assert!(store.decision_instances_by_evaluation_key(100).is_empty());
        assert!(store.decision_instance("100-1").is_none());
        assert!(store.decision_instance("100-2").is_none());
        assert_eq!(
            store.decision_instances_by_evaluation_key(200).len(),
            1,
            "deleting one evaluation must not touch another"
        );
    }

    #[test]
    fn decision_instance_deleted_is_idempotent() {
        let store = ReadStore::open(None).unwrap();
        store.export(&[&evaluated_event(5, 100, 2)]).unwrap();

        let del = Event::DecisionInstanceDeleted {
            instance_key: 5,
            decision_evaluation_key: 100,
        };
        store.export(&[&del]).unwrap();
        // Re-delivery (replay/broadcast) of the same deletion is inert, not an error.
        store.export(&[&del]).unwrap();
        // A deletion for an evaluation that never existed is also a harmless no-op.
        store
            .export(&[&Event::DecisionInstanceDeleted {
                instance_key: 7,
                decision_evaluation_key: 999,
            }])
            .unwrap();

        assert!(store.decision_instances_by_evaluation_key(100).is_empty());
    }
}

#[cfg(test)]
mod element_instance_tests {
    use std::collections::HashMap;

    use nanobpmn_engine_core::{Event, JobState, Key, ProcessBuilder};

    use super::{ElementInstanceState, ReadStore};

    const DEF_KEY: u64 = 500;
    const INST: u64 = 1000;
    const TASK_EI: u64 = 1001;

    /// Deploys a process `p` with a named service task `t`, so the projector can
    /// resolve the task's `type`/`elementName` from `definition_elements`.
    fn deploy() -> Event {
        let def = ProcessBuilder::new("p")
            .start_event("s")
            .service_task("t", "worker")
            .with_name("t", "My Task")
            .end_event("e")
            .connect("s", "t")
            .connect("t", "e")
            .build()
            .unwrap();
        Event::ProcessDeployed {
            deployment_key: 1,
            process_definition_key: DEF_KEY,
            version: 1,
            process: def,
        }
    }

    fn created() -> Event {
        Event::ProcessInstanceCreated {
            instance_key: INST,
            process_id: "p".to_string(),
            variables: HashMap::new(),
            created_at: 1,
            tags: Vec::new(),
            business_id: None,
            process_definition_key: 0,
            version: 0,
            parent_process_instance_key: None,
            parent_element_instance_key: None,
        }
    }

    #[test]
    fn projects_call_activity_parent_linkage_into_the_process_instance_row() {
        // The C8 `parentProcessInstanceKey` / `parentElementInstanceKey` surface is
        // consumer-facing, so a regression that dropped these columns from the
        // projection would be silent. Assert a `ProcessInstanceCreated` carrying
        // parent linkage round-trips into the row (and that the default top-level
        // create leaves both `None`).
        let store = ReadStore::open(None).unwrap();
        const CHILD: u64 = 2000;
        const CALL_EI: u64 = 1002;
        store
            .export(&[
                &deploy(),
                &created(),
                &Event::ProcessInstanceCreated {
                    instance_key: CHILD,
                    process_id: "p".to_string(),
                    variables: HashMap::new(),
                    created_at: 2,
                    tags: Vec::new(),
                    business_id: None,
                    process_definition_key: DEF_KEY,
                    version: 1,
                    parent_process_instance_key: Some(INST),
                    parent_element_instance_key: Some(CALL_EI),
                },
            ])
            .unwrap();

        let child = store.process_instance(CHILD).expect("child row exists");
        assert_eq!(child.parent_process_instance_key, Some(INST));
        assert_eq!(child.parent_element_instance_key, Some(CALL_EI));

        let parent = store.process_instance(INST).expect("parent row exists");
        assert_eq!(parent.parent_process_instance_key, None);
        assert_eq!(parent.parent_element_instance_key, None);
    }

    #[test]
    fn projects_activate_complete_terminate_and_incident_linkage() {
        let store = ReadStore::open(None).unwrap();
        store.export(&[&deploy(), &created()]).unwrap();

        // Activation materializes an ACTIVE row with resolved type + name.
        store
            .export(&[
                &Event::ElementActivating {
                    instance_key: INST,
                    element_instance_key: TASK_EI,
                    element_id: "t".to_string(),
                },
                &Event::ElementActivated {
                    instance_key: INST,
                    element_instance_key: TASK_EI,
                    element_id: "t".to_string(),
                    scope: 0,
                },
            ])
            .unwrap();

        let row = store.element_instance(TASK_EI).expect("row exists");
        assert_eq!(row.instance_key, INST);
        assert_eq!(row.element_id, "t");
        assert_eq!(row.element_name.as_deref(), Some("My Task"));
        assert_eq!(row.element_type, "SERVICE_TASK");
        assert_eq!(row.state, ElementInstanceState::Active);
        assert_eq!(row.process_definition_key, DEF_KEY.to_string());
        assert!(row.end_date_ms.is_none());
        assert!(!row.has_incident);

        // An incident on the element links back by key and flips has_incident.
        store
            .export(&[&Event::IncidentRaised {
                incident_key: 7,
                instance_key: INST,
                element_instance_key: TASK_EI,
                element_id: "t".to_string(),
                kind: nanobpmn_engine_core::IncidentKind::JobNoRetries,
                reason: "boom".to_string(),
                job_key: Some(42),
                created_at: 5,
                redrive: None,
            }])
            .unwrap();
        let row = store.element_instance(TASK_EI).unwrap();
        assert!(row.has_incident);
        assert_eq!(row.incident_key, Some(7));

        // Resolving the incident clears the flag.
        store
            .export(&[&Event::IncidentResolved {
                incident_key: 7,
                instance_key: INST,
                job_key: Some(42),
                resolved_at: 6,
                operation_reference: None,
            }])
            .unwrap();
        let row = store.element_instance(TASK_EI).unwrap();
        assert!(!row.has_incident);
        assert_eq!(row.incident_key, None);

        // Completion transitions to COMPLETED and stamps an end date.
        store
            .export(&[&Event::ElementCompleted {
                instance_key: INST,
                element_instance_key: TASK_EI,
                element_id: "t".to_string(),
            }])
            .unwrap();
        let row = store.element_instance(TASK_EI).unwrap();
        assert_eq!(row.state, ElementInstanceState::Completed);
        assert!(row.end_date_ms.is_some());
    }

    #[test]
    fn terminating_the_process_terminates_still_active_elements() {
        let store = ReadStore::open(None).unwrap();
        store.export(&[&deploy(), &created()]).unwrap();
        store
            .export(&[&Event::ElementActivated {
                instance_key: INST,
                element_instance_key: TASK_EI,
                element_id: "t".to_string(),
                scope: 0,
            }])
            .unwrap();

        store
            .export(&[&Event::ProcessInstanceTerminated { instance_key: INST }])
            .unwrap();
        let row = store.element_instance(TASK_EI).unwrap();
        assert_eq!(row.state, ElementInstanceState::Terminated);
        assert!(row.end_date_ms.is_some());
    }

    #[test]
    fn unresolved_element_type_falls_back_to_unknown() {
        let store = ReadStore::open(None).unwrap();
        store.export(&[&deploy(), &created()]).unwrap();
        // An element id not present in the deployed model (e.g. an inlined
        // call-activity child) resolves to UNKNOWN with no name.
        store
            .export(&[&Event::ElementActivated {
                instance_key: INST,
                element_instance_key: 2002,
                element_id: "mystery".to_string(),
                scope: 0,
            }])
            .unwrap();
        let row = store.element_instance(2002).unwrap();
        assert_eq!(row.element_type, "UNKNOWN");
        assert_eq!(row.element_name, None);
    }

    /// Gap #9 (issue #614): the synthetic ad-hoc inner instance
    /// (`<container>#innerInstance`) is not in the deployed model, so it must be
    /// resolved to the `AD_HOC_SUB_PROCESS_INNER_INSTANCE` element type by its id
    /// postfix rather than falling back to UNKNOWN — matching Zeebe's read model.
    #[test]
    fn adhoc_inner_instance_resolves_to_its_element_type() {
        let store = ReadStore::open(None).unwrap();
        store.export(&[&deploy(), &created()]).unwrap();
        let inner_id = nanobpmn_engine_core::adhoc_inner_instance_id("agent");
        store
            .export(&[&Event::ElementActivated {
                instance_key: INST,
                element_instance_key: 2003,
                element_id: inner_id,
                scope: 0,
            }])
            .unwrap();
        let row = store.element_instance(2003).unwrap();
        assert_eq!(row.element_type, "AD_HOC_SUB_PROCESS_INNER_INSTANCE");
        assert_eq!(row.element_name, None);
    }

    /// #917: `ElementKind::CompensationThrowEvent` covers both the
    /// `<intermediateThrowEvent>` and `<endEvent>` compensation-throw flavours,
    /// disambiguated at runtime purely by outgoing-flow emptiness. The read model
    /// must expose a terminal (no-outgoing-flow) compensation throw as
    /// `END_EVENT`, and one with an outgoing flow as `INTERMEDIATE_THROW_EVENT`,
    /// rather than hard-coding both to `INTERMEDIATE_THROW_EVENT`.
    #[test]
    fn compensation_throw_end_event_flavour_classifies_as_end_event() {
        const COMP_DEF_KEY: u64 = 600;
        const COMP_INST: u64 = 1100;
        let def = ProcessBuilder::new("comp")
            .start_event("s")
            .compensation_throw_event("mid")
            .compensation_throw_event("term")
            .connect("s", "mid")
            .connect("mid", "term")
            .build()
            .unwrap();
        let store = ReadStore::open(None).unwrap();
        store
            .export(&[
                &Event::ProcessDeployed {
                    deployment_key: 2,
                    process_definition_key: COMP_DEF_KEY,
                    version: 1,
                    process: def,
                },
                &Event::ProcessInstanceCreated {
                    instance_key: COMP_INST,
                    process_id: "comp".to_string(),
                    variables: HashMap::new(),
                    created_at: 1,
                    tags: Vec::new(),
                    business_id: None,
                    process_definition_key: COMP_DEF_KEY,
                    version: 1,
                    parent_process_instance_key: None,
                    parent_element_instance_key: None,
                },
                // Intermediate flavour: has an outgoing flow.
                &Event::ElementActivated {
                    instance_key: COMP_INST,
                    element_instance_key: 3001,
                    element_id: "mid".to_string(),
                    scope: 0,
                },
                // Terminal flavour: no outgoing flow.
                &Event::ElementActivated {
                    instance_key: COMP_INST,
                    element_instance_key: 3002,
                    element_id: "term".to_string(),
                    scope: 0,
                },
            ])
            .unwrap();

        assert_eq!(
            store.element_instance(3001).unwrap().element_type,
            "INTERMEDIATE_THROW_EVENT"
        );
        assert_eq!(
            store.element_instance(3002).unwrap().element_type,
            "END_EVENT"
        );
    }

    /// #1173: `ElementKind::EscalationThrowEvent` likewise covers both the
    /// `<intermediateThrowEvent>` and `<endEvent>` escalation-throw flavours,
    /// disambiguated purely by outgoing-flow emptiness. The read model must
    /// expose a terminal (no-outgoing-flow) escalation throw as `END_EVENT` and
    /// one with an outgoing flow as `INTERMEDIATE_THROW_EVENT`, rather than
    /// hard-coding both to `INTERMEDIATE_THROW_EVENT`.
    #[test]
    fn escalation_throw_end_event_flavour_classifies_as_end_event() {
        const ESC_DEF_KEY: u64 = 601;
        const ESC_INST: u64 = 1101;
        let def = ProcessBuilder::new("esc")
            .start_event("s")
            .escalation_throw_event("mid", "OVERLOAD")
            .escalation_throw_event("term", "OVERLOAD")
            .connect("s", "mid")
            .connect("mid", "term")
            .build()
            .unwrap();
        let store = ReadStore::open(None).unwrap();
        store
            .export(&[
                &Event::ProcessDeployed {
                    deployment_key: 3,
                    process_definition_key: ESC_DEF_KEY,
                    version: 1,
                    process: def,
                },
                &Event::ProcessInstanceCreated {
                    instance_key: ESC_INST,
                    process_id: "esc".to_string(),
                    variables: HashMap::new(),
                    created_at: 1,
                    tags: Vec::new(),
                    business_id: None,
                    process_definition_key: ESC_DEF_KEY,
                    version: 1,
                    parent_process_instance_key: None,
                    parent_element_instance_key: None,
                },
                // Intermediate flavour: has an outgoing flow.
                &Event::ElementActivated {
                    instance_key: ESC_INST,
                    element_instance_key: 3101,
                    element_id: "mid".to_string(),
                    scope: 0,
                },
                // Terminal flavour: no outgoing flow.
                &Event::ElementActivated {
                    instance_key: ESC_INST,
                    element_instance_key: 3102,
                    element_id: "term".to_string(),
                    scope: 0,
                },
            ])
            .unwrap();

        assert_eq!(
            store.element_instance(3101).unwrap().element_type,
            "INTERMEDIATE_THROW_EVENT"
        );
        assert_eq!(
            store.element_instance(3102).unwrap().element_type,
            "END_EVENT"
        );
    }

    /// `active_element_instances` returns only the `Active` rows for the given
    /// instance — the live token positions the explorer overlays. Completed or
    /// terminated elements, and elements belonging to other instances, are
    /// excluded.
    #[test]
    fn active_element_instances_returns_only_active_rows_for_the_instance() {
        let store = ReadStore::open(None).unwrap();
        store.export(&[&deploy(), &created()]).unwrap();

        // Two elements activate on INST; one then completes.
        store
            .export(&[
                &Event::ElementActivated {
                    instance_key: INST,
                    element_instance_key: TASK_EI,
                    element_id: "t".to_string(),
                    scope: 0,
                },
                &Event::ElementActivated {
                    instance_key: INST,
                    element_instance_key: TASK_EI + 1,
                    element_id: "t".to_string(),
                    scope: 0,
                },
                &Event::ElementCompleted {
                    instance_key: INST,
                    element_instance_key: TASK_EI + 1,
                    element_id: "t".to_string(),
                },
            ])
            .unwrap();

        // A different instance's active element must not leak in.
        store
            .export(&[
                &Event::ProcessInstanceCreated {
                    instance_key: INST + 100,
                    process_id: "p".to_string(),
                    variables: HashMap::new(),
                    created_at: 1,
                    tags: Vec::new(),
                    business_id: None,
                    process_definition_key: 0,
                    version: 0,
                    parent_process_instance_key: None,
                    parent_element_instance_key: None,
                },
                &Event::ElementActivated {
                    instance_key: INST + 100,
                    element_instance_key: TASK_EI + 2,
                    element_id: "t".to_string(),
                    scope: 0,
                },
            ])
            .unwrap();

        let active = store.active_element_instances(INST);
        assert_eq!(active.len(), 1, "only the still-active element on INST");
        assert_eq!(active[0].element_instance_key, TASK_EI);
        assert!(
            active
                .iter()
                .all(|e| e.state == ElementInstanceState::Active && e.instance_key == INST)
        );
    }

    #[test]
    fn message_subscriptions_are_projected_and_dropped_on_correlate_and_terminate() {
        use nanobpmn_engine_core::MessageSubscriptionKind;
        let store = ReadStore::open(None).unwrap();
        store.export(&[&deploy(), &created()]).unwrap();

        // An instance-scoped subscription (element_instance_key != 0) materializes
        // a MESSAGE wait-state row.
        store
            .export(&[&Event::MessageSubscriptionCreated {
                subscription_key: 3001,
                instance_key: INST,
                element_instance_key: TASK_EI,
                element_id: "await".to_string(),
                message_name: "OrderPlaced".to_string(),
                correlation_key: "A1".to_string(),
                kind: MessageSubscriptionKind::IntermediateCatch,
            }])
            .unwrap();
        let subs = store.message_subscriptions();
        assert_eq!(subs.len(), 1);
        assert_eq!(subs[0].subscription_key, 3001);
        assert_eq!(subs[0].element_instance_key, TASK_EI);
        assert_eq!(subs[0].message_name, "OrderPlaced");
        assert_eq!(subs[0].correlation_key, "A1");

        // A subscription with no element instance (element_instance_key == 0,
        // e.g. a message-start subscription) is not an element-instance wait
        // state and must not be projected.
        store
            .export(&[&Event::MessageSubscriptionCreated {
                subscription_key: 3002,
                instance_key: INST,
                element_instance_key: 0,
                element_id: "start".to_string(),
                message_name: "Kickoff".to_string(),
                correlation_key: String::new(),
                kind: MessageSubscriptionKind::IntermediateCatch,
            }])
            .unwrap();
        assert_eq!(store.message_subscriptions().len(), 1);

        // Correlation releases the token and drops the subscription.
        store
            .export(&[&Event::MessageCorrelated {
                subscription_key: 3001,
                message_key: 9,
                instance_key: INST,
                element_instance_key: TASK_EI,
                element_id: "await".to_string(),
            }])
            .unwrap();
        assert!(store.message_subscriptions().is_empty());
        // Correlation is recorded in the history read model, capturing the message
        // name and correlation key from the (now-dropped) open subscription row.
        let corr = store.correlated_message_subscriptions();
        assert_eq!(corr.len(), 1);
        assert_eq!(corr[0].message_key, 9);
        assert_eq!(corr[0].subscription_key, 3001);
        assert_eq!(corr[0].instance_key, INST);
        assert_eq!(corr[0].element_instance_key, TASK_EI);
        assert_eq!(corr[0].message_name, "OrderPlaced");
        assert_eq!(corr[0].correlation_key, "A1");

        // A subscription still open when the process terminates is cleaned up.
        store
            .export(&[&Event::MessageSubscriptionCreated {
                subscription_key: 3003,
                instance_key: INST,
                element_instance_key: TASK_EI,
                element_id: "await".to_string(),
                message_name: "OrderPlaced".to_string(),
                correlation_key: "A2".to_string(),
                kind: MessageSubscriptionKind::IntermediateCatch,
            }])
            .unwrap();
        assert_eq!(store.message_subscriptions().len(), 1);
        store
            .export(&[&Event::ProcessInstanceTerminated { instance_key: INST }])
            .unwrap();
        assert!(store.message_subscriptions().is_empty());

        // A remote correlation (multi-partition: the instance lives elsewhere)
        // settles the canonical subscription with RemoteMessageCorrelation and
        // must also clear the row.
        store
            .export(&[&Event::MessageSubscriptionCreated {
                subscription_key: 3004,
                instance_key: INST,
                element_instance_key: TASK_EI,
                element_id: "await".to_string(),
                message_name: "OrderPlaced".to_string(),
                correlation_key: "A3".to_string(),
                kind: MessageSubscriptionKind::IntermediateCatch,
            }])
            .unwrap();
        assert_eq!(store.message_subscriptions().len(), 1);
        store
            .export(&[&Event::RemoteMessageCorrelation {
                subscription_key: 3004,
                message_key: 10,
                instance_key: INST,
                element_instance_key: TASK_EI,
                element_id: "await".to_string(),
                kind: MessageSubscriptionKind::IntermediateCatch,
                variables: std::collections::HashMap::new(),
            }])
            .unwrap();
        assert!(store.message_subscriptions().is_empty());
    }

    #[test]
    fn migration_remaps_live_message_subscription_onto_target_element() {
        use nanobpmn_engine_core::MessageSubscriptionKind;
        // An instance waiting on a message catch event carries a live
        // `message_subscriptions` row. Migrating it must re-home that row's
        // `element_id` onto the mapped target element, alongside the token —
        // otherwise the read model points at the source element id the engine no
        // longer runs under.
        let store = ReadStore::open(None).unwrap();

        // Target definition `p2` carries the mapped target element `await2` as a
        // message intermediate catch event (matching the source token's
        // `IntermediateCatch` subscription kind and the realistic
        // `definition_elements` metadata/type), so its metadata is resolvable.
        let target = ProcessBuilder::new("p2")
            .start_event("s2")
            .message_intermediate_catch_event("await2", "OrderPlaced", "=orderId")
            .end_event("e2")
            .connect("s2", "await2")
            .connect("await2", "e2")
            .build()
            .unwrap();
        let target_key: u64 = 600;
        store
            .export(&[
                &deploy(),
                &created(),
                &Event::ProcessDeployed {
                    deployment_key: 2,
                    process_definition_key: target_key,
                    version: 3,
                    process: target,
                },
                &Event::MessageSubscriptionCreated {
                    subscription_key: 4001,
                    instance_key: INST,
                    element_instance_key: TASK_EI,
                    element_id: "await".to_string(),
                    message_name: "OrderPlaced".to_string(),
                    correlation_key: "A1".to_string(),
                    kind: MessageSubscriptionKind::IntermediateCatch,
                },
            ])
            .unwrap();
        assert_eq!(store.message_subscriptions()[0].element_id, "await");

        store
            .export(&[&Event::ProcessInstanceMigrated {
                instance_key: INST,
                target_process_id: "p2".to_string(),
                target_process_definition_key: target_key,
                element_mappings: vec![("await".to_string(), "await2".to_string())],
            }])
            .unwrap();

        // The subscription re-homes onto the target element id; the instance row
        // re-homes onto the target definition + its version (looked up loudly).
        let subs = store.message_subscriptions();
        assert_eq!(subs.len(), 1);
        assert_eq!(subs[0].element_id, "await2");
        let inst = store.process_instance(INST).unwrap();
        assert_eq!(inst.process_definition_id, "p2");
        assert_eq!(inst.process_definition_key, target_key.to_string());
        assert_eq!(inst.version, 3);
    }

    #[test]
    fn migration_remaps_parked_and_terminal_job_element_ids() {
        // The engine applier re-points `element_id` on *every* job the instance
        // owns — it loops all of `state.jobs` and never removes terminal rows —
        // so a parked (`Failed`, retries=0) or terminal (`Errored`) job is
        // remapped there too. The read model must mirror that, or a job row is
        // left pointing at the source `element_id` the engine no longer runs
        // under. This guards the whole defect class (any job state, not just the
        // two the projection used to allow-list) against silent drift.
        let store = ReadStore::open(None).unwrap();

        let target = ProcessBuilder::new("p2")
            .start_event("s2")
            .service_task("t2", "worker")
            .with_name("t2", "My Task 2")
            .end_event("e2")
            .connect("s2", "t2")
            .connect("t2", "e2")
            .build()
            .unwrap();
        let target_key: u64 = 600;

        store
            .export(&[
                &deploy(),
                &created(),
                &Event::ProcessDeployed {
                    deployment_key: 2,
                    process_definition_key: target_key,
                    version: 3,
                    process: target,
                },
                // A parked job (retries exhausted) on the source element `t`.
                &Event::JobCreated {
                    job_key: 7001,
                    instance_key: INST,
                    element_instance_key: TASK_EI,
                    element_id: "t".to_string(),
                    job_type: "worker".to_string(),
                    created_at: 1,
                    priority: 0,
                    retries: 1,
                },
                &Event::JobFailed {
                    job_key: 7001,
                    instance_key: INST,
                    retries: 0,
                    worker: None,
                    error_message: None,
                },
                // A terminal errored job on the same source element.
                &Event::JobCreated {
                    job_key: 7002,
                    instance_key: INST,
                    element_instance_key: TASK_EI,
                    element_id: "t".to_string(),
                    job_type: "worker".to_string(),
                    created_at: 1,
                    priority: 0,
                    retries: 1,
                },
                &Event::JobErrorThrown {
                    job_key: 7002,
                    instance_key: INST,
                    error_code: "BOOM".to_string(),
                    worker: None,
                    error_message: None,
                },
            ])
            .unwrap();
        let before: HashMap<Key, JobState> =
            store.jobs().into_iter().map(|j| (j.key, j.state)).collect();
        assert_eq!(before.get(&7001), Some(&JobState::Failed));
        assert_eq!(before.get(&7002), Some(&JobState::Errored));

        store
            .export(&[&Event::ProcessInstanceMigrated {
                instance_key: INST,
                target_process_id: "p2".to_string(),
                target_process_definition_key: target_key,
                element_mappings: vec![("t".to_string(), "t2".to_string())],
            }])
            .unwrap();

        // Both the parked and the terminal job re-home onto the target element
        // id and pick up the target definition identity — matching the engine.
        for job in store.jobs() {
            assert_eq!(
                job.element_id, "t2",
                "job {} left pointing at stale source element id",
                job.key
            );
            assert_eq!(job.process_definition_id, "p2");
            assert_eq!(job.process_definition_key, target_key.to_string());
        }
    }

    #[test]
    fn preserves_the_activating_worker_on_terminal_failed_and_errored_jobs() {
        // #959 — the read model must mirror the engine: a terminal, incident-bearing
        // job transition (`JobFailed` with 0 retries → Failed, `JobErrorThrown` →
        // Errored) retains the last activating `worker` so an incident joined by
        // `jobKey` can attribute the failure. A job that returns to the activatable
        // pool (retries remaining) drops its worker.
        let store = ReadStore::open(None).unwrap();
        store
            .export(&[
                &deploy(),
                &created(),
                // A job that fails terminally after activation by `w1`.
                &Event::JobCreated {
                    job_key: 8001,
                    instance_key: INST,
                    element_instance_key: TASK_EI,
                    element_id: "t".to_string(),
                    job_type: "worker".to_string(),
                    created_at: 1,
                    priority: 0,
                    retries: 1,
                },
                &Event::JobActivated {
                    job_key: 8001,
                    instance_key: INST,
                    worker: "w1".to_string(),
                    deadline: 60_000,
                    activated_at: Some(1),
                    lease_token: None,
                    durable: false,
                    fetch_variables: Vec::new(),
                },
                &Event::JobFailed {
                    job_key: 8001,
                    instance_key: INST,
                    retries: 0,
                    worker: Some("w1".to_string()),
                    error_message: None,
                },
                // A job that throws a terminal (uncaught) error after activation by `w2`.
                &Event::JobCreated {
                    job_key: 8002,
                    instance_key: INST,
                    element_instance_key: TASK_EI,
                    element_id: "t".to_string(),
                    job_type: "worker".to_string(),
                    created_at: 1,
                    priority: 0,
                    retries: 1,
                },
                &Event::JobActivated {
                    job_key: 8002,
                    instance_key: INST,
                    worker: "w2".to_string(),
                    deadline: 60_000,
                    activated_at: Some(1),
                    lease_token: None,
                    durable: false,
                    fetch_variables: Vec::new(),
                },
                &Event::JobErrorThrown {
                    job_key: 8002,
                    instance_key: INST,
                    error_code: "BOOM".to_string(),
                    worker: Some("w2".to_string()),
                    error_message: None,
                },
                // A job that fails with retries left, returning to the pool after `w3`.
                &Event::JobCreated {
                    job_key: 8003,
                    instance_key: INST,
                    element_instance_key: TASK_EI,
                    element_id: "t".to_string(),
                    job_type: "worker".to_string(),
                    created_at: 1,
                    priority: 0,
                    retries: 2,
                },
                &Event::JobActivated {
                    job_key: 8003,
                    instance_key: INST,
                    worker: "w3".to_string(),
                    deadline: 60_000,
                    activated_at: Some(1),
                    lease_token: None,
                    durable: false,
                    fetch_variables: Vec::new(),
                },
                &Event::JobFailed {
                    job_key: 8003,
                    instance_key: INST,
                    retries: 1,
                    worker: Some("w3".to_string()),
                    error_message: None,
                },
            ])
            .unwrap();

        let jobs: HashMap<Key, super::JobRow> =
            store.jobs().into_iter().map(|j| (j.key, j)).collect();

        let failed = &jobs[&8001];
        assert_eq!(failed.state, JobState::Failed);
        assert_eq!(failed.worker.as_deref(), Some("w1"));

        let errored = &jobs[&8002];
        assert_eq!(errored.state, JobState::Errored);
        assert_eq!(errored.worker.as_deref(), Some("w2"));

        let requeued = &jobs[&8003];
        assert_eq!(requeued.state, JobState::Created);
        assert_eq!(requeued.worker, None);
    }

    #[test]
    fn attributes_the_worker_on_terminal_jobs_from_the_event_without_a_projected_activation() {
        // #959 — regression guard for the *leader-local* activation path: under
        // leader-local activation `Journal::activate_jobs` never exports
        // `JobActivated`, so the read-model row's `worker` is still NULL when the
        // terminal event arrives. Preserving the existing (NULL) column was a
        // no-op there — `/v2/jobs` still showed an empty worker. The terminal
        // event now *carries* the activating worker, so the projection sets it
        // even with no preceding `JobActivated`, making the incident attributable.
        let store = ReadStore::open(None).unwrap();
        store
            .export(&[
                &deploy(),
                &created(),
                // Failed terminally — NO JobActivated was exported (leader-local).
                &Event::JobCreated {
                    job_key: 9001,
                    instance_key: INST,
                    element_instance_key: TASK_EI,
                    element_id: "t".to_string(),
                    job_type: "worker".to_string(),
                    created_at: 1,
                    priority: 0,
                    retries: 1,
                },
                &Event::JobFailed {
                    job_key: 9001,
                    instance_key: INST,
                    retries: 0,
                    worker: Some("host-a-senior".to_string()),
                    error_message: None,
                },
                // Errored terminally — again no JobActivated projection.
                &Event::JobCreated {
                    job_key: 9002,
                    instance_key: INST,
                    element_instance_key: TASK_EI,
                    element_id: "t".to_string(),
                    job_type: "worker".to_string(),
                    created_at: 1,
                    priority: 0,
                    retries: 1,
                },
                &Event::JobErrorThrown {
                    job_key: 9002,
                    instance_key: INST,
                    error_code: "BOOM".to_string(),
                    worker: Some("host-b-senior".to_string()),
                    error_message: None,
                },
            ])
            .unwrap();

        let jobs: HashMap<Key, super::JobRow> =
            store.jobs().into_iter().map(|j| (j.key, j)).collect();

        let failed = &jobs[&9001];
        assert_eq!(failed.state, JobState::Failed);
        assert_eq!(failed.worker.as_deref(), Some("host-a-senior"));

        let errored = &jobs[&9002];
        assert_eq!(errored.state, JobState::Errored);
        assert_eq!(errored.worker.as_deref(), Some("host-b-senior"));
    }

    #[test]
    fn attributes_the_worker_on_a_successful_completion_including_a_husk() {
        // #1191 — closes the asymmetric attribution gap: a *successful* completion
        // must retain the activating worker on the read-model leader-local path
        // too (where `JobActivated` is never exported), mirroring the terminal
        // `JobFailed` / `JobErrorThrown` rows. This is what makes a **husk** — a
        // COMPLETED job that minted no AgentInstance — attributable to its worker
        // via the `AgentInstance.jobKey → completed Job.worker` join.
        let store = ReadStore::open(None).unwrap();
        store
            .export(&[
                &deploy(),
                &created(),
                // Completed — NO JobActivated was exported (leader-local path).
                &Event::JobCreated {
                    job_key: 9101,
                    instance_key: INST,
                    element_instance_key: TASK_EI,
                    element_id: "t".to_string(),
                    job_type: "review-round".to_string(),
                    created_at: 1,
                    priority: 0,
                    retries: 1,
                },
                &Event::JobCompleted {
                    job_key: 9101,
                    instance_key: INST,
                    created_at: 1,
                    job_type: "review-round".to_string(),
                    worker: Some("host-a-senior".to_string()),
                },
                // A completion with no activating worker leaves it NULL, not "".
                &Event::JobCreated {
                    job_key: 9102,
                    instance_key: INST,
                    element_instance_key: TASK_EI,
                    element_id: "t".to_string(),
                    job_type: "review-round".to_string(),
                    created_at: 1,
                    priority: 0,
                    retries: 1,
                },
                &Event::JobCompleted {
                    job_key: 9102,
                    instance_key: INST,
                    created_at: 1,
                    job_type: "review-round".to_string(),
                    worker: None,
                },
            ])
            .unwrap();

        let jobs: HashMap<Key, super::JobRow> =
            store.jobs().into_iter().map(|j| (j.key, j)).collect();

        let husk = &jobs[&9101];
        assert_eq!(husk.state, JobState::Completed);
        assert_eq!(husk.worker.as_deref(), Some("host-a-senior"));

        let no_worker = &jobs[&9102];
        assert_eq!(no_worker.state, JobState::Completed);
        assert_eq!(no_worker.worker, None);
    }

    #[test]
    fn an_empty_worker_on_a_terminal_event_projects_to_null_not_an_empty_attribution() {
        // #1191 — an *empty* worker string on a persisted terminal event
        // (`Some("")`, e.g. replayed from before the engine reducer normalized it
        // at the source) must NOT be stored as an attribution: under
        // `COALESCE(?, worker)` a non-NULL `""` would overwrite / pin the row to
        // an empty worker instead of leaving it NULL. The projection normalizes
        // `Some("")` to `None` so the row stays NULL across all three terminal
        // paths (completed / failed / errored).
        let store = ReadStore::open(None).unwrap();
        store
            .export(&[
                &deploy(),
                &created(),
                &Event::JobCreated {
                    job_key: 9201,
                    instance_key: INST,
                    element_instance_key: TASK_EI,
                    element_id: "t".to_string(),
                    job_type: "review-round".to_string(),
                    created_at: 1,
                    priority: 0,
                    retries: 1,
                },
                &Event::JobCompleted {
                    job_key: 9201,
                    instance_key: INST,
                    created_at: 1,
                    job_type: "review-round".to_string(),
                    worker: Some(String::new()),
                },
                &Event::JobCreated {
                    job_key: 9202,
                    instance_key: INST,
                    element_instance_key: TASK_EI,
                    element_id: "t".to_string(),
                    job_type: "worker".to_string(),
                    created_at: 1,
                    priority: 0,
                    retries: 1,
                },
                &Event::JobFailed {
                    job_key: 9202,
                    instance_key: INST,
                    retries: 0,
                    worker: Some(String::new()),
                    error_message: None,
                },
                &Event::JobCreated {
                    job_key: 9203,
                    instance_key: INST,
                    element_instance_key: TASK_EI,
                    element_id: "t".to_string(),
                    job_type: "worker".to_string(),
                    created_at: 1,
                    priority: 0,
                    retries: 1,
                },
                &Event::JobErrorThrown {
                    job_key: 9203,
                    instance_key: INST,
                    error_code: "BOOM".to_string(),
                    worker: Some(String::new()),
                    error_message: None,
                },
            ])
            .unwrap();

        let jobs: HashMap<Key, super::JobRow> =
            store.jobs().into_iter().map(|j| (j.key, j)).collect();

        let completed = &jobs[&9201];
        assert_eq!(completed.state, JobState::Completed);
        assert_eq!(completed.worker, None);

        let failed = &jobs[&9202];
        assert_eq!(failed.state, JobState::Failed);
        assert_eq!(failed.worker, None);

        let errored = &jobs[&9203];
        assert_eq!(errored.state, JobState::Errored);
        assert_eq!(errored.worker, None);
    }

    #[test]
    fn an_empty_durable_activation_worker_is_not_pinned_onto_the_completed_row() {
        // #1191 — the terminal `COALESCE(?, worker)` only leaves the completed row
        // NULL if the *activation* projection did not already write an empty
        // string. A durable activation carrying `worker: ""` (an explicitly
        // supplied empty worker, or replay of a `JobActivated` written before the
        // engine reducer normalized it) must project to SQL NULL, not `""` —
        // otherwise the completion, which now normalizes its own capture to
        // `worker: None`, would `COALESCE(NULL, "")` and pin the completed row to
        // an empty attribution. Guard the activation → completion path end to end.
        let store = ReadStore::open(None).unwrap();
        store
            .export(&[
                &deploy(),
                &created(),
                &Event::JobCreated {
                    job_key: 9301,
                    instance_key: INST,
                    element_instance_key: TASK_EI,
                    element_id: "t".to_string(),
                    job_type: "review-round".to_string(),
                    created_at: 1,
                    priority: 0,
                    retries: 1,
                },
                // Durable activation with an explicitly-empty worker: the
                // projection must land NULL, not `""`.
                &Event::JobActivated {
                    job_key: 9301,
                    instance_key: INST,
                    worker: String::new(),
                    deadline: 60_000,
                    activated_at: Some(1),
                    lease_token: None,
                    durable: true,
                    fetch_variables: Vec::new(),
                },
                // Completion carries no attribution (the engine normalized its
                // capture to `None`); `COALESCE(NULL, worker)` must not resurrect
                // an empty activation string.
                &Event::JobCompleted {
                    job_key: 9301,
                    instance_key: INST,
                    created_at: 1,
                    job_type: "review-round".to_string(),
                    worker: None,
                },
            ])
            .unwrap();

        let jobs: HashMap<Key, super::JobRow> =
            store.jobs().into_iter().map(|j| (j.key, j)).collect();
        let completed = &jobs[&9301];
        assert_eq!(completed.state, JobState::Completed);
        assert_eq!(completed.worker, None);
    }

    #[test]
    fn a_snapshot_seeded_empty_worker_is_not_pinned_onto_the_completed_row() {
        use nanobpmn_engine_core::{Job, JobKind, State};

        // #1191 — below-compaction-floor recovery seeds job rows directly from
        // the engine snapshot via `project_engine_state`, NOT from the
        // event-driven projection. A snapshot written before the engine reducer
        // normalized empty workers can hold `Job.worker == Some("")` for a job
        // activated with an explicitly-empty worker. Seeding that verbatim would
        // write `""` into the row; a subsequent `JobCompleted` (whose own capture
        // is now normalized to `None`) would then `COALESCE(NULL, "")` and pin the
        // completed row to a bogus empty attribution. The seeding binding must
        // normalize through `worker_attribution` exactly like the event path, so
        // the seeded row lands SQL NULL and the terminal COALESCE stays clean.
        let mut state = State::default();
        state.jobs.insert(
            9401,
            Job {
                key: 9401,
                instance_key: INST,
                element_instance_key: TASK_EI,
                element_id: "t".to_string(),
                job_type: "review-round".to_string(),
                state: JobState::Activated,
                worker: Some(String::new()),
                deadline: Some(60_000),
                activated_at: Some(1),
                activation_timeout: Some(60_000),
                lease_token: None,
                durable_activation: true,
                activated: true,
                retries: 1,
                priority: 0,
                created_at: 1,
                kind: JobKind::BpmnElement,
                error_message: None,
                error_code: None,
                has_failed_with_retries_left: false,
            },
        );

        let store = ReadStore::open(None).unwrap();
        store.seed_from_engine_state(&state).unwrap();

        // The seed itself must have normalized the empty worker to NULL.
        let seeded: HashMap<Key, super::JobRow> =
            store.jobs().into_iter().map(|j| (j.key, j)).collect();
        assert_eq!(
            seeded[&9401].worker, None,
            "seeded empty worker must be NULL"
        );

        // A completion carrying no attribution must not resurrect the empty string.
        store
            .export(&[&Event::JobCompleted {
                job_key: 9401,
                instance_key: INST,
                created_at: 2,
                job_type: "review-round".to_string(),
                worker: None,
            }])
            .unwrap();

        let jobs: HashMap<Key, super::JobRow> =
            store.jobs().into_iter().map(|j| (j.key, j)).collect();
        let completed = &jobs[&9401];
        assert_eq!(completed.state, JobState::Completed);
        assert_eq!(completed.worker, None);
    }

    #[test]
    fn a_compaction_floor_rebuild_preserves_job_error_message_code_and_retries_left_flag() {
        use nanobpmn_engine_core::{Job, JobKind, State};

        // #1327 / #1328 review — below-compaction-floor recovery re-seeds job rows
        // from the engine snapshot via `project_engine_state`, NOT from the (now
        // compacted) `JobFailed`/`JobErrorThrown` events. The engine retains the
        // last error message/code and the retries-left flag on `Job`, so the
        // seeding binding must carry them through; otherwise a reset/corrupt read
        // model comes back with NULL/false even for failures written by this
        // version. Covers all three fields across a failed-with-retries job, a
        // terminally-failed job, and a thrown-error job.
        let base_job = |key, state| Job {
            key,
            instance_key: INST,
            element_instance_key: TASK_EI,
            element_id: "t".to_string(),
            job_type: "worker".to_string(),
            state,
            worker: None,
            deadline: None,
            activated_at: None,
            activation_timeout: None,
            lease_token: None,
            durable_activation: false,
            activated: true,
            retries: 0,
            priority: 0,
            created_at: 1,
            kind: JobKind::BpmnElement,
            error_message: None,
            error_code: None,
            has_failed_with_retries_left: false,
        };
        let mut state = State::default();
        // Failed with retries left.
        state.jobs.insert(9601, {
            let mut j = base_job(9601, JobState::Created);
            j.retries = 2;
            j.error_message = Some("upstream 503".to_string());
            j.has_failed_with_retries_left = true;
            j
        });
        // Terminally failed (retries exhausted).
        state.jobs.insert(9602, {
            let mut j = base_job(9602, JobState::Failed);
            j.error_message = Some("gave up".to_string());
            j
        });
        // Thrown error (code + message, retries left → flag set).
        state.jobs.insert(9603, {
            let mut j = base_job(9603, JobState::Errored);
            j.retries = 1;
            j.error_code = Some("E42".to_string());
            j.error_message = Some("card declined".to_string());
            j.has_failed_with_retries_left = true;
            j
        });

        let store = ReadStore::open(None).unwrap();
        store.seed_from_engine_state(&state).unwrap();
        let seeded: HashMap<Key, super::JobRow> =
            store.jobs().into_iter().map(|j| (j.key, j)).collect();

        assert_eq!(seeded[&9601].error_message.as_deref(), Some("upstream 503"));
        assert!(seeded[&9601].has_failed_with_retries_left);
        assert!(seeded[&9601].error_code.is_none());

        assert_eq!(seeded[&9602].error_message.as_deref(), Some("gave up"));
        assert!(!seeded[&9602].has_failed_with_retries_left);

        assert_eq!(seeded[&9603].error_code.as_deref(), Some("E42"));
        assert_eq!(
            seeded[&9603].error_message.as_deref(),
            Some("card declined")
        );
        assert!(seeded[&9603].has_failed_with_retries_left);
    }

    #[test]
    fn a_legacy_empty_worker_row_is_cleared_by_a_terminal_event_carrying_no_attribution() {
        use rusqlite::params;

        // #1191 — the read-model DB is migrated *non-destructively* (additive
        // ALTER/CREATE; existing rows survive — a destructive rebuild only happens
        // on a format-version bump, which reprojects anyway), so a database written
        // before this PR can still hold `worker = ''` for a job the *old*
        // `JobActivated` projection recorded from an explicitly-empty worker. A
        // later terminal event carrying no attribution (`worker == None`) must not
        // resurrect that bogus empty string: a bare `COALESCE(NULL, worker)` would
        // keep `''`, so the terminal paths fall back through
        // `COALESCE(?, NULLIF(worker, ''))` and land SQL NULL. Covers all three
        // terminal paths (completed / failed / errored).
        for (job_key, terminal) in [
            (
                9501u64,
                Event::JobCompleted {
                    job_key: 9501,
                    instance_key: INST,
                    created_at: 2,
                    job_type: "review-round".to_string(),
                    worker: None,
                },
            ),
            (
                9502,
                Event::JobFailed {
                    job_key: 9502,
                    instance_key: INST,
                    retries: 0,
                    worker: None,
                    error_message: None,
                },
            ),
            (
                9503,
                Event::JobErrorThrown {
                    job_key: 9503,
                    instance_key: INST,
                    error_code: "BOOM".to_string(),
                    worker: None,
                    error_message: None,
                },
            ),
        ] {
            let store = ReadStore::open(None).unwrap();
            // Simulate the legacy persisted row directly: the current projection
            // normalizes `''` away at the source, so bypass it and write the row
            // exactly as a pre-#1191 build would have left it.
            store
                .conn
                .lock()
                .unwrap()
                .execute(
                    "INSERT INTO jobs (key, instance_key, element_instance_key, element_id, \
                     job_type, state, retries, worker, process_definition_id, \
                     process_definition_key) \
                     VALUES (?1, ?2, ?3, 't', 'review-round', ?4, 1, '', 'p', '500')",
                    params![
                        job_key as i64,
                        INST as i64,
                        TASK_EI as i64,
                        super::job_state_code(JobState::Activated),
                    ],
                )
                .unwrap();

            // Precondition: the legacy row really does hold an empty attribution.
            let before: HashMap<Key, super::JobRow> =
                store.jobs().into_iter().map(|j| (j.key, j)).collect();
            assert_eq!(
                before[&job_key].worker,
                Some(String::new()),
                "legacy row must start with an empty-string worker"
            );

            store.export(&[&terminal]).unwrap();

            let jobs: HashMap<Key, super::JobRow> =
                store.jobs().into_iter().map(|j| (j.key, j)).collect();
            assert_eq!(
                jobs[&job_key].worker, None,
                "terminal event carrying no attribution must clear the legacy '' worker to NULL"
            );
        }
    }

    #[test]
    fn projects_job_timing_creation_last_update_and_end() {
        // #1344 — Camunda-parity job timing. A created→completed job satisfies
        // `creationTime <= lastUpdateTime == endTime`; a re-delivery is a no-op; a
        // cancelled job carries an `endTime` while a failed or timed-out one does
        // not; and the deadline is NOT cleared on completion.
        let store = ReadStore::open(None).unwrap();
        let job = |key, eik, element_id: &str| Event::JobCreated {
            job_key: key,
            instance_key: INST,
            element_instance_key: eik,
            element_id: element_id.to_string(),
            job_type: "worker".to_string(),
            created_at: 123,
            priority: 0,
            retries: 3,
        };
        let activate = |key, deadline| Event::JobActivated {
            job_key: key,
            instance_key: INST,
            worker: "W".into(),
            deadline,
            activated_at: Some(1),
            fetch_variables: Vec::new(),
            lease_token: None,
            durable: false,
        };
        let complete = |key| Event::JobCompleted {
            job_key: key,
            instance_key: INST,
            created_at: 0,
            job_type: String::new(),
            worker: None,
        };
        let get = |k: Key| store.jobs().into_iter().find(|j| j.key == k).unwrap();

        store
            .export(&[&deploy(), &created(), &job(7001, TASK_EI, "t")])
            .unwrap();
        let created_row = get(7001);
        assert_eq!(created_row.created_at_ms, 123);
        // `lastUpdateTime` seeds from the (deterministic) creation instant; the
        // job has not ended yet.
        assert_eq!(created_row.last_update_ms, Some(123));
        assert_eq!(created_row.end_ms, None);

        // Activate (not projected onto the timing row — Camunda parity) then
        // complete.
        store.export(&[&activate(7001, 9999)]).unwrap();
        assert_eq!(
            get(7001).last_update_ms,
            Some(123),
            "activation must NOT move lastUpdateTime"
        );
        store.export(&[&complete(7001)]).unwrap();
        let completed = get(7001);
        assert!(completed.end_ms.is_some(), "a completed job has an endTime");
        assert_eq!(
            completed.last_update_ms, completed.end_ms,
            "lastUpdateTime == endTime on completion"
        );
        assert!(
            completed.created_at_ms <= completed.last_update_ms.unwrap(),
            "creationTime <= lastUpdateTime"
        );
        assert_eq!(
            completed.deadline_ms,
            Some(9999),
            "the deadline is kept on completion (Camunda parity)"
        );

        // Re-delivering the terminal event is a no-op for both timestamps.
        store.export(&[&complete(7001)]).unwrap();
        let redelivered = get(7001);
        assert_eq!(redelivered.end_ms, completed.end_ms);
        assert_eq!(redelivered.last_update_ms, completed.last_update_ms);

        // A cancelled job gets an endTime.
        store.export(&[&job(7002, 1002, "t")]).unwrap();
        store
            .export(&[&Event::JobCanceled {
                job_key: 7002,
                instance_key: INST,
            }])
            .unwrap();
        assert!(get(7002).end_ms.is_some(), "a cancelled job has an endTime");

        // A terminally failed job (retries exhausted) has NO endTime, but its
        // lastUpdateTime moved off the creation instant.
        store.export(&[&job(7003, 1003, "t")]).unwrap();
        store
            .export(&[&Event::JobFailed {
                job_key: 7003,
                instance_key: INST,
                retries: 0,
                worker: None,
                error_message: None,
            }])
            .unwrap();
        let failed = get(7003);
        assert_eq!(failed.end_ms, None, "a failed job has no endTime");
        assert!(failed.last_update_ms.is_some());

        // A timed-out (lock-expired) job likewise has no endTime.
        store.export(&[&job(7004, 1004, "t")]).unwrap();
        store.export(&[&activate(7004, 50)]).unwrap();
        store
            .export(&[&Event::JobLockExpired {
                job_key: 7004,
                instance_key: INST,
            }])
            .unwrap();
        assert_eq!(get(7004).end_ms, None, "a timed-out job has no endTime");
    }

    #[test]
    fn job_timing_last_update_frozen_once_terminal() {
        // #1344 regression — the `lastUpdateTime == endTime` invariant + the
        // "re-delivering events doesn't move the times" contract must survive a
        // stray non-terminal job event landing on an ALREADY-terminal job (the
        // overlapping-prefix replay the `export` docstring promises is idempotent
        // re-projects the WHOLE prefix with a fresh batch `now_ms`, so a terminal
        // job's earlier FAILED/RETRIES_UPDATED/TIMEOUT_UPDATED record re-runs
        // after its COMPLETED/CANCELED). Without the `end_ms`-guard on
        // `last_update_ms` these would push `lastUpdateTime` PAST `endTime`.
        let store = ReadStore::open(None).unwrap();
        let job = |key, eik, element_id: &str| Event::JobCreated {
            job_key: key,
            instance_key: INST,
            element_instance_key: eik,
            element_id: element_id.to_string(),
            job_type: "worker".to_string(),
            created_at: 123,
            priority: 0,
            retries: 3,
        };
        let get = |k: Key| store.jobs().into_iter().find(|j| j.key == k).unwrap();

        // Completed job, then every non-terminal job event re-delivered for it.
        store
            .export(&[
                &deploy(),
                &created(),
                &job(8001, TASK_EI, "t"),
                &Event::JobCompleted {
                    job_key: 8001,
                    instance_key: INST,
                    created_at: 0,
                    job_type: String::new(),
                    worker: None,
                },
            ])
            .unwrap();
        let completed = get(8001);
        let end = completed.end_ms.expect("completed job has an endTime");
        assert_eq!(completed.last_update_ms, Some(end));

        for stray in [
            Event::JobFailed {
                job_key: 8001,
                instance_key: INST,
                retries: 2,
                worker: None,
                error_message: Some("late".into()),
            },
            Event::JobErrorThrown {
                job_key: 8001,
                instance_key: INST,
                error_code: "E".into(),
                worker: None,
                error_message: None,
            },
            Event::JobRetriesUpdated {
                job_key: 8001,
                instance_key: INST,
                retries: 5,
                operation_reference: None,
            },
            Event::JobTimeoutUpdated {
                job_key: 8001,
                instance_key: INST,
                deadline: 77,
                operation_reference: None,
            },
        ] {
            store.export(&[&stray]).unwrap();
            let row = get(8001);
            assert_eq!(
                row.end_ms,
                Some(end),
                "a stray non-terminal event must not change endTime"
            );
            assert_eq!(
                row.last_update_ms,
                Some(end),
                "lastUpdateTime stays frozen at endTime for a terminal job (no inversion)"
            );
            assert!(
                row.last_update_ms.unwrap() <= row.end_ms.unwrap(),
                "lastUpdateTime must never exceed endTime"
            );
        }

        // A cancelled job (end set) is equally protected against a stray FAILED.
        store.export(&[&job(8002, 1002, "t")]).unwrap();
        store
            .export(&[&Event::JobCanceled {
                job_key: 8002,
                instance_key: INST,
            }])
            .unwrap();
        let canceled_end = get(8002).end_ms.expect("cancelled job has an endTime");
        store
            .export(&[&Event::JobFailed {
                job_key: 8002,
                instance_key: INST,
                retries: 1,
                worker: None,
                error_message: None,
            }])
            .unwrap();
        assert_eq!(
            get(8002).last_update_ms,
            Some(canceled_end),
            "lastUpdateTime stays frozen at endTime for a cancelled job"
        );
    }

    #[test]
    fn job_timing_last_update_frozen_for_terminal_states_without_end_time() {
        // #1344 regression — a FAILED (retries exhausted) or ERROR_THROWN job is
        // TERMINAL but deliberately keeps `end_ms` NULL (Camunda stamps no
        // `endTime` for a failed/errored job). The `end_ms`-guard alone therefore
        // never engages for these states, so re-delivering the SAME terminal event
        // (the overlapping-prefix replay `export` promises is idempotent) would
        // stamp a fresh `now_ms` onto `lastUpdateTime` every time. The freeze must
        // ALSO hold the value once the job sits in a terminal `endTime`-less state
        // (Failed/Errored), while still stamping the FIRST transition.
        let store = ReadStore::open(None).unwrap();
        let job = |key, eik, element_id: &str| Event::JobCreated {
            job_key: key,
            instance_key: INST,
            element_instance_key: eik,
            element_id: element_id.to_string(),
            job_type: "worker".to_string(),
            created_at: 123,
            priority: 0,
            retries: 3,
        };
        let get = |k: Key| store.jobs().into_iter().find(|j| j.key == k).unwrap();

        // Terminal FAILED (retries exhausted): first delivery stamps, re-delivery
        // must NOT move `lastUpdateTime`.
        store
            .export(&[&deploy(), &created(), &job(8201, TASK_EI, "t")])
            .unwrap();
        let fail = |key| Event::JobFailed {
            job_key: key,
            instance_key: INST,
            retries: 0,
            worker: None,
            error_message: Some("boom".into()),
        };
        store.export(&[&fail(8201)]).unwrap();
        let failed = get(8201);
        assert_eq!(failed.end_ms, None, "a failed job has no endTime");
        let failed_lu = failed
            .last_update_ms
            .expect("a failed job has a lastUpdateTime");
        assert!(
            failed_lu > 123,
            "the first terminal transition stamps lastUpdateTime"
        );
        store.export(&[&fail(8201)]).unwrap();
        assert_eq!(
            get(8201).last_update_ms,
            Some(failed_lu),
            "re-delivering FAILED must not move lastUpdateTime (no endTime to freeze at)"
        );

        // Terminal ERROR_THROWN: same contract — first stamps, re-delivery frozen.
        store.export(&[&job(8202, 1002, "t")]).unwrap();
        let throw = |key| Event::JobErrorThrown {
            job_key: key,
            instance_key: INST,
            error_code: "E".into(),
            worker: None,
            error_message: None,
        };
        store.export(&[&throw(8202)]).unwrap();
        let errored = get(8202);
        assert_eq!(errored.end_ms, None, "an errored job has no endTime");
        let errored_lu = errored
            .last_update_ms
            .expect("an errored job has a lastUpdateTime");
        store.export(&[&throw(8202)]).unwrap();
        assert_eq!(
            get(8202).last_update_ms,
            Some(errored_lu),
            "re-delivering ERROR_THROWN must not move lastUpdateTime"
        );

        // A terminal-state job must ALSO be frozen against EVERY stray
        // non-terminal event replayed after it (FAILED/ERROR_THROWN/
        // RETRIES_UPDATED/TIMEOUT_UPDATED) — none may move `lastUpdateTime`.
        for stray in [
            Event::JobFailed {
                job_key: 8202,
                instance_key: INST,
                retries: 0,
                worker: None,
                error_message: Some("stray".into()),
            },
            Event::JobErrorThrown {
                job_key: 8202,
                instance_key: INST,
                error_code: "E2".into(),
                worker: None,
                error_message: None,
            },
            Event::JobRetriesUpdated {
                job_key: 8202,
                instance_key: INST,
                retries: 9,
                operation_reference: None,
            },
            Event::JobTimeoutUpdated {
                job_key: 8202,
                instance_key: INST,
                deadline: 77,
                operation_reference: None,
            },
        ] {
            store.export(&[&stray]).unwrap();
            assert_eq!(
                get(8202).last_update_ms,
                Some(errored_lu),
                "a stray non-terminal event must not move an errored job's lastUpdateTime"
            );
        }
    }

    #[test]
    fn legacy_job_without_created_at_surfaces_non_null_creation_time() {
        // #1344 — a legacy `JobCreated` (created_at == 0, serialized before the
        // field existed) must still surface a non-null `creationTime`: both
        // `created_at_ms` and `last_update_ms` fall back to the batch observation
        // time, so `creationTime == lastUpdateTime` rather than a null creation
        // time against a populated last-update time. Covers both the event path
        // and the snapshot (`project_engine_state`) reseed path.
        let store = ReadStore::open(None).unwrap();
        let get = |k: Key| store.jobs().into_iter().find(|j| j.key == k).unwrap();

        store
            .export(&[
                &deploy(),
                &created(),
                &Event::JobCreated {
                    job_key: 8101,
                    instance_key: INST,
                    element_instance_key: TASK_EI,
                    element_id: "t".to_string(),
                    job_type: "worker".to_string(),
                    created_at: 0,
                    priority: 0,
                    retries: 3,
                },
            ])
            .unwrap();
        let row = get(8101);
        assert_ne!(
            row.created_at_ms, 0,
            "a legacy job must surface a non-null creationTime (batch-time fallback)"
        );
        assert_eq!(
            Some(row.created_at_ms),
            row.last_update_ms,
            "creationTime == lastUpdateTime for a legacy job"
        );
    }

    #[test]
    fn legacy_snapshot_job_surfaces_non_null_creation_time() {
        use nanobpmn_engine_core::{Job, JobKind, State};
        // The snapshot reseed path (`project_engine_state`) must apply the same
        // batch-time fallback to `created_at_ms` for a legacy job (created_at == 0)
        // so a compaction-floor recovery does not leave `creationTime` null while
        // `lastUpdateTime` is populated (#1344).
        let mut state = State::default();
        state.jobs.insert(
            8201,
            Job {
                key: 8201,
                instance_key: INST,
                element_instance_key: TASK_EI,
                element_id: "t".to_string(),
                job_type: "worker".to_string(),
                state: JobState::Created,
                worker: None,
                deadline: None,
                activated_at: None,
                activation_timeout: None,
                lease_token: None,
                durable_activation: false,
                activated: false,
                retries: 3,
                priority: 0,
                created_at: 0,
                kind: JobKind::BpmnElement,
                error_message: None,
                error_code: None,
                has_failed_with_retries_left: false,
            },
        );
        let store = ReadStore::open(None).unwrap();
        store.export(&[&deploy(), &created()]).unwrap();
        store.seed_from_engine_state(&state).unwrap();
        let row = store.jobs().into_iter().find(|j| j.key == 8201).unwrap();
        assert_ne!(
            row.created_at_ms, 0,
            "a legacy snapshot-seeded job must surface a non-null creationTime"
        );
        assert_eq!(
            Some(row.created_at_ms),
            row.last_update_ms,
            "creationTime == lastUpdateTime for a legacy snapshot job"
        );
    }

    #[test]
    fn projects_opaque_job_leases_for_both_activation_read_set_forms() {
        let store = ReadStore::open(None).unwrap();
        store
            .export(&[
                &deploy(),
                &created(),
                &Event::JobCreated {
                    job_key: 7001,
                    instance_key: INST,
                    element_instance_key: TASK_EI,
                    element_id: "t".into(),
                    job_type: "worker".into(),
                    created_at: 1,
                    priority: 0,
                    retries: 3,
                },
            ])
            .unwrap();
        assert!(store.jobs()[0].lease_token.is_none());
        for (token, fetch_variables) in [
            ("lease:opaque/0001", Vec::new()),
            ("lease:opaque/0002", vec!["input".into()]),
        ] {
            store
                .export(&[&Event::JobActivated {
                    job_key: 7001,
                    instance_key: INST,
                    worker: "W".into(),
                    deadline: 100,
                    activated_at: Some(1),
                    fetch_variables,
                    lease_token: Some(token.into()),
                    durable: false,
                }])
                .unwrap();
            assert_eq!(store.jobs()[0].lease_token.as_deref(), Some(token));
        }
        {
            let conn = store.conn.lock().unwrap();
            conn.execute_batch(
                "ALTER TABLE jobs RENAME COLUMN lease_token TO old_lease_token; \
                 ALTER TABLE jobs ADD COLUMN lease_token INTEGER; \
                 UPDATE jobs SET lease_token = 314;",
            )
            .unwrap();
        }
        assert_eq!(store.jobs()[0].lease_token.as_deref(), Some("314"));
    }

    #[test]
    fn projects_job_error_message_code_and_failed_with_retries_left() {
        // #1327 — Zeebe stores the worker's errorMessage on the job on EVERY fail
        // (retries left or not) and errorCode on a thrown error; the exporter
        // sets `jobFailedWithRetriesLeft` on FAILED / ERROR_THROWN from the
        // record's retries. The row must carry all three.
        let store = ReadStore::open(None).unwrap();
        let job = |key| Event::JobCreated {
            job_key: key,
            instance_key: INST,
            element_instance_key: TASK_EI,
            element_id: "t".to_string(),
            job_type: "worker".to_string(),
            created_at: 1,
            priority: 0,
            retries: 3,
        };
        let row = |store: &ReadStore, key| store.jobs().into_iter().find(|j| j.key == key).unwrap();
        store
            .export(&[&deploy(), &created(), &job(7001), &job(7002)])
            .unwrap();
        assert!(row(&store, 7001).error_message.is_none());
        assert!(!row(&store, 7001).has_failed_with_retries_left);

        store
            .export(&[&Event::JobFailed {
                job_key: 7001,
                instance_key: INST,
                retries: 2,
                worker: None,
                error_message: Some("upstream 503".to_string()),
            }])
            .unwrap();
        let r = row(&store, 7001);
        assert_eq!(r.error_message.as_deref(), Some("upstream 503"));
        assert!(r.has_failed_with_retries_left);
        assert!(r.error_code.is_none());

        // A legacy (pre-field) event keeps the last known message; the final
        // fail overwrites it and clears the retries-left flag.
        store
            .export(&[&Event::JobFailed {
                job_key: 7001,
                instance_key: INST,
                retries: 1,
                worker: None,
                error_message: None,
            }])
            .unwrap();
        assert_eq!(
            row(&store, 7001).error_message.as_deref(),
            Some("upstream 503")
        );
        store
            .export(&[&Event::JobFailed {
                job_key: 7001,
                instance_key: INST,
                retries: 0,
                worker: None,
                error_message: Some("gave up".to_string()),
            }])
            .unwrap();
        let r = row(&store, 7001);
        assert_eq!(r.error_message.as_deref(), Some("gave up"));
        assert!(!r.has_failed_with_retries_left);

        store
            .export(&[&Event::JobErrorThrown {
                job_key: 7002,
                instance_key: INST,
                error_code: "E42".to_string(),
                worker: None,
                error_message: Some("card declined".to_string()),
            }])
            .unwrap();
        let r = row(&store, 7002);
        assert_eq!(r.error_code.as_deref(), Some("E42"));
        assert_eq!(r.error_message.as_deref(), Some("card declined"));
        // Exporter parity: ERROR_THROWN with retries > 0 counts as "failed with
        // retries left" (JobHandler keys the flag off the record's retries).
        assert!(r.has_failed_with_retries_left);
    }

    #[test]
    fn projects_the_declared_read_set_from_job_activated_onto_the_row() {
        // #986 — a `JobActivated` that carries a declared read-set (`fetchVariables`)
        // must surface it on the read-model job row (engine-native read provenance
        // for reification). A declaration-free activation leaves the read-set empty
        // ("undeclared / fetch-all"), and re-activating with an empty set does not
        // erase a previously declared set.
        let store = ReadStore::open(None).unwrap();
        store
            .export(&[
                &deploy(),
                &created(),
                // Declared: worker asked for [a, c].
                &Event::JobCreated {
                    job_key: 7001,
                    instance_key: INST,
                    element_instance_key: TASK_EI,
                    element_id: "t".to_string(),
                    job_type: "worker".to_string(),
                    created_at: 1,
                    priority: 0,
                    retries: 1,
                },
                &Event::JobActivated {
                    job_key: 7001,
                    instance_key: INST,
                    worker: "w1".to_string(),
                    deadline: 60_000,
                    activated_at: Some(1),
                    fetch_variables: vec!["a".to_string(), "c".to_string()],
                    lease_token: None,
                    durable: false,
                },
                // Declaration-free: no fetchVariables ⇒ read-set stays empty.
                &Event::JobCreated {
                    job_key: 7002,
                    instance_key: INST,
                    element_instance_key: TASK_EI,
                    element_id: "t".to_string(),
                    job_type: "worker".to_string(),
                    created_at: 1,
                    priority: 0,
                    retries: 1,
                },
                &Event::JobActivated {
                    job_key: 7002,
                    instance_key: INST,
                    worker: "w2".to_string(),
                    deadline: 60_000,
                    activated_at: Some(1),
                    fetch_variables: Vec::new(),
                    lease_token: None,
                    durable: false,
                },
            ])
            .unwrap();

        let jobs: HashMap<Key, super::JobRow> =
            store.jobs().into_iter().map(|j| (j.key, j)).collect();

        assert_eq!(jobs[&7001].read_set, vec!["a".to_string(), "c".to_string()]);
        assert!(
            jobs[&7002].read_set.is_empty(),
            "declaration-free activation leaves an empty (undeclared) read-set"
        );

        // A later declaration-free re-activation (e.g. lock re-lease) must not
        // erase the previously declared provenance.
        store
            .export(&[&Event::JobActivated {
                job_key: 7001,
                instance_key: INST,
                worker: "w1".to_string(),
                deadline: 120_000,
                activated_at: Some(2),
                fetch_variables: Vec::new(),
                lease_token: None,
                durable: false,
            }])
            .unwrap();
        let jobs: HashMap<Key, super::JobRow> =
            store.jobs().into_iter().map(|j| (j.key, j)).collect();
        assert_eq!(
            jobs[&7001].read_set,
            vec!["a".to_string(), "c".to_string()],
            "a declaration-free re-activation preserves the last declared read-set"
        );
    }

    #[test]
    fn non_interrupting_boundary_stays_open_and_records_each_correlation() {
        use nanobpmn_engine_core::MessageSubscriptionKind;
        let store = ReadStore::open(None).unwrap();
        store.export(&[&deploy(), &created()]).unwrap();

        // A non-interrupting message boundary subscription: correlating spawns a
        // parallel token but leaves the subscription open, so it can correlate
        // again for every matching message.
        store
            .export(&[&Event::MessageSubscriptionCreated {
                subscription_key: 4001,
                instance_key: INST,
                element_instance_key: TASK_EI,
                element_id: "task".to_string(),
                message_name: "Ping".to_string(),
                correlation_key: "K1".to_string(),
                kind: MessageSubscriptionKind::NonInterruptingBoundary {
                    boundary_element_id: "boundary".to_string(),
                },
            }])
            .unwrap();
        assert_eq!(store.message_subscriptions().len(), 1);

        // First correlation: history recorded AND the open row is kept.
        store
            .export(&[&Event::MessageCorrelated {
                subscription_key: 4001,
                message_key: 11,
                instance_key: INST,
                element_instance_key: TASK_EI,
                element_id: "task".to_string(),
            }])
            .unwrap();
        assert_eq!(
            store.message_subscriptions().len(),
            1,
            "non-interrupting subscription stays open after correlating"
        );
        assert_eq!(store.correlated_message_subscriptions().len(), 1);

        // Second correlation (different message): another history row, still open.
        store
            .export(&[&Event::MessageCorrelated {
                subscription_key: 4001,
                message_key: 12,
                instance_key: INST,
                element_instance_key: TASK_EI,
                element_id: "task".to_string(),
            }])
            .unwrap();
        assert_eq!(store.message_subscriptions().len(), 1);
        let corr = store.correlated_message_subscriptions();
        assert_eq!(corr.len(), 2, "each correlation is recorded in history");
        // partition_id is stored 1-based (Camunda/Zeebe convention).
        assert!(corr.iter().all(|c| c.partition_id == 1));
        assert!(corr.iter().all(|c| c.message_name == "Ping"));

        // Cancelling (activity completes / instance ends) finally drops the row.
        store
            .export(&[&Event::MessageSubscriptionCanceled {
                subscription_key: 4001,
                instance_key: INST,
                element_instance_key: TASK_EI,
                element_id: "task".to_string(),
            }])
            .unwrap();
        assert!(store.message_subscriptions().is_empty());
        // History survives the cancel.
        assert_eq!(store.correlated_message_subscriptions().len(), 2);
    }

    #[test]
    fn correlation_without_an_open_row_records_no_history() {
        // A correlation event whose open subscription row is absent (out-of-order
        // replay / partial seeding) must not fabricate a history row with an empty
        // message name / correlation key.
        let store = ReadStore::open(None).unwrap();
        store.export(&[&deploy(), &created()]).unwrap();
        store
            .export(&[&Event::MessageCorrelated {
                subscription_key: 5001,
                message_key: 13,
                instance_key: INST,
                element_instance_key: TASK_EI,
                element_id: "await".to_string(),
            }])
            .unwrap();
        assert!(
            store.correlated_message_subscriptions().is_empty(),
            "no history row without a captured open subscription"
        );
    }
}

/// Read-surface parity checks for the shared projection, exercising the exact
/// readstore-shaped queries the in-browser (wasm) test engine will serve through
/// this same crate: `GetFormByKey` and `searchUserTasks` (with its open/closed
/// `state` filter). The projection and SQL are backend-agnostic — identical on
/// `native` and `wasm` — so proving them here (natively runnable, on the default
/// backend) proves the query surface the wasm backend answers byte-for-byte.
///
/// This is the acceptance coverage for the wasm read-model backend (epic
/// Magikcraft/nano-bpm#796): it mirrors the epic's spike, so a regression in the
/// shared read surface fails a plain `cargo test` regardless of backend.
#[cfg(test)]
mod read_surface_tests {
    use nanobpmn_engine_core::{Event, UserTaskState};

    use super::{Key, ReadStore};

    fn form_deployed(form_key: Key, form_id: &str, version: i32, schema: &str) -> Event {
        Event::FormDeployed {
            deployment_key: 1,
            form_key,
            version,
            form_id: form_id.to_string(),
            resource_name: format!("{form_id}.form"),
            schema: schema.to_string(),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn user_task_created(user_task_key: Key, instance_key: Key, element_id: &str) -> Event {
        Event::UserTaskCreated {
            user_task_key,
            instance_key,
            element_instance_key: instance_key,
            element_id: element_id.to_string(),
            created_at: 0,
            assignee: None,
            candidate_groups: Vec::new(),
            candidate_users: Vec::new(),
            due_date: None,
            follow_up_date: None,
            priority: 0,
            form_key: None,
            external_form_reference: None,
        }
    }

    /// `GetFormByKey` resolves each deployed form version by its unique key, and a
    /// redeploy of the same `form_id` yields the *latest* schema at the latest
    /// version — the spike's "form_by_key returns the latest schema".
    #[test]
    fn form_by_key_serves_the_latest_deployed_schema() {
        let store = ReadStore::open(None).expect("in-memory read store opens");

        // Deploy v1 then a new version v2 of the same form id, each with a
        // distinct schema and its own unique form key.
        let v1 = form_deployed(10, "greeting", 1, r#"{"schemaVersion":1}"#);
        let v2 = form_deployed(11, "greeting", 2, r#"{"schemaVersion":2}"#);
        store.export(&[&v1]).expect("project form v1");
        store.export(&[&v2]).expect("project form v2");

        // The latest key resolves the latest schema/version.
        let latest = store.form_by_key(11).expect("latest form version resolves");
        assert_eq!(latest.form_id, "greeting");
        assert_eq!(latest.version, 2);
        assert_eq!(latest.schema, r#"{"schemaVersion":2}"#);

        // Every prior version remains servable by its own key (Zeebe parity).
        let older = store
            .form_by_key(10)
            .expect("older form version still resolves");
        assert_eq!(older.version, 1);
        assert_eq!(older.schema, r#"{"schemaVersion":1}"#);

        // An unknown key has no form.
        assert!(store.form_by_key(999).is_none());
    }

    /// The projection records a `state` per user task, distinguishing open
    /// (`Created`) from completed tasks off the exact projected data — the spike's
    /// second assertion. This validates the *state projection*: `user_tasks()` is
    /// unfiltered, so the open/completed split is asserted here in Rust, which is
    /// exactly the data `searchUserTasks({state:'CREATED'})` filters on downstream.
    #[test]
    fn user_tasks_projection_records_open_and_completed_state() {
        let store = ReadStore::open(None).expect("in-memory read store opens");

        // Two user tasks are created (both open); one is then completed.
        let open = user_task_created(100, 1, "review");
        let closing = user_task_created(101, 1, "approve");
        store.export(&[&open]).expect("project open task");
        store.export(&[&closing]).expect("project task to complete");
        let completed = Event::UserTaskCompleted {
            user_task_key: 101,
            instance_key: 1,
        };
        store.export(&[&completed]).expect("project completion");

        let tasks = store.user_tasks();
        assert_eq!(tasks.len(), 2, "both tasks remain projected");

        // Filtering to the open state (what searchUserTasks({state:'CREATED'})
        // does) yields exactly the un-completed task.
        let open_only: Vec<Key> = tasks
            .iter()
            .filter(|t| t.state == UserTaskState::Created)
            .map(|t| t.key)
            .collect();
        assert_eq!(open_only, vec![100]);

        // The completed task carries the terminal state, so it is excluded above
        // and included by a complementary filter.
        let completed_only: Vec<Key> = tasks
            .iter()
            .filter(|t| t.state == UserTaskState::Completed)
            .map(|t| t.key)
            .collect();
        assert_eq!(completed_only, vec![101]);
    }

    /// Issue #1095 (mirrors #977 for user tasks): the root of a user task is
    /// derived from the **single** existing resolver walking the task's
    /// `processInstanceKey` up the call-activity parent chain — a task parked on a
    /// call-activity child instance roots to the top-level parent, while a
    /// top-level task self-roots. Both the gateway REST projection and the
    /// engine-wasm `searchUserTasks` inherit their `rootProcessInstanceKey` from
    /// exactly this walk, so there is no duplicate root derivation to drift.
    #[test]
    fn user_task_root_key_resolves_through_the_call_activity_hierarchy() {
        let store = ReadStore::open(None).expect("in-memory read store opens");

        // top (10, no parent) <- child (20, spawned by call activity on 10).
        // A user task is parked on the child instance, and a second on the top.
        let top_instance = Event::ProcessInstanceCreated {
            instance_key: 10,
            process_id: "p".to_string(),
            variables: std::collections::HashMap::new(),
            created_at: 0,
            tags: Vec::new(),
            business_id: None,
            process_definition_key: 0,
            version: 0,
            parent_process_instance_key: None,
            parent_element_instance_key: None,
        };
        let child_instance = Event::ProcessInstanceCreated {
            instance_key: 20,
            process_id: "p".to_string(),
            variables: std::collections::HashMap::new(),
            created_at: 0,
            tags: Vec::new(),
            business_id: None,
            process_definition_key: 0,
            version: 0,
            parent_process_instance_key: Some(10),
            parent_element_instance_key: Some(111),
        };
        store
            .export(&[
                &top_instance,
                &child_instance,
                &user_task_created(200, 20, "child-review"),
                &user_task_created(100, 10, "top-review"),
            ])
            .unwrap();

        let tasks = store.user_tasks();
        let child_task = tasks
            .iter()
            .find(|t| t.key == 200)
            .expect("child user task projected");
        let top_task = tasks
            .iter()
            .find(|t| t.key == 100)
            .expect("top user task projected");

        // The child-instance task roots to the top-level parent, not its own
        // instance key — the correlation the escalation → epic proof needs.
        assert_eq!(
            store.root_process_instance_key(child_task.instance_key),
            10,
            "a task on a call-activity child roots to the top-level parent"
        );
        // No-regression guard: a top-level task self-roots.
        assert_eq!(
            store.root_process_instance_key(top_task.instance_key),
            top_task.instance_key,
            "a top-level task roots to its own process instance key"
        );
    }

    /// Defect-class guard: a `JobCreated` replay/repair must *refresh*
    /// `created_at_ms`, not leave it stuck at a stale value. The
    /// `/v2/jobs/statistics/*` aggregations count `created` jobs off
    /// `created_at_ms`, so a row whose `created_at_ms` was persisted as `0` by an
    /// older projection (predating the column) would be under-counted forever if
    /// the upsert's `ON CONFLICT` clause did not overwrite it. Re-projecting the
    /// same job with its real timestamp must repair the stale value. Since #1344
    /// a legacy (`created_at == 0`) projection no longer lands a literal `0`
    /// either — it seeds the batch-observation fallback so `creationTime` is
    /// non-null — but the refresh-on-conflict contract is what this guards.
    #[test]
    fn job_created_replay_repairs_stale_created_at_ms() {
        let store = ReadStore::open(None).unwrap();
        // Initial projection of a legacy (created_at == 0) event lands a
        // non-authoritative batch-time fallback (#1344), standing in for the
        // stale value an old DB predating the column would carry.
        store
            .export(&[&Event::JobCreated {
                job_key: 8001,
                instance_key: 7000,
                element_instance_key: 7001,
                element_id: "t".to_string(),
                job_type: "worker".to_string(),
                created_at: 0,
                priority: 0,
                retries: 3,
            }])
            .unwrap();
        assert_ne!(
            store
                .jobs()
                .into_iter()
                .find(|j| j.key == 8001)
                .unwrap()
                .created_at_ms,
            1_724_000_000_000,
            "precondition: stale row does not yet carry the authoritative timestamp"
        );

        // A repair/replay re-projects the same job carrying its real creation time.
        store
            .export(&[&Event::JobCreated {
                job_key: 8001,
                instance_key: 7000,
                element_instance_key: 7001,
                element_id: "t".to_string(),
                job_type: "worker".to_string(),
                created_at: 1_724_000_000_000,
                priority: 0,
                retries: 3,
            }])
            .unwrap();

        assert_eq!(
            store
                .jobs()
                .into_iter()
                .find(|j| j.key == 8001)
                .unwrap()
                .created_at_ms,
            1_724_000_000_000,
            "ON CONFLICT must refresh created_at_ms so statistics stop under-counting"
        );
    }

    /// Apply one event at an explicit `now_ms` (bypassing wall clock) so a
    /// re-delivery/replay in a *later* batch can be modelled deterministically.
    fn apply_at(store: &ReadStore, event: &Event, now_ms: u64) {
        let mut conn = store.conn.lock().unwrap();
        let tx = conn.transaction().unwrap();
        super::project(&tx, event, now_ms).unwrap();
        tx.commit().unwrap();
    }

    fn job_row(store: &ReadStore, key: Key) -> super::JobRow {
        store
            .jobs()
            .into_iter()
            .find(|j| j.key == key)
            .expect("job present")
    }

    /// Defect-class guard (#1344, finding 1): a legacy (`created_at == 0`)
    /// `JobCreated` seeds its `creationTime`/`lastUpdateTime` from the
    /// *batch-observation* fallback. Re-delivering or replaying that SAME legacy
    /// event in a later batch must be an idempotent no-op — the `ON CONFLICT`
    /// upsert must NOT overwrite the already-seeded timestamps with a fresh
    /// `now_ms`, or `creationTime` (and a live row's `lastUpdateTime`) would
    /// jump on every re-delivery. The sibling `ExecutionListenerJobCreated`
    /// upsert shares the class and the same guard clause.
    #[test]
    fn legacy_job_created_redelivery_does_not_move_timestamps() {
        let store = ReadStore::open(None).unwrap();
        let legacy = Event::JobCreated {
            job_key: 8300,
            instance_key: 7300,
            element_instance_key: 7301,
            element_id: "t".to_string(),
            job_type: "worker".to_string(),
            created_at: 0,
            priority: 0,
            retries: 3,
        };
        // First delivery stamps the batch-time fallback for both timestamps.
        apply_at(&store, &legacy, 1_000);
        let first = job_row(&store, 8300);
        assert_eq!(first.created_at_ms, 1_000);
        assert_eq!(first.last_update_ms, Some(1_000));

        // A later-batch re-delivery (different `now_ms`) of the SAME legacy
        // CREATED must not move either timestamp.
        apply_at(&store, &legacy, 9_999);
        let again = job_row(&store, 8300);
        assert_eq!(
            again.created_at_ms, 1_000,
            "legacy re-delivery must not move creationTime"
        );
        assert_eq!(
            again.last_update_ms,
            Some(1_000),
            "legacy re-delivery must not move lastUpdateTime"
        );
    }

    /// Defect-class guard (#1344, finding 2): replaying a whole committed prefix
    /// `[JobCreated, JobFailed]` (or `[JobCreated, JobErrorThrown]`) must not
    /// resurrect a terminal, end-time-less job back to `Created` nor un-freeze
    /// its `lastUpdateTime`. The replayed `JobCreated` hits the `ON CONFLICT`
    /// path, which must preserve the advanced/terminal state and its frozen
    /// timestamp so the trailing terminal event stays an idempotent no-op.
    #[test]
    fn whole_prefix_replay_keeps_terminal_job_frozen() {
        for terminal in [
            Event::JobFailed {
                job_key: 8400,
                instance_key: 7400,
                retries: 0,
                worker: None,
                error_message: Some("boom".to_string()),
            },
            Event::JobErrorThrown {
                job_key: 8400,
                instance_key: 7400,
                error_code: "ERR".to_string(),
                worker: None,
                error_message: Some("boom".to_string()),
            },
        ] {
            let store = ReadStore::open(None).unwrap();
            let created = Event::JobCreated {
                job_key: 8400,
                instance_key: 7400,
                element_instance_key: 7401,
                element_id: "t".to_string(),
                job_type: "worker".to_string(),
                created_at: 123,
                priority: 0,
                retries: 0,
            };
            apply_at(&store, &created, 1_000);
            apply_at(&store, &terminal, 2_000);
            let frozen = job_row(&store, 8400);
            assert_eq!(frozen.end_ms, None, "terminal park carries no endTime");
            assert_eq!(
                frozen.last_update_ms,
                Some(2_000),
                "the terminal event stamps lastUpdateTime"
            );
            let terminal_state = frozen.state;

            // Replay the WHOLE prefix in a later batch: the replayed CREATED must
            // not regress the terminal state nor un-freeze lastUpdateTime, so the
            // trailing terminal event stays a no-op.
            apply_at(&store, &created, 5_000);
            let after_created = job_row(&store, 8400);
            assert_eq!(
                after_created.state, terminal_state,
                "replayed CREATED must not regress a terminal job to Created"
            );
            assert_eq!(
                after_created.last_update_ms,
                Some(2_000),
                "replayed CREATED must not move a terminal job's lastUpdateTime"
            );

            apply_at(&store, &terminal, 6_000);
            let after_terminal = job_row(&store, 8400);
            assert_eq!(
                after_terminal.last_update_ms,
                Some(2_000),
                "whole-prefix replay must leave lastUpdateTime frozen"
            );
        }
    }

    /// Defect-class guard: `created_at_ms` is persisted as a signed `INTEGER`,
    /// so a negative value in the DB (corruption, manual edits, a bad migration)
    /// must not survive the read-back as a wrapped, enormous `u64` — it would
    /// badly skew `/v2/jobs/statistics/*` created counts and window filters. The
    /// mappers clamp with `.max(0)`; this pins that for the job/user-task/incident
    /// mappers together, since they share the one canonical convention.
    #[test]
    fn negative_created_at_ms_clamps_to_zero_on_read() {
        let store = ReadStore::open(None).unwrap();
        store
            .export(&[&Event::JobCreated {
                job_key: 8100,
                instance_key: 7100,
                element_instance_key: 7101,
                element_id: "t".to_string(),
                job_type: "worker".to_string(),
                created_at: 1_724_000_000_000,
                priority: 0,
                retries: 3,
            }])
            .unwrap();

        // Simulate a corrupt / hand-edited row carrying a negative timestamp.
        {
            let conn = store.conn.lock().unwrap();
            conn.execute(
                "UPDATE jobs SET created_at_ms = ?1 WHERE key = ?2",
                rusqlite::params![-5_i64, 8100_i64],
            )
            .unwrap();
        }

        let job = store
            .jobs()
            .into_iter()
            .find(|j| j.key == 8100)
            .expect("job present");
        assert_eq!(
            job.created_at_ms, 0,
            "a negative persisted created_at_ms must clamp to 0, not wrap to a huge u64"
        );
    }
}

#[cfg(test)]
mod agent_projection_tests {
    use nanobpmn_engine_core::{
        AgentDefinition, AgentHistoryCommitStatus, AgentHistoryMetrics, AgentHistoryRecord,
        AgentHistoryRole, AgentInstance, AgentInstanceLimits, AgentInstanceMetrics,
        AgentInstanceStatus, AgentType, Event, Key,
    };

    use super::{
        AgentHistoryFilter, AgentHistorySortField, AgentInstanceFilter, AgentInstanceSortField,
        ReadStore, SortOrder,
    };

    fn instance(
        key: Key,
        element_id: &str,
        status: AgentInstanceStatus,
        created_at: u64,
    ) -> AgentInstance {
        AgentInstance {
            agent_instance_key: key,
            agent_definition_key: 7,
            element_instance_key: key + 1000,
            element_instance_keys: vec![key + 1000],
            element_id: element_id.to_string(),
            process_instance_key: 42,
            root_process_instance_key: 42,
            bpmn_process_id: "proc".to_string(),
            process_definition_key: 99,
            process_definition_version: 1,
            process_definition_version_tag: None,
            tenant_id: "<default>".to_string(),
            agent_type: AgentType::AiAgentTask,
            status,
            definition: AgentDefinition {
                model: Some("gpt".to_string()),
                provider: Some("openai".to_string()),
                system_prompt: Some(vec![nanobpmn_engine_core::AgentHistoryContent {
                    content_type: nanobpmn_engine_core::AgentHistoryContentType::Text,
                    text: Some("be helpful".to_string()),
                    document_reference: None,
                    object: None,
                }]),
            },
            limits: AgentInstanceLimits::default(),
            metrics: AgentInstanceMetrics::default(),
            tools: Vec::new(),
            job_key: 0,
            job_lease: String::new(),
            created_at,
            last_updated_at: created_at,
            completed_at: if matches!(status, AgentInstanceStatus::Completed) {
                created_at + 500
            } else {
                0
            },
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn record(
        key: Key,
        instance_key: Key,
        loop_iteration: i32,
        produced_at: u64,
        role: AgentHistoryRole,
    ) -> AgentHistoryRecord {
        AgentHistoryRecord {
            changed_attributes: Vec::new(),
            agent_history_key: key,
            agent_instance_key: instance_key,
            element_instance_key: instance_key + 1000,
            process_instance_key: 42,
            root_process_instance_key: 42,
            bpmn_process_id: "proc".to_string(),
            process_definition_key: 99,
            tenant_id: "<default>".to_string(),
            job_key: 0,
            job_lease: String::new(),
            loop_iteration,
            role,
            produced_at,
            content: Vec::new(),
            system_prompt: None,
            tool_calls: Vec::new(),
            metrics: None,
            history_item_id: None,
            tools: Vec::new(),
            model: None,
            provider: None,
            limits: None,
            is_duplicate: false,
            commit_status: AgentHistoryCommitStatus::Pending,
        }
    }

    fn store_with(events: &[Event]) -> ReadStore {
        let store = ReadStore::open(None).unwrap();
        let refs: Vec<&Event> = events.iter().collect();
        store.export(&refs).unwrap();
        store
    }

    #[test]
    fn legacy_prompt_rows_preserve_json_looking_text() {
        let agent = instance(1, "agent", AgentInstanceStatus::Thinking, 10);
        let store = store_with(&[Event::AgentInstanceCreated {
            instance_key: 42,
            agent_instance: agent,
        }]);
        for prompt in [
            "ordinary prompt",
            r#"[{"content_type":"Text","text":"not a block"}]"#,
            "[]",
        ] {
            store
                .conn
                .lock()
                .unwrap()
                .execute(
                    "UPDATE agent_instances SET system_prompt = ?1, system_prompt_json = NULL",
                    rusqlite::params![prompt],
                )
                .unwrap();
            let row = store.agent_instance(1).unwrap();
            let encoded = serde_json::to_value(row.system_prompt).unwrap();
            assert!(
                encoded.is_array(),
                "legacy text must decode as a typed content array"
            );
            assert_eq!(encoded[0]["text"], prompt);
        }
    }

    #[test]
    fn canonical_prompt_rows_are_arrays_and_corruption_is_not_legacy_text() {
        let agent = instance(1, "agent", AgentInstanceStatus::Thinking, 10);
        let history = record(2, 1, 1, 20, AgentHistoryRole::Configuration);
        let store = store_with(&[
            Event::AgentInstanceCreated {
                instance_key: 42,
                agent_instance: agent,
            },
            Event::AgentHistoryCreated {
                instance_key: 42,
                record: history,
            },
        ]);
        let blocks = serde_json::json!([
            {"content_type":"Text","text":"[]","document_reference":null,"object":null},
        ]);
        for table in ["agent_instances", "agent_history"] {
            store
                .conn
                .lock()
                .unwrap()
                .execute(
                    &format!(
                        "UPDATE {table} SET system_prompt_json = ?1, system_prompt = 'old fallback'"
                    ),
                    rusqlite::params![blocks.to_string()],
                )
                .unwrap();
        }
        let filter = AgentHistoryFilter {
            agent_instance_key: Some(1),
            commit_status: Some(vec![AgentHistoryCommitStatus::Pending]),
            ..Default::default()
        };
        assert_eq!(
            serde_json::to_value(store.agent_instance(1).unwrap().system_prompt).unwrap(),
            blocks
        );
        assert_eq!(
            serde_json::to_value(&store.agent_history(&filter, None)[0].system_prompt).unwrap(),
            blocks
        );
        for corrupt in [
            "broken JSON",
            r#""a new-column string is not legacy text""#,
            "[{}]",
        ] {
            for table in ["agent_instances", "agent_history"] {
                store
                    .conn
                    .lock()
                    .unwrap()
                    .execute(
                        &format!("UPDATE {table} SET system_prompt_json = ?1"),
                        rusqlite::params![corrupt],
                    )
                    .unwrap();
            }
            assert!(store.try_agent_instance(1).is_err());
            assert!(
                store
                    .try_agent_instances(&AgentInstanceFilter::default(), None)
                    .is_err()
            );
            assert!(store.try_agent_history(&filter, None).is_err());
        }
    }

    #[test]
    fn history_metric_columns_allow_absence_without_zero() {
        let store = ReadStore::open(None).unwrap();
        let conn = store.conn.lock().unwrap();
        for column in [
            "input_tokens",
            "output_tokens",
            "reasoning_token_count",
            "cache_creation_token_count",
            "cache_read_token_count",
            "duration_ms",
        ] {
            let notnull: i64 = conn
                .query_row(
                    "SELECT \"notnull\" FROM pragma_table_info('agent_history') WHERE name = ?1",
                    rusqlite::params![column],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(notnull, 0, "{column} must preserve absent observations");
        }
    }

    #[test]
    fn history_metric_nullability_migration_preserves_existing_observations() {
        let mut old = record(2, 1, 1, 20, AgentHistoryRole::Assistant);
        old.metrics = Some(AgentHistoryMetrics {
            input_tokens: Some(2),
            output_tokens: Some(0),
            duration_ms: Some(7),
            ..Default::default()
        });
        let store = store_with(&[Event::AgentHistoryCreated {
            instance_key: 42,
            record: old,
        }]);
        {
            let conn = store.conn.lock().unwrap();
            let columns = super::target_shape()
                .unwrap()
                .tables
                .remove("agent_history")
                .unwrap()
                .1;
            for column in columns
                .into_iter()
                .filter(|c| c.decl_type == "INTEGER" && !c.notnull && !c.primary_key)
            {
                conn.execute_batch(&format!(
                    "ALTER TABLE agent_history RENAME COLUMN \"{0}\" TO old_metric;
                     ALTER TABLE agent_history ADD COLUMN \"{0}\" INTEGER NOT NULL DEFAULT 0;
                     UPDATE agent_history SET \"{0}\" = COALESCE(old_metric, 0);
                     ALTER TABLE agent_history DROP COLUMN old_metric;",
                    column.name,
                ))
                .unwrap();
            }
            conn.execute("UPDATE agent_history SET metrics_json = NULL", [])
                .unwrap();
            super::reconcile_to_schema(&conn).unwrap();
        }
        let fresh = record(3, 1, 1, 30, AgentHistoryRole::Assistant);
        store
            .export(&[&Event::AgentHistoryCreated {
                instance_key: 42,
                record: fresh,
            }])
            .unwrap();
        let rows = store.agent_history(
            &AgentHistoryFilter {
                agent_instance_key: Some(1),
                commit_status: Some(vec![AgentHistoryCommitStatus::Pending]),
                ..Default::default()
            },
            None,
        );
        assert_eq!(rows[0].input_tokens, Some(2));
        assert_eq!(rows[0].output_tokens, Some(0));
        assert_eq!(rows[1].input_tokens, None);
        assert_eq!(rows[1].metrics_json.as_deref(), Some("null"));
    }

    #[test]
    fn legacy_history_metrics_preserve_observed_counters() {
        let mut history = record(2, 1, 1, 20, AgentHistoryRole::Assistant);
        history.metrics = Some(AgentHistoryMetrics {
            input_tokens: Some(2),
            output_tokens: Some(0),
            duration_ms: Some(7),
            ..Default::default()
        });
        let store = store_with(&[Event::AgentHistoryCreated {
            instance_key: 42,
            record: history,
        }]);
        store
            .conn
            .lock()
            .unwrap()
            .execute("UPDATE agent_history SET metrics_json = NULL", [])
            .unwrap();
        let filter = AgentHistoryFilter {
            agent_instance_key: Some(1),
            commit_status: Some(vec![AgentHistoryCommitStatus::Pending]),
            ..Default::default()
        };
        let rows = store.agent_history(&filter, None);
        let metrics: AgentHistoryMetrics =
            serde_json::from_str(rows[0].metrics_json.as_deref().unwrap()).unwrap();
        assert_eq!(metrics.input_tokens, Some(2));
        assert_eq!(metrics.output_tokens, Some(0));
        assert_eq!(metrics.duration_ms, Some(7));
    }

    #[test]
    fn history_projection_preserves_loop_iteration_boundary() {
        let mut history = record(2, 1, 1, 20, AgentHistoryRole::User);
        history.loop_iteration = i32::MAX;
        let store = store_with(&[Event::AgentHistoryCreated {
            instance_key: 42,
            record: history,
        }]);
        let filter = AgentHistoryFilter {
            agent_instance_key: Some(1),
            commit_status: Some(vec![AgentHistoryCommitStatus::Pending]),
            ..Default::default()
        };
        let rows = store.try_agent_history(&filter, None).unwrap();
        assert_eq!(rows[0].loop_iteration, i32::MAX);
    }

    #[test]
    fn lease_projection_schema_preserves_opaque_and_legacy_values() {
        let store = ReadStore::open(None).unwrap();
        let conn = store.conn.lock().unwrap();
        for (table, column) in [
            ("jobs", "lease_token"),
            ("agent_instances", "job_lease"),
            ("agent_history", "job_lease"),
        ] {
            let ty: String = conn
                .query_row(
                    "SELECT type FROM pragma_table_info(?1) WHERE name = ?2",
                    rusqlite::params![table, column],
                    |r| r.get(0),
                )
                .expect("lease column must exist");
            assert_eq!(ty, "TEXT");
        }
    }

    #[test]
    fn agent_lease_projection_round_trips_opaque_and_legacy_numeric_rows() {
        let mut agent = instance(1, "agent", AgentInstanceStatus::Thinking, 100);
        agent.job_lease = "lease:opaque/0007".into();
        let mut history = record(10, 1, 1, 100, AgentHistoryRole::User);
        history.job_lease = agent.job_lease.clone();
        let store = store_with(&[
            Event::AgentInstanceCreated {
                instance_key: 42,
                agent_instance: agent,
            },
            Event::AgentHistoryCreated {
                instance_key: 42,
                record: history,
            },
        ]);
        let filter = AgentHistoryFilter {
            agent_instance_key: Some(1),
            commit_status: Some(vec![AgentHistoryCommitStatus::Pending]),
            ..Default::default()
        };
        assert_eq!(
            store.agent_instance(1).unwrap().job_lease,
            "lease:opaque/0007"
        );
        assert_eq!(
            store.agent_history(&filter, None)[0].job_lease,
            "lease:opaque/0007"
        );
        {
            let conn = store.conn.lock().unwrap();
            for table in ["agent_instances", "agent_history"] {
                conn.execute_batch(&format!(
                    "ALTER TABLE {table} RENAME COLUMN job_lease TO old_job_lease; \
                     ALTER TABLE {table} ADD COLUMN job_lease INTEGER NOT NULL DEFAULT 0; \
                     UPDATE {table} SET job_lease = 314;"
                ))
                .unwrap();
            }
        }
        assert_eq!(store.agent_instance(1).unwrap().job_lease, "314");
        assert_eq!(store.agent_history(&filter, None)[0].job_lease, "314");
        {
            let conn = store.conn.lock().unwrap();
            super::reconcile_to_schema(&conn).unwrap();
            for table in ["agent_instances", "agent_history"] {
                conn.execute(&format!("UPDATE {table} SET job_lease = ?1"), ["0007"])
                    .unwrap();
            }
        }
        assert_eq!(store.agent_instance(1).unwrap().job_lease, "0007");
        assert_eq!(store.agent_history(&filter, None)[0].job_lease, "0007");
    }

    #[test]
    fn canonical_agent_and_history_filters_apply_every_supplied_identity() {
        let mut agent = instance(1, "agent", AgentInstanceStatus::Thinking, 100);
        agent.element_instance_keys.push(2002);
        agent.process_definition_version_tag = Some("v1".into());
        let mut history = record(10, 1, 3, 200, AgentHistoryRole::Assistant);
        history.job_key = 700;
        let store = store_with(&[
            Event::AgentInstanceCreated {
                instance_key: 42,
                agent_instance: agent,
            },
            Event::AgentHistoryCreated {
                instance_key: 42,
                record: history,
            },
        ]);
        let mut filter = AgentInstanceFilter {
            process_definition_id: Some("proc".into()),
            process_definition_version: Some(1),
            process_definition_version_tag: Some("v1".into()),
            element_instance_keys: vec![1001, 2002],
            creation_date_ms: Some(100),
            last_updated_date_ms: Some(100),
            ..Default::default()
        };
        assert_eq!(store.agent_instances(&filter, None).len(), 1);
        filter.element_instance_keys.push(9999);
        assert!(store.agent_instances(&filter, None).is_empty());
        let mut filter = AgentHistoryFilter {
            history_item_key: Some(10),
            element_instance_key: Some(1001),
            job_key: Some(700),
            loop_iteration: Some(3),
            produced_at_ms: Some(200),
            role: Some(AgentHistoryRole::Assistant),
            commit_status: Some(vec![AgentHistoryCommitStatus::Pending]),
            ..Default::default()
        };
        assert_eq!(store.agent_history(&filter, None).len(), 1);
        filter.job_key = Some(701);
        assert!(store.agent_history(&filter, None).is_empty());
    }

    #[test]
    fn projects_agent_instances_and_searches_by_filter() {
        let store = store_with(&[
            Event::AgentInstanceCreated {
                instance_key: 42,
                agent_instance: instance(1, "agent-a", AgentInstanceStatus::Thinking, 100),
            },
            Event::AgentInstanceCreated {
                instance_key: 42,
                agent_instance: instance(2, "agent-b", AgentInstanceStatus::Completed, 200),
            },
        ]);

        // Unfiltered: both instances project.
        let all = store.agent_instances(&AgentInstanceFilter::default(), None);
        assert_eq!(all.len(), 2);

        // Filter by element_id returns just that instance.
        let by_element = store.agent_instances(
            &AgentInstanceFilter {
                element_id: Some("agent-b".to_string()),
                ..Default::default()
            },
            None,
        );
        assert_eq!(by_element.len(), 1);
        assert_eq!(by_element[0].agent_instance_key, 2);
        assert_eq!(by_element[0].status, AgentInstanceStatus::Completed);
        assert_eq!(by_element[0].completion_date_ms, Some(700));

        // Filter by status.
        let thinking = store.agent_instances(
            &AgentInstanceFilter {
                status: Some(AgentInstanceStatus::Thinking),
                ..Default::default()
            },
            None,
        );
        assert_eq!(thinking.len(), 1);
        assert_eq!(thinking[0].agent_instance_key, 1);

        // By-key lookup.
        assert_eq!(store.agent_instance(1).unwrap().element_id, "agent-a");
        assert!(store.agent_instance(999).is_none());
    }

    #[test]
    fn projects_version_tag_and_element_instance_keys() {
        let mut ai = instance(5, "agent-c", AgentInstanceStatus::Idle, 300);
        ai.process_definition_version_tag = Some("v1.2.3".to_string());
        ai.element_instance_keys = vec![1005, 2005, 3005];
        let store = store_with(&[Event::AgentInstanceCreated {
            instance_key: 42,
            agent_instance: ai,
        }]);
        let row = store.agent_instance(5).expect("instance projects");
        assert_eq!(
            row.process_definition_version_tag.as_deref(),
            Some("v1.2.3"),
            "the version tag is projected"
        );
        assert_eq!(
            row.element_instance_keys,
            vec![1005, 2005, 3005],
            "the full element-instance-key set is projected as a JSON array"
        );

        // ON CONFLICT must refresh the version tag: a later UPDATED event carrying a
        // non-null tag replaces a previously-projected NULL (additive projection
        // preserves the row, so the UPSERT must update the tag, not keep it stale).
        let mut untagged = instance(6, "agent-d", AgentInstanceStatus::Initializing, 400);
        untagged.process_definition_version_tag = None;
        let mut tagged = instance(6, "agent-d", AgentInstanceStatus::Thinking, 400);
        tagged.process_definition_version_tag = Some("v9.9.9".to_string());
        let store = store_with(&[
            Event::AgentInstanceCreated {
                instance_key: 42,
                agent_instance: untagged,
            },
            Event::AgentInstanceUpdated {
                instance_key: 42,
                agent_instance: tagged,
            },
        ]);
        let row = store.agent_instance(6).expect("instance projects");
        assert_eq!(
            row.process_definition_version_tag.as_deref(),
            Some("v9.9.9"),
            "the version tag is refreshed on conflicting UPSERT, not left NULL"
        );
    }

    #[test]
    fn instance_sort_by_each_field_works() {
        let store = store_with(&[
            Event::AgentInstanceCreated {
                instance_key: 42,
                agent_instance: instance(2, "b", AgentInstanceStatus::Thinking, 100),
            },
            Event::AgentInstanceCreated {
                instance_key: 42,
                agent_instance: instance(1, "a", AgentInstanceStatus::Thinking, 300),
            },
        ]);

        let by_key_asc = store.agent_instances(
            &AgentInstanceFilter::default(),
            Some((AgentInstanceSortField::AgentInstanceKey, SortOrder::Asc)),
        );
        assert_eq!(
            by_key_asc
                .iter()
                .map(|r| r.agent_instance_key)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );

        let by_creation_desc = store.agent_instances(
            &AgentInstanceFilter::default(),
            Some((AgentInstanceSortField::CreationDate, SortOrder::Desc)),
        );
        assert_eq!(
            by_creation_desc
                .iter()
                .map(|r| r.agent_instance_key)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );

        // Every declared sort field must produce a stable, non-panicking order.
        for field in [
            AgentInstanceSortField::AgentInstanceKey,
            AgentInstanceSortField::AgentDefinitionKey,
            AgentInstanceSortField::Status,
            AgentInstanceSortField::ElementId,
            AgentInstanceSortField::ProcessInstanceKey,
            AgentInstanceSortField::RootProcessInstanceKey,
            AgentInstanceSortField::ProcessDefinitionKey,
            AgentInstanceSortField::TenantId,
            AgentInstanceSortField::CreationDate,
            AgentInstanceSortField::LastUpdatedDate,
            AgentInstanceSortField::CompletionDate,
        ] {
            let rows = store.agent_instances(
                &AgentInstanceFilter::default(),
                Some((field, SortOrder::Asc)),
            );
            assert_eq!(rows.len(), 2, "sort by {field:?} must return all rows");
        }
    }

    #[test]
    fn history_default_filter_returns_only_committed() {
        // Three turns; commit #10, discard #12, leave #11 pending.
        let store = store_with(&[
            Event::AgentHistoryCreated {
                instance_key: 42,
                record: record(10, 1, 0, 100, AgentHistoryRole::User),
            },
            Event::AgentHistoryCreated {
                instance_key: 42,
                record: record(11, 1, 0, 200, AgentHistoryRole::Assistant),
            },
            Event::AgentHistoryCreated {
                instance_key: 42,
                record: record(12, 1, 1, 300, AgentHistoryRole::ToolResult),
            },
            Event::AgentHistoryCommitted {
                instance_key: 42,
                agent_instance_key: 1,
                agent_history_keys: vec![10],
            },
            Event::AgentHistoryDiscarded {
                instance_key: 42,
                agent_instance_key: 1,
                agent_history_keys: vec![12],
            },
        ]);

        // No commit_status filter → COMMITTED only.
        let committed = store.agent_history(
            &AgentHistoryFilter {
                agent_instance_key: Some(1),
                ..Default::default()
            },
            None,
        );
        assert_eq!(
            committed
                .iter()
                .map(|r| r.agent_history_key)
                .collect::<Vec<_>>(),
            vec![10]
        );

        // Explicit PENDING filter surfaces the still-pending turn.
        let pending = store.agent_history(
            &AgentHistoryFilter {
                agent_instance_key: Some(1),
                process_instance_key: None,
                commit_status: Some(vec![AgentHistoryCommitStatus::Pending]),
                ..Default::default()
            },
            None,
        );
        assert_eq!(
            pending
                .iter()
                .map(|r| r.agent_history_key)
                .collect::<Vec<_>>(),
            vec![11]
        );

        // Explicit DISCARDED filter surfaces the discarded turn.
        let discarded = store.agent_history(
            &AgentHistoryFilter {
                agent_instance_key: Some(1),
                process_instance_key: None,
                commit_status: Some(vec![AgentHistoryCommitStatus::Discarded]),
                ..Default::default()
            },
            None,
        );
        assert_eq!(
            discarded
                .iter()
                .map(|r| r.agent_history_key)
                .collect::<Vec<_>>(),
            vec![12]
        );

        // Empty list is treated as the COMMITTED default, not "match nothing".
        let empty = store.agent_history(
            &AgentHistoryFilter {
                agent_instance_key: Some(1),
                process_instance_key: None,
                commit_status: Some(vec![]),
                ..Default::default()
            },
            None,
        );
        assert_eq!(
            empty
                .iter()
                .map(|r| r.agent_history_key)
                .collect::<Vec<_>>(),
            vec![10]
        );
    }

    #[test]
    fn history_sort_by_each_field_works() {
        // All three committed so the default filter returns them.
        let store = store_with(&[
            Event::AgentHistoryCreated {
                instance_key: 42,
                record: record(30, 1, 2, 100, AgentHistoryRole::User),
            },
            Event::AgentHistoryCreated {
                instance_key: 42,
                record: record(31, 1, 0, 300, AgentHistoryRole::Assistant),
            },
            Event::AgentHistoryCreated {
                instance_key: 42,
                record: record(32, 1, 1, 200, AgentHistoryRole::ToolResult),
            },
            Event::AgentHistoryCommitted {
                instance_key: 42,
                agent_instance_key: 1,
                agent_history_keys: vec![30, 31, 32],
            },
        ]);
        let filter = AgentHistoryFilter {
            agent_instance_key: Some(1),
            ..Default::default()
        };

        let by_produced = store.agent_history(
            &filter,
            Some((AgentHistorySortField::ProducedAt, SortOrder::Asc)),
        );
        assert_eq!(
            by_produced
                .iter()
                .map(|r| r.produced_at_ms)
                .collect::<Vec<_>>(),
            vec![100, 200, 300]
        );

        let by_key = store.agent_history(
            &filter,
            Some((AgentHistorySortField::HistoryItemKey, SortOrder::Desc)),
        );
        assert_eq!(
            by_key
                .iter()
                .map(|r| r.agent_history_key)
                .collect::<Vec<_>>(),
            vec![32, 31, 30]
        );

        let by_iter = store.agent_history(
            &filter,
            Some((AgentHistorySortField::LoopIteration, SortOrder::Asc)),
        );
        assert_eq!(
            by_iter.iter().map(|r| r.loop_iteration).collect::<Vec<_>>(),
            vec![0, 1, 2]
        );
    }

    #[test]
    fn projection_is_idempotent_and_replay_safe() {
        let created = Event::AgentInstanceCreated {
            instance_key: 42,
            agent_instance: instance(1, "agent-a", AgentInstanceStatus::Thinking, 100),
        };
        let turn = Event::AgentHistoryCreated {
            instance_key: 42,
            record: record(10, 1, 0, 100, AgentHistoryRole::User),
        };
        let commit = Event::AgentHistoryCommitted {
            instance_key: 42,
            agent_instance_key: 1,
            agent_history_keys: vec![10],
        };
        let store = store_with(&[created.clone(), turn.clone(), commit.clone()]);

        // Redeliver every event: counts stay put and the commit status is preserved.
        store.export(&[&created, &turn, &commit]).unwrap();
        assert_eq!(
            store
                .agent_instances(&AgentInstanceFilter::default(), None)
                .len(),
            1
        );
        let committed = store.agent_history(
            &AgentHistoryFilter {
                agent_instance_key: Some(1),
                ..Default::default()
            },
            None,
        );
        assert_eq!(committed.len(), 1);
        assert_eq!(
            committed[0].commit_status,
            AgentHistoryCommitStatus::Committed
        );

        // A committed turn can never be discarded (PENDING-guarded transition).
        store
            .export(&[&Event::AgentHistoryDiscarded {
                instance_key: 42,
                agent_instance_key: 1,
                agent_history_keys: vec![10],
            }])
            .unwrap();
        let still = store.agent_history(
            &AgentHistoryFilter {
                agent_instance_key: Some(1),
                process_instance_key: None,
                commit_status: Some(vec![AgentHistoryCommitStatus::Discarded]),
                ..Default::default()
            },
            None,
        );
        assert!(
            still.is_empty(),
            "a committed turn must not become discarded"
        );
    }

    #[test]
    fn update_and_completed_events_reproject_the_mutable_state() {
        // The read model must reflect a PATCH's status advance and a COMPLETE's
        // terminal status + completion date (both events reuse the CREATE upsert),
        // else GET-after-PATCH on the REST channel (S5) reports stale INITIALIZING.
        let created = Event::AgentInstanceCreated {
            instance_key: 42,
            agent_instance: instance(1, "agent-a", AgentInstanceStatus::Initializing, 100),
        };
        let store = store_with(&[created]);
        assert_eq!(
            store.agent_instance(1).unwrap().status,
            AgentInstanceStatus::Initializing
        );

        // UPDATED advances the projected status.
        let updated = Event::AgentInstanceUpdated {
            instance_key: 42,
            agent_instance: instance(1, "agent-a", AgentInstanceStatus::Thinking, 100),
        };
        store.export(&[&updated]).unwrap();
        assert_eq!(
            store.agent_instance(1).unwrap().status,
            AgentInstanceStatus::Thinking,
            "an AgentInstanceUpdated event reprojects the advanced status"
        );

        // COMPLETED drives to the terminal status and records the completion date.
        let completed = Event::AgentInstanceCompleted {
            instance_key: 42,
            agent_instance: instance(1, "agent-a", AgentInstanceStatus::Completed, 100),
        };
        store.export(&[&completed]).unwrap();
        let row = store.agent_instance(1).unwrap();
        assert_eq!(row.status, AgentInstanceStatus::Completed);
        assert!(
            row.completion_date_ms.is_some(),
            "a completed instance carries its completion date"
        );
    }
}
