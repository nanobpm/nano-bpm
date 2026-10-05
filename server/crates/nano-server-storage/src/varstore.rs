//! An authoritative, boot-surviving durable store for process-instance variables.
//!
//! This is the persistence tier that lets snapshots become **control-only**
//! (lean). A periodic engine snapshot otherwise has to serialize every resident
//! instance's variable payload — multi-GB under a large active backlog — and the
//! resulting file `sync_all` (tens of seconds) starves the journal fsync, driving
//! the phase-B tail latency. Moving variables into this store lets the snapshot
//! carry just the control state (jobs, timers, scopes, tokens), so the snapshot
//! file shrinks to the working-set skeleton and its fsync stops competing.
//!
//! Unlike [`crate::varspill::VarSpillStore`] — a *derived, destructive* cache
//! whose rows are taken on rehydration (and only retained across a restart for
//! legacy placeholder-snapshot recovery, #1331) — this store is
//! **authoritative**: it survives a restart and is the source recovery reads to
//! restore variables the lean snapshot omitted. It is written incrementally at
//! each snapshot checkpoint from the engine's dirty-var set (the instances whose
//! variables changed since the last checkpoint), not per event, so the hot
//! single-writer engine thread never touches SQLite; only an off-thread batch
//! does, at snapshot cadence.
//!
//! ## Consistency contract with recovery
//! Each [`checkpoint`](VarStore::checkpoint) upserts the changed instances'
//! *whole* current maps, deletes forgotten (terminal) instances, and records the
//! partition's event `position` — all in one transaction. Because the store
//! accumulates (it is never wiped) it therefore holds the exact current variable
//! map for *every live instance* as of the recorded position. A lean snapshot at
//! that same position can then be rehydrated by [`load_all`](VarStore::load_all)
//! plus a replay of the journal tail past the position (which re-applies any
//! newer creates / variable merges). `synchronous=NORMAL` is safe: position and
//! contents commit together, so a power-loss rollback leaves them mutually
//! consistent, and compaction is gated on this position so the journal still
//! holds every segment needed to replay forward from it.

// Phase 1 scaffolding: the public API (open/checkpoint/position/load_all/…) is
// exercised by unit tests now and wired into the snapshot maintenance + recovery
// paths in later phases (behind NANOBPMN_LEAN_SNAPSHOT). Remove this allow once
// those call sites land.
#![allow(dead_code)]

use std::collections::HashMap;
use std::path::Path;
use std::sync::Mutex;

use nanobpmn_engine_core::{Key, Value};
use rusqlite::{Connection, OptionalExtension, params};

/// The on-disk schema version. Bump when the row/serialization format changes so
/// an incompatible store is discarded (and repopulated from the next full
/// checkpoint) rather than misread.
const SCHEMA_VERSION: i64 = 1;

/// A SQLite-backed authoritative `instance key -> variables` map plus a
/// per-partition event position cursor.
pub struct VarStore {
    conn: Mutex<Connection>,
}

/// Whether lean (control-only) snapshots backed by the authoritative
/// [`VarStore`] are enabled, from `NANOBPMN_LEAN_SNAPSHOT` (default off).
/// Only the bounded-disk segmented multi-partition paths honour it.
pub fn lean_snapshot_enabled() -> bool {
    matches!(
        std::env::var("NANOBPMN_LEAN_SNAPSHOT")
            .unwrap_or_default()
            .trim(),
        "1" | "true" | "on" | "yes"
    )
}

/// The interval between truncating WAL checkpoints of the durable var-store,
/// from `NANOBPMN_VARSTORE_WAL_CHECKPOINT_SECS` (default 30s; `0`/`off` disables).
/// Bounds the `-wal` file's disk growth under a sustained write load (see
/// [`VarStore::checkpoint_wal`]). Driven from the maintenance loop, never the hot
/// engine thread.
pub fn wal_checkpoint_interval() -> Option<std::time::Duration> {
    match std::env::var("NANOBPMN_VARSTORE_WAL_CHECKPOINT_SECS") {
        Ok(v) => {
            let t = v.trim();
            if matches!(t.to_ascii_lowercase().as_str(), "off" | "none" | "false") {
                return None;
            }
            match t.parse::<u64>() {
                Ok(0) => None,
                Ok(secs) => Some(std::time::Duration::from_secs(secs)),
                Err(_) => Some(std::time::Duration::from_secs(30)),
            }
        }
        Err(_) => Some(std::time::Duration::from_secs(30)),
    }
}

impl VarStore {
    /// Opens (creating if absent) the authoritative variable store at `path`, or
    /// an in-memory store when `path` is `None` (tests / ephemeral runs).
    ///
    /// Crucially — and unlike the spill cache — this **does not** wipe existing
    /// rows: the store is durable across restarts. A schema-version mismatch is
    /// the one case where the tables are dropped and recreated empty (the format
    /// changed, so old rows are unreadable); recovery then falls back to a full
    /// snapshot until the next checkpoint repopulates the store.
    pub fn open(path: Option<&Path>) -> rusqlite::Result<Self> {
        let conn = match path {
            Some(p) => Connection::open(p)?,
            None => Connection::open_in_memory()?,
        };
        conn.execute_batch(
            "PRAGMA journal_mode=WAL;
             PRAGMA synchronous=NORMAL;
             CREATE TABLE IF NOT EXISTS schema (version INTEGER NOT NULL);
             CREATE TABLE IF NOT EXISTS vars (key INTEGER PRIMARY KEY, vars TEXT NOT NULL);
             CREATE TABLE IF NOT EXISTS position (partition INTEGER PRIMARY KEY, pos INTEGER NOT NULL);",
        )?;
        let version: Option<i64> = conn
            .query_row("SELECT version FROM schema LIMIT 1", [], |r| r.get(0))
            .optional()?;
        match version {
            Some(v) if v == SCHEMA_VERSION => {}
            Some(_) => {
                // Incompatible format: discard and recreate empty.
                conn.execute_batch("DELETE FROM vars; DELETE FROM position; DELETE FROM schema;")?;
                conn.execute(
                    "INSERT INTO schema (version) VALUES (?1)",
                    params![SCHEMA_VERSION],
                )?;
            }
            None => {
                conn.execute(
                    "INSERT INTO schema (version) VALUES (?1)",
                    params![SCHEMA_VERSION],
                )?;
            }
        }
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    /// Applies one checkpoint for `partition` in a single transaction: upsert the
    /// current whole variable map of every instance in `upserts`, delete every
    /// key in `forgets` (instances that reached a terminal state and were
    /// evicted), and record `position` (the partition's applied event count that
    /// this checkpoint is current through). Atomicity keeps the recorded position
    /// consistent with the rows it describes.
    pub fn checkpoint(
        &self,
        partition: u64,
        position: u64,
        upserts: &[(Key, &HashMap<String, Value>)],
        forgets: &[Key],
    ) -> rusqlite::Result<()> {
        let mut conn = self.conn.lock().expect("var store poisoned");
        let tx = conn.transaction()?;
        {
            let mut put =
                tx.prepare_cached("INSERT OR REPLACE INTO vars (key, vars) VALUES (?1, ?2)")?;
            for (key, vars) in upserts {
                let json = serde_json::to_string(vars).expect("variables serialize to JSON");
                put.execute(params![*key as i64, json])?;
            }
            let mut del = tx.prepare_cached("DELETE FROM vars WHERE key = ?1")?;
            for &key in forgets {
                del.execute(params![key as i64])?;
            }
            tx.prepare_cached("INSERT OR REPLACE INTO position (partition, pos) VALUES (?1, ?2)")?
                .execute(params![partition as i64, position as i64])?;
        }
        tx.commit()
    }

    /// Writes through a single instance's current variable map outside the
    /// checkpoint cycle — used when variable spill evicts an instance's payload
    /// from hot RAM. The row must be durable *before* the payload leaves memory
    /// (the store is now the only copy the recovery path reads), so spill calls
    /// this at eviction. It does not advance the partition position: the row may
    /// be ahead of the last checkpoint, which recovery tolerates (the journal
    /// tail past the checkpoint re-applies any newer change).
    pub fn put_current(&self, key: Key, vars: &HashMap<String, Value>) -> rusqlite::Result<()> {
        let json = serde_json::to_string(vars).expect("variables serialize to JSON");
        let conn = self.conn.lock().expect("var store poisoned");
        conn.execute(
            "INSERT OR REPLACE INTO vars (key, vars) VALUES (?1, ?2)",
            params![key as i64, json],
        )?;
        Ok(())
    }

    /// Reads an instance's current variable map **non-destructively** (unlike the
    /// spill cache's `take`): the store is authoritative, so rehydrating a spilled
    /// instance on job activation must leave the durable row in place for the next
    /// recovery. `None` if the instance is absent.
    pub fn get(&self, key: Key) -> Option<HashMap<String, Value>> {
        let conn = self.conn.lock().expect("var store poisoned");
        let json: Option<String> = conn
            .query_row(
                "SELECT vars FROM vars WHERE key = ?1",
                params![key as i64],
                |r| r.get(0),
            )
            .optional()
            .ok()?;
        serde_json::from_str(&json?).ok()
    }

    /// The last checkpointed event position for `partition` (0 if never
    /// checkpointed). Compaction gates on this so no segment needed to replay the
    /// journal tail past it is deleted.
    pub fn position(&self, partition: u64) -> u64 {
        let conn = self.conn.lock().expect("var store poisoned");
        conn.query_row(
            "SELECT pos FROM position WHERE partition = ?1",
            params![partition as i64],
            |r| r.get::<_, i64>(0),
        )
        .optional()
        .ok()
        .flatten()
        .unwrap_or(0) as u64
    }

    /// Bulk-loads every stored instance's current variable map, for recovery to
    /// rehydrate the instances a lean snapshot restored with empty variables.
    /// Rows that fail to deserialize are skipped (the journal tail can still
    /// re-create them), keeping a partially-corrupt store non-fatal.
    pub fn load_all(&self) -> HashMap<Key, HashMap<String, Value>> {
        let conn = self.conn.lock().expect("var store poisoned");
        let mut stmt = match conn.prepare("SELECT key, vars FROM vars") {
            Ok(s) => s,
            Err(_) => return HashMap::new(),
        };
        let rows = match stmt.query_map([], |r| {
            Ok((r.get::<_, i64>(0)? as Key, r.get::<_, String>(1)?))
        }) {
            Ok(rows) => rows,
            Err(_) => return HashMap::new(),
        };
        let mut out = HashMap::new();
        for row in rows.flatten() {
            let (key, json) = row;
            if let Ok(vars) = serde_json::from_str::<HashMap<String, Value>>(&json) {
                out.insert(key, vars);
            }
        }
        out
    }

    /// Forces a **truncating** WAL checkpoint (`PRAGMA wal_checkpoint(TRUNCATE)`):
    /// flushes the write-ahead log's committed pages back into the main database
    /// file and then resets the `-wal` file to zero length.
    ///
    /// Why this is needed. The store runs in `journal_mode=WAL` with
    /// `synchronous=NORMAL`, and it is written continuously (every snapshot tick
    /// plus each spill eviction) while recovery/`load_all` may read concurrently.
    /// SQLite's automatic checkpoint moves pages into the db file but never
    /// *shrinks* the `-wal` file, and a long-lived reader can pin the checkpoint
    /// so the WAL only grows — observed climbing to ~843 MB during a large-payload
    /// soak. A periodic truncating checkpoint (driven off-thread from the
    /// maintenance loop, never the hot engine thread) bounds that disk growth. It
    /// is a no-op cost when the WAL is already small.
    ///
    /// A `SQLITE_BUSY` from a concurrent reader is not fatal — the WAL is simply
    /// truncated on a later tick — so callers should log-and-continue on error.
    pub fn checkpoint_wal(&self) -> rusqlite::Result<()> {
        let conn = self.conn.lock().expect("var store poisoned");
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
    }

    /// Number of instances currently held. Test/observability helper.
    pub fn len(&self) -> usize {
        let conn = self.conn.lock().expect("var store poisoned");
        conn.query_row("SELECT COUNT(*) FROM vars", [], |r| r.get::<_, i64>(0))
            .unwrap_or(0) as usize
    }

    /// Whether the store holds no instances.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vars(payload: &str) -> HashMap<String, Value> {
        let mut m = HashMap::new();
        m.insert("data".to_string(), Value::Str(payload.to_string()));
        m
    }

    #[test]
    fn checkpoint_upserts_and_load_all_round_trips() {
        let store = VarStore::open(None).unwrap();
        let a = vars("alpha");
        let b = vars("beta");
        store.checkpoint(0, 10, &[(1, &a), (2, &b)], &[]).unwrap();

        let all = store.load_all();
        assert_eq!(all.len(), 2);
        assert_eq!(
            all.get(&1).and_then(|m| m.get("data")),
            Some(&Value::Str("alpha".into()))
        );
        assert_eq!(
            all.get(&2).and_then(|m| m.get("data")),
            Some(&Value::Str("beta".into()))
        );
        assert_eq!(store.position(0), 10);
    }

    #[test]
    fn upsert_replaces_and_forget_deletes() {
        let store = VarStore::open(None).unwrap();
        let v1 = vars("one");
        store.checkpoint(0, 1, &[(7, &v1)], &[]).unwrap();
        // Replace 7's map, add 8, forget a not-yet-present key (no-op).
        let v2 = vars("two");
        let v8 = vars("eight");
        store
            .checkpoint(0, 2, &[(7, &v2), (8, &v8)], &[99])
            .unwrap();
        let all = store.load_all();
        assert_eq!(
            all.get(&7).and_then(|m| m.get("data")),
            Some(&Value::Str("two".into()))
        );
        assert_eq!(all.len(), 2);
        // Forget 7 in a later checkpoint.
        store.checkpoint(0, 3, &[], &[7]).unwrap();
        let all = store.load_all();
        assert!(!all.contains_key(&7), "forgotten instance is gone");
        assert!(all.contains_key(&8));
        assert_eq!(store.position(0), 3);
    }

    #[test]
    fn per_partition_positions_are_independent() {
        let store = VarStore::open(None).unwrap();
        let v = vars("x");
        store.checkpoint(0, 5, &[(1, &v)], &[]).unwrap();
        store.checkpoint(1, 9, &[(2, &v)], &[]).unwrap();
        assert_eq!(store.position(0), 5);
        assert_eq!(store.position(1), 9);
        assert_eq!(store.position(2), 0, "unseen partition is 0");
    }

    #[test]
    fn survives_reopen() {
        let dir = std::env::temp_dir().join(format!(
            "nanobpmn-varstore-reopen-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("vars.sqlite");
        {
            let store = VarStore::open(Some(&path)).unwrap();
            let v = vars("durable");
            store.checkpoint(0, 42, &[(3, &v)], &[]).unwrap();
        }
        // Reopen: rows and position must survive (authoritative, not wiped).
        let store = VarStore::open(Some(&path)).unwrap();
        let all = store.load_all();
        assert_eq!(
            all.get(&3).and_then(|m| m.get("data")),
            Some(&Value::Str("durable".into()))
        );
        assert_eq!(store.position(0), 42);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn wal_checkpoint_truncates_and_preserves_contents() {
        let dir = std::env::temp_dir().join(format!(
            "nanobpmn-varstore-wal-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("vars.sqlite");
        let wal = dir.join("vars.sqlite-wal");
        let store = VarStore::open(Some(&path)).unwrap();
        // Write enough distinct instances to grow the WAL past its initial size.
        for i in 0..500i64 {
            let v = vars(&format!("payload-{i}"));
            store
                .checkpoint(0, i as u64, &[(i as Key, &v)], &[])
                .unwrap();
        }
        let before = std::fs::metadata(&wal).map(|m| m.len()).unwrap_or(0);
        assert!(before > 0, "WAL should have grown from the writes");

        store
            .checkpoint_wal()
            .expect("truncating checkpoint succeeds");

        // TRUNCATE resets the -wal file to zero length; contents survive (they were
        // flushed into the main db file).
        let after = std::fs::metadata(&wal).map(|m| m.len()).unwrap_or(0);
        assert!(
            after < before,
            "WAL should shrink after TRUNCATE: {before} -> {after}"
        );
        assert_eq!(store.len(), 500, "all rows survive the checkpoint");
        assert_eq!(
            store.get(499).and_then(|m| m.get("data").cloned()),
            Some(Value::Str("payload-499".into()))
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
}
