//! Shared SQLite on-disk space accounting and reclamation helpers.
//!
//! Both the read-model store ([`crate::readstore`]) and the destructive var-spill
//! cache ([`crate::varspill`]) sit on SQLite files whose *live* row set is kept
//! bounded (adaptive retention / destructive reads + terminal eviction), but whose
//! *file* only ever grows to the freelist high-water mark: SQLite reuses freed
//! pages for later inserts and never returns them to the OS on its own. A single
//! backlog spike therefore inflates the file for the lifetime of the process.
//!
//! This module centralises the two primitives that address that so neither store
//! duplicates the knowledge:
//!
//! * [`page_stats`] — O(1) `(file_bytes, live_bytes)` accounting from the header.
//! * [`freelist_bytes`] — O(1) reclaimable-bytes accounting (`file − live`).
//! * [`enable_incremental_auto_vacuum`] — puts a store into `INCREMENTAL`
//!   auto-vacuum mode so freed pages land on a reclaimable freelist.
//! * [`reclaim_freelist_step`] — returns *up to `max_pages`* freelist pages to the
//!   OS via a bounded `incremental_vacuum`, plus a WAL truncate. Bounded so a caller
//!   can drain a large freelist cooperatively (releasing its lock between passes)
//!   instead of blocking on one long copy-back; pass `max_pages == 0` for a full
//!   one-shot drain (used where blocking is acceptable, e.g. tests/shutdown).
//! * [`freelist_bytes`] — reports the currently reclaimable freelist size, the
//!   signal the background reclaim worker gates on.
//!
//! The reclaim primitives are deliberately fsync-heavy (checkpoint + copy-back), so
//! callers must keep them **off any latency-critical path**: on a shared disk the
//! fsyncs contend with the engine's raft-log fsync and can trip its saturation
//! guard. [`crate::varspill`] therefore drives them from a background thread that
//! only fires while the store is quiescent.

/// Reads a SQLite database's size as `(file_bytes, live_bytes)` from its header.
///
/// The canonical implementation now lives in the shared `nanobpmn-read-model`
/// crate (it underpins the read model's own pruning/sizing) and is re-exported
/// here so this module — and its var-spill caller — reuse the exact same
/// derivation with no drift. See [`nanobpmn_read_model::page_stats`].
pub use nanobpmn_read_model::page_stats;
use rusqlite::Connection;

/// Freelist bytes currently reclaimable: `file_bytes − live_bytes` (see
/// [`page_stats`]). O(1) header reads, cheap enough to poll.
pub fn freelist_bytes(conn: &Connection) -> u64 {
    let (file, live) = page_stats(conn);
    file.saturating_sub(live)
}

/// Puts `conn` into `INCREMENTAL` auto-vacuum mode so that deleted rows move their
/// pages onto a freelist that [`reclaim_freelist_step`] can later return to the OS.
///
/// Setting the pragma only *records* the request; the accompanying `VACUUM`
/// rewrites the database to install the auto-vacuum pointer map and actually switch
/// mode (a new database is fine before any table exists, but an existing one needs
/// the VACUUM). The VACUUM runs only when the database is not already
/// `INCREMENTAL`, so it is paid once per store (free on a fresh, empty file) and
/// re-opening a populated store — e.g. [`crate::varspill`], whose rows survive a
/// restart (#1331) — never rewrites it.
pub fn enable_incremental_auto_vacuum(conn: &Connection) -> rusqlite::Result<()> {
    // auto_vacuum: 0 = NONE, 1 = FULL, 2 = INCREMENTAL.
    let mode: i64 = conn.query_row("PRAGMA auto_vacuum", [], |r| r.get(0))?;
    if mode != 2 {
        conn.execute_batch("PRAGMA auto_vacuum=INCREMENTAL; VACUUM;")?;
    }
    Ok(())
}

/// Reclaims freelist pages to the OS in a **single bounded pass**: at most
/// `max_pages` pages are moved out of the main file (`max_pages == 0` means "all
/// reclaimable pages", i.e. a full drain). Returns the bytes the file shrank by.
///
/// Sequence (WAL-mode safe): the pages freed by prior committed deletes live in the
/// `-wal` sidecar until a checkpoint copies them into the main database, and
/// `incremental_vacuum` can only truncate pages already in the main file — so we
/// `wal_checkpoint(TRUNCATE)` first (land the frees), then vacuum (move freed pages
/// off the end and shrink the main file), then checkpoint again to flush the
/// truncation out of the WAL. Checkpoints are best-effort: a concurrent reader can
/// hold TRUNCATE back, which is fine — the next pass retries.
///
/// **Bounded on purpose.** `PRAGMA incremental_vacuum(N)` frees pages as the
/// statement is *stepped*, and rusqlite's `execute*`/`execute_batch` step a
/// no-result pragma only once (freeing a single page); driving a prepared statement
/// to completion frees the requested `N` (or all, when unbounded), the way the
/// sqlite3 CLI's exec loop does. Capping `N` lets a caller drain a huge freelist
/// across several short passes, releasing its connection lock between them, rather
/// than holding it for one multi-second copy-back.
///
/// This is fsync-heavy — **never call it on a latency-critical path.**
pub fn reclaim_freelist_step(conn: &Connection, max_pages: u32) -> rusqlite::Result<u64> {
    let (file_before, _) = page_stats(conn);
    let _ = conn.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_| {
        Ok::<_, rusqlite::Error>(())
    });
    let sql = if max_pages == 0 {
        "PRAGMA incremental_vacuum".to_string()
    } else {
        format!("PRAGMA incremental_vacuum({max_pages})")
    };
    {
        let mut stmt = conn.prepare(&sql)?;
        let mut rows = stmt.query([])?;
        while rows.next()?.is_some() {}
    }
    let _ = conn.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_| {
        Ok::<_, rusqlite::Error>(())
    });
    let (file_after, _) = page_stats(conn);
    Ok(file_before.saturating_sub(file_after))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn open_incremental() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        enable_incremental_auto_vacuum(&conn).unwrap();
        conn.execute_batch(
            "PRAGMA journal_mode=WAL;
             CREATE TABLE t (k INTEGER PRIMARY KEY, v TEXT NOT NULL);",
        )
        .unwrap();
        conn
    }

    #[test]
    fn page_stats_reports_growth_and_freelist() {
        let conn = open_incremental();
        let (file0, _live0) = page_stats(&conn);
        let blob = "x".repeat(4096);
        for k in 0..2000 {
            conn.execute("INSERT INTO t (k, v) VALUES (?1, ?2)", (k, &blob))
                .unwrap();
        }
        let (file1, live1) = page_stats(&conn);
        assert!(file1 > file0, "file should grow after inserts");
        assert!(live1 > 0);
        // Delete everything: pages move onto the freelist, file stays at high-water.
        conn.execute("DELETE FROM t", []).unwrap();
        let (file2, live2) = page_stats(&conn);
        assert!(
            file2.saturating_sub(live2) > 0,
            "deleted pages should sit on the freelist"
        );
    }

    #[test]
    fn full_drain_shrinks_file_to_live_set() {
        let conn = open_incremental();
        let blob = "x".repeat(4096);
        for k in 0..2000 {
            conn.execute("INSERT INTO t (k, v) VALUES (?1, ?2)", (k, &blob))
                .unwrap();
        }
        let (file_peak, _) = page_stats(&conn);
        conn.execute("DELETE FROM t", []).unwrap();
        assert!(freelist_bytes(&conn) > 0, "deletes populate the freelist");
        // max_pages == 0 => full drain in one call.
        let reclaimed = reclaim_freelist_step(&conn, 0).unwrap();
        let (file_after, live_after) = page_stats(&conn);
        assert!(reclaimed > 0, "should reclaim freed pages to the OS");
        // The whole freelist must be drained, not a single page: the file collapses
        // back toward the (now empty) live set rather than holding the high-water.
        assert!(
            file_after < file_peak / 4,
            "file should collapse after reclaim, not free one page (peak={file_peak}, after={file_after})"
        );
        let freelist_after = file_after.saturating_sub(live_after);
        assert!(
            freelist_after < file_peak / 4,
            "freelist should be drained after reclaim (leftover={freelist_after})"
        );
    }

    #[test]
    fn freelist_bytes_reports_reclaimable_space() {
        let conn = open_incremental();
        let blob = "x".repeat(4096);
        assert_eq!(freelist_bytes(&conn), 0, "fresh db has an empty freelist");
        for k in 0..100 {
            conn.execute("INSERT INTO t (k, v) VALUES (?1, ?2)", (k, &blob))
                .unwrap();
        }
        // Live pages are not on the freelist yet.
        assert_eq!(freelist_bytes(&conn), 0, "live pages are not reclaimable");
        conn.execute("DELETE FROM t", []).unwrap();
        // Deleted pages become reclaimable — the signal the background worker gates on.
        assert!(
            freelist_bytes(&conn) > 0,
            "deleted pages report as reclaimable freelist bytes"
        );
    }

    #[test]
    fn reclaim_step_is_bounded_then_drains_across_passes() {
        let conn = open_incremental();
        let blob = "x".repeat(4096);
        for k in 0..4000 {
            conn.execute("INSERT INTO t (k, v) VALUES (?1, ?2)", (k, &blob))
                .unwrap();
        }
        conn.execute("DELETE FROM t", []).unwrap();
        let (_, live) = page_stats(&conn);
        let freelist_start = freelist_bytes(&conn);
        assert!(freelist_start > 0, "deleted pages sit on the freelist");

        // One tiny bounded pass reclaims *some* space but not the whole freelist.
        let first = reclaim_freelist_step(&conn, 8).unwrap();
        assert!(
            first > 0,
            "a bounded pass should reclaim at least one chunk"
        );
        assert!(
            freelist_bytes(&conn) > 0,
            "a single small pass must not drain the whole freelist"
        );

        // Draining in bounded passes eventually empties the freelist and collapses
        // the file toward the live set — same end state as a full drain.
        for _ in 0..2000 {
            if freelist_bytes(&conn) == 0 {
                break;
            }
            reclaim_freelist_step(&conn, 64).unwrap();
        }
        let (file_after, _) = page_stats(&conn);
        assert_eq!(freelist_bytes(&conn), 0, "passes eventually drain freelist");
        assert!(
            file_after <= live + 64 * 1024,
            "file collapses to ~live after draining (live={live}, after={file_after})"
        );
    }

    // The `mode == 2` early-return is the entire guard against rewriting a
    // populated database on every reopen (#1331): once a store is INCREMENTAL,
    // `enable_incremental_auto_vacuum` must NOT run `VACUUM` again. Prove it by
    // calling the helper a second time *inside an open transaction* — SQLite
    // rejects `VACUUM` within a transaction, so the second call succeeds only
    // because it skips the VACUUM.
    #[test]
    fn enable_incremental_auto_vacuum_skips_vacuum_when_already_incremental() {
        let conn = open_incremental();
        // Sanity: the helper left the db in INCREMENTAL mode.
        let mode: i64 = conn
            .query_row("PRAGMA auto_vacuum", [], |r| r.get(0))
            .unwrap();
        assert_eq!(mode, 2, "precondition: already INCREMENTAL");

        // A direct VACUUM inside a transaction fails — the control proving the
        // transaction really does reject it.
        conn.execute_batch("BEGIN").unwrap();
        assert!(
            conn.execute_batch("VACUUM").is_err(),
            "control: VACUUM is rejected inside a transaction"
        );
        // The helper must succeed here, which is only possible because it skipped
        // the VACUUM (mode is already 2).
        enable_incremental_auto_vacuum(&conn)
            .expect("re-enabling on an already-incremental db must not VACUUM");
        conn.execute_batch("ROLLBACK").unwrap();
    }
}
