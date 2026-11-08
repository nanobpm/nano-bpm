# Runbook: snapshot blockers and off-heap orphans

How to diagnose and clear a node whose snapshots have stopped folding. This is
the operator half of the durability work in
[#1383](https://github.com/nanobpm/nano-bpm/issues/1383) (a single orphaned
spilled instance blocked every snapshot for weeks),
[#1384](https://github.com/nanobpm/nano-bpm/issues/1384) (durable incident on
quarantine), and [#1385](https://github.com/nanobpm/nano-bpm/issues/1385)
(snapshot-health observability). The admin endpoint below is
[#1386](https://github.com/nanobpm/nano-bpm/issues/1386).

## Background

Each partition periodically folds its event journal into a snapshot so the
on-disk log can be compacted. Folding needs every live instance's state —
including variables that were spilled off-heap (to a cold/spill store). If one
instance's off-heap payload is **unreadable or definitively absent**, the fold
fails closed rather than snapshot a corrupt state: snapshots stop, compaction
stops, and the journal grows without bound.

Two distinct conditions matter, and the tooling reports them separately:

- **Snapshot blocker** — an instance whose off-heap payload aborted the *most
  recent* snapshot fold. The fold is retried each tick; if the row becomes
  readable again the blocker clears on the next successful fold.
- **Quarantined off-heap orphan** — an instance whose spilled/cold variable row
  is *definitively absent* from a readable store. Its spilled payload is
  unrecoverable; the instance keeps only its surviving resident variables and a
  durable `VariablesLost` incident is raised (#1384). An orphan does not clear
  on its own — the data is gone.

## Step 1 — confirm the node is degraded

```bash
curl -sS http://localhost:8080/health | jq
```

A healthy node answers `200` with `"status": "healthy"`. A node whose snapshots
are blocked answers **`503`** with `"status": "degraded"`, a `causes` list, and
the `blockingKeys` whose off-heap payload is aborting the fold. Wire `/health`
to alerting/readiness, **not** liveness — a restart does not repair an
unreadable store or a full disk.

The same gauges are exported for Prometheus at `GET /metrics`
(`nanobpm_snapshot_health_degraded`, `nanobpm_journal_quarantined_orphans`,
`nanobpm_journal_uncompacted_bytes`, …).

## Step 2 — list the blockers and orphans

```bash
curl -sS http://localhost:8080/debug/snapshot-blockers | jq
```

Response shape (camelCase, pinned by test):

```json
{
  "snapshotBlockers": [11, 12, 13],
  "quarantinedOrphans": [101, 102],
  "partitions": [
    { "partition": 0, "snapshotBlockers": [11, 12], "quarantinedOrphans": [101] },
    { "partition": 1, "snapshotBlockers": [12, 13], "quarantinedOrphans": [101, 102] }
  ]
}
```

- `snapshotBlockers` / `quarantinedOrphans` — the sorted, deduplicated union
  across every owned partition.
- `partitions` — the per-partition breakdown, so you know *which* partition's
  journal is failing to fold.

## Step 3 — act

**If a blocker is transient** (the store was briefly unreadable): fix the
underlying store and wait for the next snapshot tick. The blocker clears on the
next successful fold and `/health` returns to `200`.

**If an instance is a quarantined orphan** (its spilled payload is gone): the
instance can never be made whole. Cancel it so the partition's snapshots can
fold again:

```bash
curl -sS -X POST http://localhost:8080/debug/snapshot-blockers/101/cancel | jq
```

The cancel routes through the normal engine command path — leader-forwarded,
durable-committed, and Raft-replicated like any other cancel — so it is itself
durable and consistent under failure. The response is operator-meaningful:

- `200` — the instance was cancelled (state `TERMINATED`).
- `404` — no *active* instance with that key (already terminated/completed, or
  never existed). Safe to treat as already-clear.
- `503` — a transient leader transition / handoff pause; **retry**.
- `500` — an unexpected engine/peer error; inspect the node logs.

Cancelling the orphan removes it from the live set, so the next snapshot fold
no longer needs its (lost) off-heap payload and compaction resumes. Confirm with
a re-read of `/debug/snapshot-blockers` (the key drops out) and `/health`
(returns to `200` once a fold succeeds).

## `c8ctl nano doctor`

The `c8ctl` CLI's `nano doctor` command consumes `GET /debug/snapshot-blockers`
and `POST /debug/snapshot-blockers/{key}/cancel` to present this same list and
offer the cancel action interactively. The HTTP contract above is the source of
truth the CLI is built against.
