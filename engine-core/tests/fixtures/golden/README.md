# Golden serde corpus — snapshot & event on-disk shape

This directory holds the **checked-in golden serialized fixtures** for the
engine's persisted shapes. They serve two jobs at once:

1. **CI serde-drift guard (issue #1069 / L3 of the snapshot-durability epic
   #1067).** The test [`engine-core/tests/golden_serde_drift.rs`] serializes a
   deterministic, representative corpus and asserts it is **byte-for-byte** equal
   to the files here. Any change to the serialized shape of
   [`EngineSnapshot`]/[`State`] (`engine-core/src/engine/mod.rs`) or [`Event`]
   (`engine-core/src/event.rs`) changes those bytes and fails CI — forcing an
   explicit human decision instead of shipping an incompatible on-disk format
   that only explodes at a production restart (incident #1065). This is the
   engine-core analogue of the read-model's `schema_edit_requires_version_bump`
   (`read-model/src/store.rs`).

2. **Cross-version replay corpus** consumed by **#1070** (L4 golden replay tests)
   and **#1071** (L5 replay-migrator). Those tasks **reuse** the fixtures here —
   they do not create their own. If a later task needs an additional cross-version
   journal sample, **add it into this directory using the naming scheme below**
   (and post a `file-claim`); never stand up a parallel corpus.

## Files & naming scheme

    engine_snapshot.v<N>.json   # a serialized EngineSnapshot (the snapshot graph)
    event_corpus.v<N>.json      # a serialized Vec<Event> (an ordered journal)
    event_witnesses.v<N>.json   # serialized Vec<Event> shape witnesses (NOT a journal)

`<N>` is the [`SNAPSHOT_FORMAT_VERSION`] (from L2 #1068,
`engine-core/src/engine/mod.rs`) the fixture was produced at. The guard test
derives the filename from the current `SNAPSHOT_FORMAT_VERSION`, so **bumping the
version points the guard at a fresh `v<N>` file** you must regenerate, while the
prior `v<N-1>` files stay checked in as the historical corpus #1071's migrator
replays. The live `<N>` is always whatever [`SNAPSHOT_FORMAT_VERSION`] resolves
to at build time — the guard reads it from the constant, so this doc deliberately
does not restate a specific number here (which would silently drift on the next
bump); check the constant in `engine-core/src/engine/mod.rs` for the current value.

The JSON is emitted in a **canonical** form (object keys sorted, pretty-printed)
so diffs are reviewable and stable across machines. Do not hand-edit it.

## Regenerating (the documented helper)

When you deliberately change the shape in a **compatible** way (e.g. add a field
carrying `#[serde(default)]`), or you have bumped `SNAPSHOT_FORMAT_VERSION` and
need the new `v<N>` corpus, regenerate deterministically:

```sh
UPDATE_GOLDEN=1 cargo test --features serde --test golden_serde_drift
```

then review the diff and commit. The regeneration routine lives in
`build_golden_corpus()` in the test file.

## CRITICAL: `--features serde`

The `Event`/`EngineSnapshot` serde derives are feature-gated
(`cfg_attr(feature = "serde", …)`). The guard test **and** its CI job must run
with `--features serde` — without it the types don't derive `Serialize`, the test
compiles to nothing, and the guard silently passes. The `engine-core (clippy +
test)` CI job is wired to pass `--features serde`; downstream tasks reuse it.

## Shape witnesses (`event_witnesses.v<N>.json`)

Some event frames / fields are part of the persisted shape before any engine
path emits them (e.g. the #1374 modify / migrate surface — `ProcessInstanceModified`
and the populated `ProcessInstanceMigrated` flow-scope / subscription /
child-instance fields — landed as a scaffold ahead of their behaviour). They
cannot appear in `event_corpus` (a real, replayable journal), so
`build_event_witnesses` in `golden_serde_drift.rs` pins them here with every
field non-default. This file is **not** replayed; it is refreshed by the same
`UPDATE_GOLDEN=1` command.
