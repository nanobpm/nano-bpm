# Determinism corpus (ADR 0066 slice 6)

`corpus.rs` builds the command corpus, `runner.rs` replays it (and is copied
into a previous-release checkout by `scripts/determinism-ci.sh prev-release`),
`compare.rs` compares record files.

## Previous-release gate (#1698)

The previous release replays the same corpus as HEAD. Nothing is dropped or
ignored silently:

1. **Explained drops.** Every entry the previous release cannot decode — an
   unknown command variant, an unknown model shape, or a *lossy* decode (a
   non-default field its `Command` type ignores) — must be explained by the
   additive set `vocabulary(golden @ HEAD) − vocabulary(golden @ prev tag)`
   over `tests/fixtures/golden/` (`command_shapes` / `event_shapes` pin the
   corpus's own vocabulary). An unexplained drop fails, and the kept share
   must clear `corpus::MIN_KEPT_PERCENT`. A lossy field that holds HEAD's
   default is kept (ignoring it is the field's own back-compatible meaning).
2. **Erased additive fields.** Golden-added fields the previous release never
   emits are erased from both sides before comparison.
3. **`divergence-allowlist.json`** ratifies behavioural changes:
   * `observational_variants` — new event variants filtered from the HEAD
     stream; any other golden-added variant HEAD emits fails.
   * `divergences` — per corpus scenario (`Gen::scenario`), the declared
     `expected` normalizations (`reorder`, `remint_keys`, `only_in_prev` /
     `only_in_head` event patterns). An entry still differing after them is
     an *undeclared extra divergence* and fails.

   Every entry names its `issue`, the `introduced_in` release, an `upgrade`
   mode and a `rationale` that names that mode and says why it applies.
   Raft members **re-execute** committed commands (ADR 0066, "Rolling
   upgrade", option C), so an allowlisted divergence makes mixed-version
   members disagree unless gated. `upgrade` is required and is exactly one of:
   * `feature-gated` — the new behaviour runs only once every voter AND
     learner advertises the capability (#1661 cluster-join feature
     advertisement); the old behaviour runs until then.
   * `upgrade-boundary` — the release requires an all-nodes-same-version
     (quiesced) upgrade; no rolling upgrade across it. Every such entry must
     be called out in the release notes.

   An entry **expires** (must be removed) once the
   previous release is at or past `introduced_in`, and a **stale** entry —
   one the run never needs — fails too, so the file cannot drift.

Self-tests: `tests/determinism_corpus.rs` (`prev_release_gate_*`).
