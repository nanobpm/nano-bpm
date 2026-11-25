# Kani: code-level proofs over `engine-core` (#1960)

TLA+ (`formal/tla`) proves properties of a *model*; trace validation and fuzzing
link it to `engine-core` by sampling. [Kani](https://github.com/model-checking/kani)
is a bounded model checker over the Rust code itself: within the bound, every
input is covered, and any panic, arithmetic overflow or out-of-bounds access is a
counterexample (the code-level complement of the unwrap/expect ratchet, #1357).

Harnesses are compiled only under `cfg(kani)` (declared in
`engine-core/Cargo.toml`'s `check-cfg`), so normal builds are unaffected.

## What is proven

| Harness | File | Property | Bound |
|---|---|---|---|
| `flow_arrivals_preserve_join_invariant` | `engine-core/src/state/kani_proofs.rs` | Any interleaving of join arrivals (`record`/`add`) and firings (`consume_one_each`) keeps every stored token count positive, entries strictly sorted (one per flow), conserves tokens, never under/overflows. Restates the `TokenFlow.tla` join invariant. Proven over `BoundedArrivals` (fixed-array stand-in, `kani_model.rs`). | 2 ops over 3 symbolic flows, checked against a ghost count per flow, `add` ≤ 2 tokens; a firing (as in the engine) only once every flow holds a token |
| `job_index_resync_keeps_lockstep` | `engine-core/src/state/kani_proofs.rs` | The `resync_job_index` algorithm — the one index maintainer every job applier arm calls after a state change — keeps `activatable_jobs`, `deferred_jobs` / `deferred_job_deadlines` and `activated_jobs` in lockstep with the job across any two consecutive changes to any of the six job states, with or without a retry backoff (any deadline), at any priority. Proven over `JobIndexes` (bounded-set stand-in, `kani_model.rs`), which shares the membership decision `job_index_membership` with production. | 2 transitions from a `Created` job |
| `parallel_join_fires_only_when_every_flow_taken` | `engine-core/src/state/kani_proofs.rs` | The parallel-diamond join (`P2`, two incoming flows) under the guard `Engine::activate_join` applies: over any arrival order it fires exactly when both flows have been taken, once per full round (`fired == min(from_A, from_B)`), keeps the surplus, and is never left ready-but-waiting. | 2 arrivals |

The earlier engine-level diamond harness (full deploy + create-instance through `Engine::apply_command`) did not prove inside the CI box — it needed unwind 24 over the whole command path — so it was replaced by the pure join transition above.

## Stand-in types (#1994)

The heap-backed production types (`Vec<(IncomingFlow, usize)>`, `BTreeMap` /
`BTreeSet`) do not fit a GitHub-hosted runner (OOM / formula build > 7 min), so
the first two harnesses prove over the fixed-size, bounded-length stand-ins in
`engine-core/src/state/kani_model.rs`. The stand-ins are tied to production by
the `state::kani_model::refinement` unit tests, which run stand-in and
production through **every** operation sequence up to a bound (4 arrival ops
over 3 flows; 3 job transitions over 6 states × 3 backoffs × 4 priorities) and
assert identical observable state.

The parallel-join harness declares `#[kani::unwind(3)]` (2 steps + loop exit); the stand-in harnesses declare `#[kani::unwind(4)]` (capacity-3 array loops + exit); `--default-unwind 3` covers anything else. Setting `KANI_UNWIND` passes `--unwind`, overriding every harness.

## Running

```bash
formal/kani/check.sh            # every harness; installs the pinned kani-verifier on first use
formal/kani/check.sh <harness>  # just that harness
```

CI runs it in its own `kani (engine-core proofs)` job (same change gate as
`formal (tlc)`, job timeout 25 minutes), one matrix leg per harness, each boxed to
`KANI_HARNESS_TIMEOUT` seconds (default 420: a ~2 min cold build plus ~5 min of verification) and reporting its Kani
`VERIFICATION:` line and wall time. If a harness outgrows the box, shrink its bound or
replace it with a smaller transition rather than optimising the solver.

## Defects found

None so far. Any counterexample is filed as an issue and fixed red/green.

Further harnesses (bounded `kani::any()` graphs from the `formal/corpus`
vocabulary, `take_flow` / `has_active_path_to`) are tracked in the follow-up
checklist issue.
