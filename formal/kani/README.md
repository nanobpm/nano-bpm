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
| `flow_arrivals_preserve_join_invariant` | `engine-core/src/state/kani_proofs.rs` | Any interleaving of join arrivals (`record`/`add`) and firings (`consume_one_each`) keeps every stored token count positive, entries strictly sorted (one per flow), conserves tokens, never under/overflows. Restates the `TokenFlow.tla` join invariant. | 2 ops over 2 flow identities (same source, distinct ordinals), `add` ≤ 2 tokens |
| `job_lease_lifecycle_keeps_indexes_in_lockstep` | `engine-core/src/state/kani_proofs.rs` | Any job event (activate / lock-expire / complete / fail / retries-update) with arbitrary deadlines, timestamps, retries and priority, applied through the real `state::apply`, never panics, and the derived indexes (`activated_jobs`, `activatable_jobs`, `deferred_job_deadlines`) stay in lockstep with the job state. | one symbolic event from `Created` or (concretely) `Activated` |
| `parallel_join_fires_only_when_every_flow_taken` | `engine-core/src/state/kani_proofs.rs` | The parallel-diamond join (`P2`, two incoming flows) under the guard `Engine::activate_join` applies: over any arrival order it fires exactly when both flows have been taken, once per full round (`fired == min(from_A, from_B)`), keeps the surplus, and is never left ready-but-waiting. | 2 arrivals |

The earlier engine-level diamond harness (full deploy + create-instance through `Engine::apply_command`) did not prove inside the CI box — it needed unwind 24 over the whole command path — so it was replaced by the pure join transition above.

Harnesses declare their own `#[kani::unwind(3)]` (2 steps + loop exit; every collection holds ≤ 2 entries); `--default-unwind 3` covers anything else. Setting `KANI_UNWIND` passes `--unwind`, overriding every harness.

## Running

```bash
formal/kani/check.sh            # every harness; installs the pinned kani-verifier on first use
formal/kani/check.sh <harness>  # just that harness
```

CI runs it in its own `kani (engine-core proofs)` job (same change gate as
`formal (tlc)`, job timeout 25 minutes), one matrix leg per harness, each boxed to
`KANI_HARNESS_TIMEOUT` seconds (default 300) and reporting its Kani
`VERIFICATION:` line and wall time. If a harness outgrows the box, shrink its bound or
replace it with a smaller transition rather than optimising the solver.

## Defects found

None so far. Any counterexample is filed as an issue and fixed red/green.

Further harnesses (bounded `kani::any()` graphs from the `formal/corpus`
vocabulary, `take_flow` / `has_active_path_to`) are tracked in the follow-up
checklist issue.
