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
| `flow_arrivals_preserve_join_invariant` | `engine-core/src/state/kani_proofs.rs` | Any interleaving of join arrivals (`record`/`add`) and firings (`consume_one_each`) keeps every stored token count positive, entries strictly sorted (one per flow), conserves tokens, never under/overflows. Restates the `TokenFlow.tla` join invariant. | 3 ops over 3 flow identities, `add` ≤ 4 tokens |
| `job_lease_lifecycle_keeps_indexes_in_lockstep` | `engine-core/src/state/kani_proofs.rs` | Any sequence of job events (activate / lock-expire / complete / fail / retries-update) with arbitrary deadlines, timestamps, retries and priority, applied through the real `state::apply`, never panics, and the derived indexes (`activated_jobs`, `activatable_jobs`, `deferred_job_deadlines`) stay in lockstep with the job state. | 3 events after creation |
| `parallel_diamond_creates_one_instance` | `engine-core/src/engine/kani_proofs.rs` | Deploy + create-instance of a parallel fork/join diamond runs `activate`/`complete`/`finalize_completion` through the fork without panic. | fixed graph |

Harnesses declare their own `#[kani::unwind(N)]` (4 for the state harnesses, 24 for the engine diamond, whose bound must cover every loop in the deploy + create command execution — `ProcessBuilder::build` over 6 elements + 6 flows and the engine's passes over the emitted event list); `--default-unwind 4` covers anything else. Setting `KANI_UNWIND` passes `--unwind`, overriding every harness.

## Running

```bash
formal/kani/check.sh   # installs the pinned kani-verifier on first use
```

CI runs it in the `formal (tlc)` job (change-gated on `engine-core/src/**`),
time-boxed to 20 minutes. If a harness outgrows the box, shrink its bound or
replace it with a smaller transition rather than optimising the solver.

## Defects found

None so far. Any counterexample is filed as an issue and fixed red/green.

Further harnesses (bounded `kani::any()` graphs from the `formal/corpus`
vocabulary, `take_flow` / `has_active_path_to`) are tracked in the follow-up
checklist issue.
