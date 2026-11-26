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
| `flow_arrivals_preserve_join_invariant` | `engine-core/src/state/kani_proofs.rs` | Any interleaving of join arrivals (`record`/`add`) and firings (`consume_one_each`) keeps every stored token count positive, entries strictly sorted (one per flow), conserves tokens, never under/overflows. Restates the `TokenFlow.tla` join invariant. Proven over `BoundedArrivals` (fixed-array stand-in, `kani_model.rs`). | 3 ops over 2 symbolic flows (the three-flow case is covered by the `kani_model.rs` refinement test, not by Kani), checked against a ghost count per flow, `add` ≤ 2 tokens; a firing (as in the engine) only once every flow holds a token |
| `job_index_resync_keeps_lockstep` | `engine-core/src/state/kani_proofs.rs` | The `resync_job_index` algorithm — the one index maintainer every job applier arm calls after a state change — keeps `activatable_jobs`, `deferred_jobs` / `deferred_job_deadlines` and `activated_jobs` in lockstep with the job across any two consecutive changes to any of the six job states, with or without a retry backoff (any deadline), at any priority. Proven over `JobIndexes` (bounded-set stand-in, `kani_model.rs`), which shares the membership decision `job_index_membership` with production. | 2 transitions from a `Created` job |
| `parallel_join_fires_only_when_every_flow_taken` | `engine-core/src/state/kani_proofs.rs` | The parallel-diamond join (`P2`, two incoming flows) under the guard `Engine::activate_join` applies: over any arrival order it fires exactly when both flows have been taken, once per full round (`fired == min(from_A, from_B)`), keeps the surplus, and is never left ready-but-waiting. Proven over `BoundedArrivals` (`kani_model.rs`); a `cover!` keeps the firing branch non-vacuous. | 3 arrivals |
| `join_ready_kernel_total_and_exact` | `engine-core/src/engine/join_guard/kani_proofs.rs` | The pure join guard kernel `join_guard::join_ready` (#2010) — the code the Lean refinement proof `formal/lean/Refinement/Proof.lean` is about — never panics or overflows on any graph (including out-of-range targets/live nodes), saturates the Nano-only `unidentified` count, accepts whenever every incoming flow is accounted for, never accepts with no arrival, and for a parallel join accepts exactly when all flows arrived. Two `cover!`s keep the inclusive-accept-with-pending and still-waiting branches non-vacuous. Full functional equality with Zeebe is the Lean proof, not this harness. | 2 nodes, one symbolic out-edge each, 1 live source, 2 incoming flows, unwind 3 (complete: BFS ≤ 2 rounds, ≤ 2 incoming; Kani's unwinding assertions hold). Cost: ~4.4 GiB peak RSS, ~2 min verification (`check.sh`, 4-core/16 GiB host). Unwind 4 exceeded 7 GiB and killed the hosted runner (#2040); 3 nodes / unwind 5 likewise. |
| `inclusive_join_ready_iff_no_active_path` | `engine-core/src/engine/join_guard/kani_proofs.rs` | #2024: an inclusive join with at least one but not every incoming flow taken fires (`join_ready`) **iff** no live token can still reach an untaken incoming flow (checked against a closed-form reachability oracle, so CBMC runs one search); with every flow taken it fires regardless of live tokens, with none it never fires. Two `cover!`s keep the held-by-live-path and live-token-that-cannot-reach branches non-vacuous. | 2 nodes (node 1 = join), one symbolic edge out of node 0, 1 live source, 2 incoming flows, unwind 3 (complete; same order of cost as `join_ready_kernel_total_and_exact`). Unwind 4 exceeded the hard memory cap on the hosted runner (#2040). |
| `event_gateway_trigger_first_wins` | `engine-core/src/engine/event_gateway_trigger.rs` | #2004: the pure event-based gateway trigger kernel `decide` is total and accepts only the first trigger of a real subscription target (an index below the gateway's target count); a gateway already triggered rejects every later one. Two `cover!`s keep the accept and reject branches non-vacuous. | scalar inputs, no loops (seconds) |

The earlier engine-level diamond harness (full deploy + create-instance through `Engine::apply_command`) did not prove inside the CI box — it needed unwind 24 over the whole command path — so it was replaced by the pure join transition above.

## Stand-in types (#1994)

The heap-backed production types (`Vec<(IncomingFlow, usize)>`, `BTreeMap` /
`BTreeSet`) do not fit a GitHub-hosted runner (OOM / formula build > 7 min), so
all three harnesses prove over the fixed-size, bounded-length stand-ins in
`engine-core/src/state/kani_model.rs`. The stand-ins are tied to production by
the `state::kani_model::refinement` unit tests, which run stand-in and
production through **every** operation sequence up to a bound (4 arrival ops
over 3 flows; 3 job transitions over 6 states × 3 backoffs × 4 priorities) and
assert identical observable state.

The three `state` harnesses declare `#[kani::unwind(4)]` (capacity-3 array loops + exit); `join_ready_kernel_total_and_exact` declares `#[kani::unwind(3)]` (its loops run at most twice); `--default-unwind 3` covers anything else. Never raise an unwind beyond the loops' real bound: each extra round multiplies the formula (#2040). Setting `KANI_UNWIND` passes `--unwind`, overriding every harness.

## Running

```bash
formal/kani/check.sh            # every harness; installs the pinned kani-verifier on first use
formal/kani/check.sh <harness>  # just that harness
```

`check.sh` is **Linux-only**: its hard memory cap (`memguard.sh`) reads
`/proc/meminfo` and needs util-linux `setsid` and `ps -g`. On other hosts it
exits 2 rather than run a harness uncapped; use a Linux VM/container (or the
nightly workflow) there.

### CI: nightly, non-gating

Kani is **not** a PR gate. `.github/workflows/kani-nightly.yml` runs every
harness nightly (and on `workflow_dispatch`, and on PRs that touch the harnesses
as a non-required preview) on a GitHub-hosted `ubuntu-24.04` runner, one matrix
leg per harness. It is never a required check.

Each harness is boxed by `check.sh`:

- **Wall time:** `KANI_HARNESS_TIMEOUT` seconds (default 420: a ~2 min cold
  build plus ~5 min of verification).
- **Memory (hard cap, #2040):** `KANI_MAX_RSS_PCT` (default 60) percent of
  host memory is enforced *while the harness runs*: `formal/kani/memguard.sh`
  first sets a kernel-backed `RLIMIT_DATA` of the cap on every process (so a
  burst allocation fails in the kernel, between samples) and, where a
  transient systemd scope can be created (user manager or passwordless
  `sudo`), an aggregate cgroup `MemoryMax` of cap + 25% with no swap, so
  concurrent sub-cap bursts cannot sum past it either; then it runs it in its
  own process group, samples the group's total RSS every second and kills it
  the moment it crosses the cap, failing the leg cleanly with a
  `memory` record (`kani <harness> killed at peak RSS …: over the hard cap`).
  An allocation the kernel limit refuses exits with the tool's own status, so
  `check.sh` also records `memory` when the output carries an allocator OOM
  diagnostic (CBMC `Out of memory`, `std::bad_alloc`, …; the one definition is
  `formal/kani/oom.sh`, exercised by the self-check).
  A post-exit check alone could never fire on a harness that exhausts memory,
  because the OOM killer takes the runner down first and nothing is reported.
  Peak RSS (exact via `/usr/bin/time -v` when installed, else the sample) is
  printed with the wall time (`kani <harness>: peak RSS <n> MiB, wall <s>s`),
  and a harness over the cap **fails even if it proved**.
  `formal/kani/memguard-selftest.sh` (run in every leg before the harness)
  proves both layers: a slow tree-wide overrun is killed by the sampler, and a
  single 2 GiB burst is refused by the kernel limit, and (when scoped) four
  concurrent 200 MiB bursts are stopped by the cgroup limit.

The per-run cost is in each leg's job summary. A harness that passes on one run
and is killed on the next is a defect in the harness bound, not a flaky test:
shrink its bound or replace it with a smaller transition rather than tuning
the solver.

The `report failures` job runs whenever any leg did not succeed — including
a leg cancelled or lost with its runner, which uploads no outcome record and
is reported as `timeout-or-lost`. On PR previews it writes the failures to the
job summary and fails; only scheduled/dispatched runs file issues.

A failing scheduled run files an issue titled `kani: <harness> <class>`, with a
`<!-- kani-signature:… -->` marker built from the failure class
(`timeout | verification | vacuous-cover | memory | error`), the harness and the
first failed check. If an open issue already has that signature, the run
comments on it instead of filing a duplicate. This is the same dedupe-by-signature
pattern the nightly fuzz workflows use.

Self-hosted runs with larger bounds are deferred until engine-core has more
provable code (#1983).

## Defects found

None so far. Any counterexample is filed as an issue and fixed red/green.

Further harnesses (bounded `kani::any()` graphs from the `formal/corpus`
vocabulary, `take_flow` / `has_active_path_to`) are tracked in the follow-up
checklist issue.
