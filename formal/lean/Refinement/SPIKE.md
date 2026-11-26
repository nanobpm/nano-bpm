# Refinement slice — Aeneas spike report (#1961)

Goal of #1961: extract Nano's Rust join guard with
[Aeneas](https://github.com/AeneasVerif/aeneas) (Charon → LLBC → pure Lean 4)
and prove, for all graphs, that it equals Zeebe's `ZeebeJoinReady`
(`formal/tla/ZeebeTokenFlow.tla`, Zeebe pinned in `formal/parity/zeebe-pin.json`).

The issue's fallback clause applies: **Aeneas cannot take the current code.**
This document records which constructs block it and the smallest refactor that
unblocks the extraction. The refactor, the extraction, the Lean proof and the
CI anti-drift gate are tracked in #2010.

## The code under proof

| Rust (`engine-core/src/engine/mod.rs`) | Zeebe reference (TLA) |
| --- | --- |
| `Engine::has_active_path_to` (method) | `ZeebeHasActivePath(j, taken, liveSources)` |
| free fn `path_reaches_join` (BFS) | the BFS of `BpmnInclusiveGatewayBehavior.hasActivePathToTheGateway` |
| join readiness in `Engine::activate_join` (`all_taken`, dispatch on join kind; `take_flow` only records the arrival) | `ZeebeParallelReady(j, taken)` |

## Blockers, construct by construct

1. **Receiver is the whole `Engine`.** `has_active_path_to(&self, …)` reads
   `self.state.instances`, `scope_of` and `process_of_instance` (the
   timer-batch `doomed_by_due_boundary_timers` view it used to call was
   removed by #2024). `Engine` (`engine-core/src/engine/mod.rs`)
   holds in-memory state and scratch data (no journal, no trait objects), but
   the `&self` receiver still drags the full transitive type closure of
   `State` (instances, timers, deployed definitions, FEEL values) into Charon.
   This receiver-closure problem is the dominant blocker; the specific
   collection types inside that closure are blocker 2.
2. **std collections without an Aeneas model.** `BTreeSet<&str>`,
   `VecDeque<&str>`, `BTreeMap<ElementId, Element>` (`process.elements`,
   `ProcessDefinition::element`) and `BTreeMap<Key, Vec<&str>>`. Aeneas' Lean
   standard library models `Vec`/slices/`Option`/integers, not the B-tree or
   ring-buffer collections.
3. **Iterator adaptors with closures (untested combination, not a verified
   blocker).** `filter`, `filter_map`, `flat_map`, `chain`, `any`,
   `enumerate`, `extend(… .filter_map(|other| …))`, `Option::is_some_and`,
   `bool::then_some`. Upstream Aeneas (`69280e07`) does model `Iterator`,
   `enumerate` and `Option::is_some_and` with a closure; whether this exact
   chain of adaptors extracts cleanly has not been tried against a pinned
   revision. Rewriting them as explicit indexed `while` loops is a *proposed
   simplification* (smaller, more predictable Lean output), not a requirement.
4. **Owned-string operations and std-heavy lookups.** Borrowed `&str` is not
   opaque upstream (`Str` is `Slice U8` with decidable equality), so string
   identity alone does not force a change. What remains unverified is the
   owned-`String` work in `incoming_flow` (`to_string`, `clone`) and
   string-keyed `BTreeMap` lookups (blocker 2). Dense `usize` node/flow
   indices are a *proposed simplification* that removes those operations and
   makes the BFS proof a plain array-reachability argument.
5. **Let-else / slice range patterns** (`let Some(..) = .. else`,
   `outgoing[..index]`) are supported by recent Charon but add noise; they
   disappear with the refactor below anyway.

Not a blocker: everything above is safe Rust (no `unsafe`, no interior
mutability in the guard), so Verus is not needed — the issue's fallback to
Verus is not triggered.

## Smallest refactor (proposed)

Split the guard into an *impure collector* and a *pure decision*, the latter
in a dependency-free module (`engine-core/src/engine/join_guard.rs`) that is
the only thing Charon is pointed at:

```rust
/// Dense, borrowed graph view built once per deployment (or per call).
pub struct JoinGraph<'a> {
    /// succ[n] = (target node, flow index) for every outgoing flow of n,
    /// plus link-throw → same-scope link-catch edges and
    /// host → attached boundary edges (flow index = NO_FLOW).
    pub succ: &'a [Vec<(usize, usize)>],
}

/// `path_reaches_join` over indices: BFS with an explicit Vec worklist and a
/// Vec<bool> visited set; `taken[f]` is the join's taken-flow count.
pub fn path_reaches_join(g: &JoinGraph, source: usize, join: usize, taken: &[u32]) -> bool;

/// `ZeebeHasActivePath`: any live source reaches the join.
pub fn has_active_path(g: &JoinGraph, live: &[usize], join: usize, taken: &[u32]) -> bool;

/// `ZeebeJoinReady`, parallel and inclusive.
/// `unidentified` counts arrivals with no known incoming flow (`via` is
/// `None` in `activate_join`), which today count towards `all_taken`.
pub fn join_ready(g: &JoinGraph, kind: JoinKind, join: usize,
                  incoming: &[usize], live: &[usize], taken: &[u32],
                  unidentified: u32) -> bool;
```

`Engine::has_active_path_to` keeps computing the live-source set (scope
filtering, #1989 doomed-boundary substitution, in-transit `Step::Activate`s)
and the index mapping, then delegates. A `proptest` equivalence test pins the
old string-based BFS to the new index-based one during the transition, so the
refactor is behaviour-preserving by test before it is by proof.

## Then (follow-up slice)

1. `charon` + `aeneas -backend lean` on `join_guard.rs` →
   `formal/lean/Refinement/Extracted/JoinGuard.lean` (committed).
2. `formal/lean/Refinement/Zeebe.lean`: `ZeebeJoinReady` (parallel +
   inclusive) as a declarative reachability predicate, citing the pinned Zeebe
   classes (`ProcessInstanceStateTransitionGuard.canActivateParallelGateway`,
   `BpmnInclusiveGatewayBehavior.hasActivePathToTheGateway`) at the sha in
   `formal/parity/zeebe-pin.json`.
3. `formal/lean/Refinement/Proof.lean`: `∀ g live join taken unidentified,
   join_ready … = ok b ↔ (b = true ↔ ZeebeJoinReady …)` — BFS soundness and
   completeness against reflexive-transitive reachability that avoids the
   taken flows into the join; no bound on graph size. The theorem is stated
   under the applicability hypothesis `unidentified = 0`: an unidentified
   arrival (`via = None`) has no per-flow counterpart in Zeebe's state, and
   for arbitrary `unidentified` the equivalence is false (two incoming flows,
   `taken = [0, 0]`, `unidentified = 2`: Nano's `activate_join` accepts,
   `ZeebeParallelReady` in `formal/tla/ZeebeTokenFlow.tla` rejects). The
   proof must therefore also discharge (or the slice must state as an
   explicit applicability invariant) that every join arrival on a modelled
   path is identified; the `unidentified > 0` case is out of the parity
   claim unless a justified mapping into Zeebe's per-flow state is added.
4. Lakefile: a `Refinement` `lean_lib` target (with the Aeneas Lean library
   as a Lake dependency — its pinned Lean toolchain must be reconciled with
   `formal/lean/lean-toolchain`, a known integration risk).
5. CI anti-drift (as #1279): re-run Charon/Aeneas and `git diff --exit-code`
   the extracted Lean, so any Rust change to the guard re-runs the proof.

## Environment note

The Aeneas toolchain (OCaml/opam or Nix, plus a pinned nightly Rust for
Charon) is not available on the agent workers or in CI today; the follow-up
must add it (CI job pinned by Aeneas/Charon commit, cached).
