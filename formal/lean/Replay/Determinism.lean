import Replay.Journal

/-!
# Replay determinism (issue #1231)

The core correctness property of the engine's crash-recovery path:

> **reconstructing engine state from a snapshot plus replaying the surviving
> tail equals a full replay of the whole event journal** —
> `snapshot ∘ replay(tail) = full replay`.

Its *violation* is the #1065 silent-rewind incident class: recovery that yields
a state other than a full replay silently loses (or duplicates) history. Proving
the equality here — for **every** compaction split point, and for the key
counter as well as the domain state — rules that class out structurally.

The proof is short because replay is a pure left fold: it reduces to
`foldl` distributing over `++` (`List.foldl_append`) composed with
`take k ++ drop k = evs` (`List.take_append_drop`).

**Scope.** `Engine` here is the replay-relevant projection `(state, nextLocal)`
(see `Replay.Journal`), so "equals a full replay" is equality *of that
projection*, not of a bitwise-identical Rust engine. The snapshot-carried scalars
`partition_id`, `num_partitions`, `now`, and `start_dispatch_rr` are restored
verbatim by `from_snapshot` while a full `replay_partition` re-defaults them, so
they are outside the fold and outside these theorems by construction.

**Rust anchor (issue #1279).** This proof is generic over an abstract applier
`apply : σ → Event → σ`, so on its own it is an *unanchored* spec — the drift
surface epic #1224's anti-drift rule forbids. It is tied to the real engine by
the differential guard `engine-core/tests/replay_determinism_differential.rs`,
which is the **concrete instance** of `recover_snapshotAt` (and, across the range
of split points, `recover_split_invariant`) on the real `Engine`:
`from_snapshot(snapshot(replay_partition(take k))) ⊕ apply_replayed_events(drop k)`
equals `replay_partition(evs)` on **exactly** the projection `(state, next_local)`
these theorems assert equality of — for every journal and every split `k`. The
snapshot is round-tripped through real serde JSON there, the faithful analogue of
the `deserialize_serialize` identity used below. That guard makes the abstract
`apply` non-abstract and fails CI the moment the real recovery path diverges from
the equality proved here; keep the two in lockstep.
-/

namespace Replay

variable {σ : Type}

/-- Replaying `a ++ b` is replaying `a`, then replaying `b` on the result —
`foldl` distributes over list append. This is the whole engine of the
determinism theorem. -/
theorem replayFrom_append (pid : Nat) (apply : σ → Event → σ)
    (e : Engine σ) (a b : List Event) :
    replayFrom pid apply e (a ++ b)
      = replayFrom pid apply (replayFrom pid apply e a) b := by
  simp only [replayFrom, List.foldl_append]

/-- **Replay determinism.** Snapshotting after the first `k` events and then
replaying the surviving tail (`evs.drop k`) reconstructs exactly the full replay
of the whole journal — for *any* split point `k` and *any* deterministic
applier. This is the literal `snapshot ∘ replay(tail) = full replay` property. -/
theorem recover_snapshotAt (pid : Nat) (apply : σ → Event → σ) (init : σ)
    (evs : List Event) (k : Nat) :
    recover pid apply (snapshotAt pid apply init evs k) (evs.drop k)
      = replay pid apply init evs := by
  unfold recover snapshotAt replay
  rw [deserialize_serialize, ← replayFrom_append, List.take_append_drop]

/-- The determinism property phrased as the issue's literal composition
`snapshot ∘ replay(tail) = full replay`: recovering from the snapshot taken at
`k` is a function of the tail that, applied to `evs.drop k`, equals the full
replay. -/
theorem snapshot_replay_tail_eq_full (pid : Nat) (apply : σ → Event → σ)
    (init : σ) (evs : List Event) (k : Nat) :
    (fun tail => recover pid apply (snapshotAt pid apply init evs k) tail)
        (evs.drop k)
      = replay pid apply init evs :=
  recover_snapshotAt pid apply init evs k

/-- Corollary (domain state): the recovered **state** equals the full-replay
state. This is the user-visible datum a silent rewind would corrupt. -/
theorem recover_state_eq (pid : Nat) (apply : σ → Event → σ) (init : σ)
    (evs : List Event) (k : Nat) :
    (recover pid apply (snapshotAt pid apply init evs k) (evs.drop k)).state
      = (replay pid apply init evs).state := by
  rw [recover_snapshotAt]

/-- Corollary (key counter — no counter rewind): the recovered `nextLocal`
equals the full-replay `nextLocal`. Because a mint-key counter that rewinds on
recovery re-issues live keys, this is exactly the second half of the #1065
guarantee. -/
theorem recover_nextLocal_eq (pid : Nat) (apply : σ → Event → σ) (init : σ)
    (evs : List Event) (k : Nat) :
    (recover pid apply (snapshotAt pid apply init evs k) (evs.drop k)).nextLocal
      = (replay pid apply init evs).nextLocal := by
  rw [recover_snapshotAt]

/-- **No silent rewind, for any compaction boundary.** Recovery is independent
of *where* the journal was compacted: snapshotting-and-recovering at split `j`
yields the same engine as at split `k`. A compaction that could shift the
recovered state — the #1065 failure mode — is impossible. -/
theorem recover_split_invariant (pid : Nat) (apply : σ → Event → σ) (init : σ)
    (evs : List Event) (j k : Nat) :
    recover pid apply (snapshotAt pid apply init evs j) (evs.drop j)
      = recover pid apply (snapshotAt pid apply init evs k) (evs.drop k) := by
  rw [recover_snapshotAt, recover_snapshotAt]

end Replay
