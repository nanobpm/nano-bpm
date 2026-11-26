import Lake
open Lake DSL

/-!
# Refinement — Aeneas extraction + Lean proof of the join guard (#2010)

A separate Lake package (not a `lean_lib` of `formal/lean`): the Aeneas Lean
library pins its own Lean toolchain and Mathlib, which differ from the
`formal/lean` package's (`formal/lean/lean-toolchain`). Keeping this slice in
its own package lets each follow its own pin; see `README.md`.

* `Extracted/JoinGuard.lean` — **generated** by `extract.sh` from
  `engine-core/src/engine/join_guard.rs` (Charon → Aeneas). Never hand-edit; CI
  regenerates it and fails on any diff.
* `Zeebe.lean` — `ZeebeJoinReady` (parallel + inclusive) as a declarative
  reachability predicate, citing Zeebe at `formal/parity/zeebe-pin.json`.
* `Proof.lean` — the extracted `join_ready` terminates without error and
  equals `ZeebeJoinReady`, for all graphs.
-/

-- Pinned with the Aeneas binary release used by `extract.sh` (AENEAS_REV).
require aeneas from git
  "https://github.com/AeneasVerif/aeneas.git" @ "fb44cb400eb6bfe7dce97422ebe8fa748b7e4f9b" / "backends/lean"

package «nano-refinement» where
  leanOptions := #[
    ⟨`warningAsError, true⟩
  ]

@[default_target]
lean_lib «Refinement» where
  roots := #[`Extracted.JoinGuard, `Zeebe, `Proof]
