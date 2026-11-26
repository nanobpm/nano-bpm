module
public import Mathlib.Logic.Relation
@[expose] public section

/-!
# `ZeebeJoinReady` — Zeebe's join guard as a declarative predicate (#2010)

The reference side of the refinement proof in `Proof.lean`. This restates the
operators of `formal/tla/ZeebeTokenFlow.tla` (`ZeebePathReaches`,
`ZeebeBlocked`, `ZeebeHasActivePath`, `ZeebeParallelReady`,
`ZeebeInclusiveReady`, `ZeebeJoinReady`) over plain Lean data. Like the TLA+
module it is derived from Zeebe, not from Nano: Camunda `stable/8.10` at
`5a3c4a1ca7cd9a0c50be34227fe3b4d8bf731dcf` (`formal/parity/zeebe-pin.json`),

* `zeebe/engine/src/main/java/io/camunda/zeebe/engine/processing/bpmn/behavior/BpmnStateTransitionBehavior.java`
  — `takeSequenceFlow` records a taken flow on the join;
* `.../processing/bpmn/ProcessInstanceStateTransitionGuard.java`
  — `canActivateParallelGateway`, `canActivateInclusiveGateway`;
* `.../processing/bpmn/behavior/BpmnInclusiveGatewayBehavior.java`
  — `hasActivePathToTheGateway`, the graph search.

## Graph encoding

Nodes are natural numbers. `out g u` lists the edges out of `u` as
`(target, flow)`. A *sequence flow* is an edge whose `flow` id is below
`nFlows` (the size of the per-flow `taken` table); every other edge (in Nano's
encoding a boundary attachment, or a link throw → catch hand-off) is never a
sequence flow, so it is never blocked. `taken f` is the number of
`SEQUENCE_FLOW_TAKEN` records on flow `f` not yet consumed by the join.

The TLA+ `ZeebePathReaches` unfolds the successor relation `Cardinality(Nodes)`
times; over a finite node set that is exactly reflexive-transitive
reachability, which is how it is stated here (for graphs of any size).
-/

namespace Zeebe

/-- A dense process graph: `out g u` are the `(target, flow)` edges out of `u`. -/
abbrev Graph := List (List (Nat × Nat))

/-- The edges out of `u` (none for a node outside the graph). -/
def out (g : Graph) (u : Nat) : List (Nat × Nat) := g.getD u []

/-- `In(j)`: the sequence flows into `j`. -/
def In (g : Graph) (nFlows j f : Nat) : Prop :=
  f < nFlows ∧ ∃ u, (j, f) ∈ out g u

/-- `ZeebeBlocked(j, taken)`: the already-taken incoming flows of `j`, which the
search skips (each incoming flow needs to be taken only once). -/
def Blocked (g : Graph) (nFlows : Nat) (taken : Nat → Nat) (j f : Nat) : Prop :=
  In g nFlows j f ∧ 0 < taken f

/-- `Succ(n, blocked)`: one step over an edge whose flow is not blocked. -/
def Step (g : Graph) (nFlows : Nat) (taken : Nat → Nat) (j u v : Nat) : Prop :=
  ∃ f, (v, f) ∈ out g u ∧ ¬ Blocked g nFlows taken j f

/-- `ZeebePathReaches(src, j, ZeebeBlocked(j, taken))`. -/
def PathReaches (g : Graph) (nFlows : Nat) (taken : Nat → Nat) (src j : Nat) : Prop :=
  Relation.ReflTransGen (Step g nFlows taken j) src j

/-- `ZeebeHasActivePath(j, taken, liveSources)`: some live source other than
`j` itself (whose own arrival is already counted in `taken`) reaches `j`. -/
def HasActivePath (g : Graph) (nFlows : Nat) (taken : Nat → Nat) (j : Nat)
    (live : List Nat) : Prop :=
  ∃ m ∈ live, m ≠ j ∧ PathReaches g nFlows taken m j

/-- `ZeebeParallelReady(j, taken)` (`canActivateParallelGateway`): every
incoming flow has a taken record. -/
def ParallelReady (g : Graph) (nFlows : Nat) (taken : Nat → Nat) (j : Nat) : Prop :=
  ∀ f, In g nFlows j f → 1 ≤ taken f

/-- `ZeebeInclusiveReady(j, taken, liveSources)` (`canActivateInclusiveGateway`):
every incoming flow taken, or some taken and no live path left to `j`. -/
def InclusiveReady (g : Graph) (nFlows : Nat) (taken : Nat → Nat) (j : Nat)
    (live : List Nat) : Prop :=
  ParallelReady g nFlows taken j ∨
    ((∃ f, In g nFlows j f ∧ 0 < taken f) ∧ ¬ HasActivePath g nFlows taken j live)

/-- The join's kind: the TLA+ module's `ParJoins` / inclusive joins. -/
inductive Kind where
  | parallel
  | inclusive

/-- `ZeebeJoinReady(j, taken, liveSources)`, dispatched on the join's kind. -/
def JoinReady (g : Graph) (nFlows : Nat) (taken : Nat → Nat) (kind : Kind) (j : Nat)
    (live : List Nat) : Prop :=
  match kind with
  | .parallel => ParallelReady g nFlows taken j
  | .inclusive => InclusiveReady g nFlows taken j live

/-- Well-formedness of the encoding: a sequence flow has one target, as a BPMN
`sequenceFlow` does (`Tgt` is a function in the TLA+ module). -/
def FlowsHaveOneTarget (g : Graph) (nFlows : Nat) : Prop :=
  ∀ u v t t' f, f < nFlows → (t, f) ∈ out g u → (t', f) ∈ out g v → t = t'

end Zeebe
