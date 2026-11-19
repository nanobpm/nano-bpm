----------------------------- MODULE TokenFlow -----------------------------
(***************************************************************************)
(* Token flow of a single nanobpm process instance in a single scope.      *)
(*                                                                         *)
(* This models the engine-core drain loop (`engine/mod.rs`):               *)
(*                                                                         *)
(*   - A command enqueues `Step::Activate` work; the engine drains the     *)
(*     queue to empty before the command settles. `pending` is that queue, *)
(*     held as a bag of the sequence flows being taken: the engine drains  *)
(*     FIFO, but the model lets any pending step go next. That covers every*)
(*     FIFO order, so a safety property proved here also holds for FIFO.   *)
(*   - Tasks are wait states: activation parks a token in `waiting` until  *)
(*     an external `CompleteTask` command (a job completion).              *)
(*   - A parallel gateway with >1 incoming flow is a join                  *)
(*     (`activate_join`). As in Zeebe, a flow into a join is counted when  *)
(*     it is taken (`ParallelJoinTokenArrived`, `join_flow_arrivals`);     *)
(*     the join opens when an arrival cannot fire it                       *)
(*     (`ParallelJoinOpened`). It fires once every incoming flow holds a   *)
(*     token, consuming one token per flow and keeping the surplus         *)
(*     (`ParallelJoinFired`, Zeebe's "Tetris" principle). A surplus        *)
(*     reopens the join, so it stays live (#1233).                         *)
(*   - An inclusive gateway with >1 incoming flow is a join too, with the  *)
(*     same bookkeeping. As in Zeebe, readiness is decided only when a     *)
(*     token arrives (#1241): the join fires if every incoming flow holds  *)
(*     a token, or if some flow holds one and no live source (a waiting    *)
(*     task, another open join, or a queued activation) can reach it       *)
(*     without crossing a flow that already holds a token                  *)
(*     (`has_active_path_to`). It is never re-evaluated otherwise, so a    *)
(*     join whose remaining path diverged elsewhere, or a surplus with no  *)
(*     partner, waits forever, as in Zeebe.                                *)
(*   - Conditions are abstracted: an exclusive gateway takes some single   *)
(*     outgoing flow, an inclusive split or join takes some non-empty      *)
(*     subset (condition/incident paths are out of scope for this slice). *)
(*   - An instance completes once it settles with no live tokens           *)
(*     (`complete_finished_instances`).                                    *)
(*                                                                         *)
(* `fireCount` and `premature` are ghost variables: the engine  *)
(* keeps none of them, and they exist only to state properties.            *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS
    Nodes,        \* element ids
    Kind,         \* [Nodes -> ElementKinds]
    Flows,        \* sequence-flow ids (distinct ids may share endpoints, as in the engine)
    Src,          \* [Flows -> Nodes] source element of each flow
    Tgt,          \* [Flows -> Nodes] target element of each flow
    Start         \* the (single) none start event

ElementKinds == {"start", "end", "task", "xor", "and", "or", "evt", "msg"}

ASSUME Kind \in [Nodes -> ElementKinds]
ASSUME Src \in [Flows -> Nodes] /\ Tgt \in [Flows -> Nodes]
ASSUME Start \in Nodes /\ Kind[Start] = "start"

\* The pseudo-flow that carries the instance-creation activation of Start.
InitFlow == "<create>"
ASSUME InitFlow \notin Flows
AllFlows == Flows \cup {InitFlow}

\* The element a queued activation targets.
Target(f) == IF f = InitFlow THEN Start ELSE Tgt[f]

In(n)  == {f \in Flows : Tgt[f] = n}
Out(n) == {f \in Flows : Src[f] = n}

IsJoin(n) == Kind[n] \in {"and", "or"} /\ Cardinality(In(n)) > 1
ParJoins  == {n \in Nodes : Kind[n] = "and" /\ IsJoin(n)}
IncJoins  == {n \in Nodes : Kind[n] = "or" /\ IsJoin(n)}
Tasks     == {n \in Nodes : Kind[n] = "task"}
\* Event-based gateways (#1246): a wait state like a task, but the external
\* command that resumes it (a message correlation) picks ONE outgoing flow —
\* the one leading to the intermediate catch event (`msg`) whose subscription
\* was triggered. The other subscriptions are closed with the gateway. A `msg`
\* catch event, reached only from such a gateway, is then a pass-through.
EvtGateways == {n \in Nodes : Kind[n] = "evt"}
WaitStates  == Tasks \cup EvtGateways

NonEmptySubsets(S) == (SUBSET S) \ {{}}

\* Every element from which `t` is reachable over directed sequence flows
\* (a fixpoint bounded by |Nodes| rounds). Only used to derive `Acyclic`.
ReachingFrom(t) ==
    LET R[k \in 0..Cardinality(Nodes)] ==
            IF k = 0 THEN {Src[f] : f \in In(t)}
            ELSE LET prev == R[k-1]
                 IN  prev \cup {Src[f] : f \in {g \in Flows : Tgt[g] \in prev}}
    IN  R[Cardinality(Nodes)]

\* A constant-level map, so TLC evaluates it once instead of in every guard.
ReachingMap == [t \in Nodes |-> ReachingFrom(t)]
Reaching(t) == ReachingMap[t]

VARIABLES
    pending,      \* [AllFlows -> Nat]  queued Step::Activate, keyed by flow taken
    waiting,      \* [Nodes -> Nat]     tokens parked on wait-state tasks
    joinOpen,     \* [Nodes -> BOOLEAN] join_instances has an entry
    joinTokens,   \* [Nodes -> [Flows -> Nat]] tokens per incoming flow: `join_flow_arrivals`
    fireCount,    \* ghost: [Nodes -> Nat] how often each join fired
    premature,    \* ghost: parallel joins that fired before every incoming flow delivered
    completed     \* ProcessInstanceCompleted emitted

vars == <<pending, waiting, joinOpen, joinTokens, fireCount, premature, completed>>

Add(p, S)    == [g \in DOMAIN p |-> IF g \in S THEN p[g] + 1 ELSE p[g]]
Take(p, f)   == [p EXCEPT ![f] = @ - 1]
NoArrivals   == [g \in Flows |-> 0]

TypeOK ==
    /\ pending   \in [AllFlows -> Nat]
    /\ waiting   \in [Nodes -> Nat]
    /\ joinOpen  \in [Nodes -> BOOLEAN]
    /\ joinTokens \in [Nodes -> [Flows -> Nat]]
    /\ fireCount \in [Nodes -> Nat]
    /\ premature \subseteq ParJoins
    /\ completed \in BOOLEAN

Init ==
    /\ pending   = [g \in AllFlows |-> IF g = InitFlow THEN 1 ELSE 0]
    /\ waiting   = [n \in Nodes |-> 0]
    /\ joinOpen  = [n \in Nodes |-> FALSE]
    /\ joinTokens = [n \in Nodes |-> NoArrivals]
    /\ fireCount = [n \in Nodes |-> 0]
    /\ premature = {}
    /\ completed = FALSE

Quiescent == \A g \in AllFlows : pending[g] = 0

\* A command settles once its queue is drained: no sweep runs afterwards.
Settled == Quiescent

\* The elements one step from `n` over its outgoing flows outside `blocked`.
Succ(n, blocked) == {Tgt[g] : g \in Out(n) \ blocked}

\* Mirrors `path_reaches_join`: `j` is reachable from `src` over sequence flows
\* other than those in `blocked` (the join's incoming flows that already hold
\* a token, Zeebe's `hasActivePathToTheGateway`). Each round names the
\* previous one exactly once: TLC does not cache that value inside the ENABLED
\* checks that fairness needs, so a second reference would make the cost
\* exponential in the round count.
PathReaches(src, j, blocked) ==
    LET R[k \in 0..Cardinality(Nodes)] ==
            IF k = 0 THEN {src}
            ELSE UNION {{n} \cup Succ(n, blocked) : n \in R[k-1]}
    IN  j \in R[Cardinality(Nodes)]

\* The live sources of `has_active_path_to`: the element instances of the
\* scope (waiting tasks and open joins) and the targets of queued activations
\* still in transit.
LiveSources ==
    {m \in Nodes : waiting[m] > 0 \/ joinOpen[m]} \cup {Target(g) : g \in {h \in AllFlows : pending[h] > 0}}

\* Mirrors `has_active_path_to`, which ignores `j` itself as a source, and so
\* every queued activation into `j` (their flows are already counted).
HasActivePathTo(j, arr) ==
    LET blocked == {g \in In(j) : arr[g] > 0}
    IN  \E m \in LiveSources \ {j} : PathReaches(m, j, blocked)

\* The single canonical Nano join-firing guard, as a function of a join `j` and
\* its per-flow arrivals `arr`. `ArriveJoin` fires exactly on this predicate and
\* `RefinesZeebe` compares exactly this predicate to Zeebe: routing both the
\* action guard and the refinement obligation through ONE definition is what
\* makes `RefinesZeebe` a genuine constraint on the action — an edit to the guard
\* changes both sides at once, so the invariant cannot silently pass over drift.
\* A parallel join fires once every incoming flow holds a token; an inclusive
\* join also fires when some flow holds a token and no live source can reach it.
JoinFires(j, arr) ==
    LET allTaken == \A g \in In(j) : arr[g] >= 1
    IN  IF j \in ParJoins THEN allTaken
        ELSE \/ allTaken
             \/ (\E g \in In(j) : arr[g] > 0) /\ ~HasActivePathTo(j, arr)

\* Taking flows `S` (`take_flow`): each is queued, and a flow into a join is
\* counted on it at once, as Zeebe counts a taken sequence flow (#1241).
TakeFlows(jt, S) ==
    [n \in Nodes |-> [g \in Flows |->
        IF g \in S /\ Tgt[g] = n /\ IsJoin(n) THEN jt[n][g] + 1 ELSE jt[n][g]]]

-----------------------------------------------------------------------------
(* Draining one queued activation (`activate`). *)

\* Pass-through elements: complete immediately and route to `next`.
PassThrough(f, next) ==
    /\ pending'    = Add(Take(pending, f), next)
    /\ joinTokens' = TakeFlows(joinTokens, next)
    /\ UNCHANGED <<waiting, joinOpen, fireCount, premature>>

ActivateTask(f) ==
    /\ pending' = Take(pending, f)
    /\ waiting' = [waiting EXCEPT ![Target(f)] = @ + 1]
    /\ UNCHANGED <<joinOpen, joinTokens, fireCount, premature>>

ActivateEnd(f) ==
    /\ pending' = Take(pending, f)
    /\ UNCHANGED <<waiting, joinOpen, joinTokens, fireCount, premature>>

\* Mirrors `activate_join`, which guards both join kinds. The arriving token was
\* already counted when its flow was taken. A parallel join fires once every
\* incoming flow holds a token (`canActivateParallelGateway`). An inclusive
\* join also fires when some flow holds a token and no live source can reach
\* it (`canActivateInclusiveGateway`). On firing it routes to the chosen subset
\* `S`; each incoming flow holding a token gives up one, including tokens whose
\* activation is still queued (that activation later finds nothing to do), and
\* a surplus reopens the join. A rejected arrival opens the join if it holds a
\* token. `premature` re-checks the parallel guard.
ArriveJoin(f, S) ==
    LET j        == Target(f)
        arr      == joinTokens[j]
        fires    == JoinFires(j, arr)
        rest     == [g \in Flows |-> IF arr[g] > 0 THEN arr[g] - 1 ELSE 0]
    IN  /\ IF fires
             THEN /\ pending'    = Add(Take(pending, f), S)
                  /\ joinOpen'   = [joinOpen EXCEPT ![j] = \E g \in Flows : rest[g] > 0]
                  /\ joinTokens' = TakeFlows([joinTokens EXCEPT ![j] = rest], S)
                  /\ fireCount'  = [fireCount EXCEPT ![j] = @ + 1]
                  /\ premature'  = IF j \in ParJoins /\ \E g \in In(j) : arr[g] = 0
                                     THEN premature \cup {j} ELSE premature
             ELSE /\ pending'    = Take(pending, f)
                  /\ joinOpen'   = [joinOpen EXCEPT ![j] = @ \/ \E g \in Flows : arr[g] > 0]
                  /\ UNCHANGED <<joinTokens, fireCount, premature>>
        /\ UNCHANGED waiting

\* The routing choices available when an activation of `n` completes: an
\* exclusive gateway takes one outgoing flow, an inclusive gateway (split or
\* join) takes a non-empty subset, and every other element takes all of them.
\* Each choice is its own action, so fairness can range over the choices.
Choices(n) ==
    CASE Kind[n] = "xor"                    -> {{g} : g \in Out(n)}
      [] Kind[n] = "or"                     -> NonEmptySubsets(Out(n))
      [] OTHER                              -> {Out(n)}

DrainVia(f, S) ==
    LET n == Target(f) IN
    /\ pending[f] > 0
    /\ S \in Choices(n)
    /\ CASE Kind[n] \in {"task", "evt"}  -> ActivateTask(f)
         [] Kind[n] = "end"              -> ActivateEnd(f)
         [] IsJoin(n)                    -> ArriveJoin(f, S)
         [] OTHER                        -> PassThrough(f, S)
    /\ UNCHANGED completed

Drain(f) == \E S \in Choices(Target(f)) : DrainVia(f, S)

\* A drain choice that is a stuttering step in EVERY state: a pass-through
\* (non-join, non-wait, non-end) element routing straight back onto its own
\* incoming flow `f` takes and re-adds the same token and records nothing.
\* Every other choice changes `pending` (or `fireCount`), so `<<DrainVia(f, S)>>_vars` is
\* enabled exactly when `Drain(f)` is.
StuttersVia(f, S) ==
    LET n == Target(f) IN
    /\ Kind[n] \notin {"task", "evt", "end"}
    /\ ~IsJoin(n)
    /\ S = {f}

\* Strong fairness on every routing choice of `f`, factored. Because
\* `ENABLED <<DrainVia(f, S)>>_vars` is the same for every non-stuttering choice
\* S (it is `ENABLED <<Drain(f)>>_vars`), and a stuttering choice's SF is vacuous,
\*     \A S \in Choices(Target(f)) : SF_vars(DrainVia(f, S))
\* is EQUIVALENT to this single implication. Stated per choice, TLC expands each
\* SF disjunction into its DNF tableau — 2^|Choices| branches, which runs out of
\* memory on an inclusive split with four outgoing flows (#1246). Factored, it is
\* one disjunction per flow.
DrainFair(f) ==
    []<>(ENABLED <<Drain(f)>>_vars)
        => \A S \in Choices(Target(f)) :
               ~StuttersVia(f, S) => []<><<DrainVia(f, S)>>_vars


(* External command: a job worker completes a task. *)
CompleteTask(t) ==
    /\ Settled
    /\ ~completed
    /\ waiting[t] > 0
    /\ waiting' = [waiting EXCEPT ![t] = @ - 1]
    /\ pending'    = Add(pending, Out(t))
    /\ joinTokens' = TakeFlows(joinTokens, Out(t))
    /\ UNCHANGED <<joinOpen, fireCount, premature, completed>>

(* External command: a message correlates to one subscription of a waiting
   event-based gateway (#1246), which completes and takes the flow `g` to the
   triggered catch event. *)
CorrelateEvent(n, g) ==
    /\ Settled
    /\ ~completed
    /\ waiting[n] > 0
    /\ waiting'    = [waiting EXCEPT ![n] = @ - 1]
    /\ pending'    = Add(pending, {g})
    /\ joinTokens' = TakeFlows(joinTokens, {g})
    /\ UNCHANGED <<joinOpen, fireCount, premature, completed>>

(* `complete_finished_instances`. *)
CompleteInstance ==
    /\ Settled
    /\ ~completed
    /\ \A n \in Nodes : waiting[n] = 0 /\ ~joinOpen[n]
    /\ completed' = TRUE
    /\ UNCHANGED <<pending, waiting, joinOpen, joinTokens, fireCount, premature>>

\* Stutter after completion so a completed instance is not reported as a deadlock.
\* Any other state with no enabled action is a stuck instance, which TLC reports.
Done == completed /\ UNCHANGED vars

Next ==
    \/ \E f \in AllFlows : Drain(f)
    \/ \E t \in Tasks    : CompleteTask(t)
    \/ \E n \in EvtGateways : \E g \in Out(n) : CorrelateEvent(n, g)
    \/ CompleteInstance
    \/ Done

\* Fairness, exactly as stated in `Fairness` below:
\*   - SF on each `DrainVia(f, S)`. Each
\*     routing choice S is its own action, so strong fairness means a gateway
\*     reached infinitely often eventually takes each of its branches. This is
\*     the "fair data" assumption of workflow-net soundness, and it lets a loop
\*     with an exit terminate. Conditions are abstracted, so this states what a
\*     graph *allows*, not that particular data exits a loop.
\*   - SF on each `CompleteTask(t)`: a task can only complete in a settled
\*     state, so it can be enabled intermittently while other work drains.
\*   - WF on `CompleteInstance`: once enabled, it stays enabled.
\* (Fairness on Next as a whole would only guarantee that *some* step happens,
\* so one task could starve.)
Fairness ==
    /\ \A f \in AllFlows : DrainFair(f)
    /\ \A t \in Tasks    : SF_vars(CompleteTask(t))
    /\ \A n \in EvtGateways : \A g \in Out(n) : SF_vars(CorrelateEvent(n, g))
    /\ WF_vars(CompleteInstance)

Spec == Init /\ [][Next]_vars /\ Fairness

-----------------------------------------------------------------------------
(* Properties *)

\* Join bookkeeping stays coherent (the class behind the missing
\* `ParallelJoinReset` bugs): an open join holds tokens, a join holding tokens
\* is open unless an activation into it is still queued (flows are counted
\* when taken), tokens only sit on a join's own incoming flows, only joins are
\* ever open, and a parallel join never has every incoming flow fed without an
\* activation on its way to fire it.
InTransitTo(n) == \E g \in In(n) : pending[g] > 0

JoinBookkeepingCoherent ==
    /\ \A n \in Nodes : joinOpen[n] => \E g \in Flows : joinTokens[n][g] > 0
    /\ \A n \in Nodes : (\E g \in Flows : joinTokens[n][g] > 0) /\ ~joinOpen[n] => InTransitTo(n)
    /\ \A n \in Nodes : \A g \in Flows \ In(n) : joinTokens[n][g] = 0
    /\ \A n \in Nodes \ (ParJoins \cup IncJoins) : ~joinOpen[n]
    /\ \A j \in ParJoins : (\A g \in In(j) : joinTokens[j][g] > 0) => InTransitTo(j)

\* BPMN / Zeebe parity: a parallel join activates only once *every* incoming
\* sequence flow has been taken (Zeebe counts distinct taken flows,
\* `ProcessInstanceStateTransitionGuard.canActivateParallelGateway`).
ParallelJoinWaitsForEveryFlow == premature = {}

\* Once a settled instance has no task left that could drive it on, no join may
\* still be open. Otherwise the instance can never complete: it is stuck
\* forever on a join that no token will ever reach.
NoStuckInstance ==
    (Settled /\ ~completed /\ \A t \in WaitStates : waiting[t] = 0)
        => \A n \in Nodes : ~joinOpen[n]

\* Whether the process graph has no cycle. This is derived from the graph, never
\* declared, so a model cannot be mislabelled and silently skip the property
\* below.
Acyclic == \A n \in Nodes : n \notin Reaching(n)

\* Without cycles, every join fires at most once per instance. A rework loop
\* legitimately re-fires a join. Acyclicity alone does not rule out a second
\* firing: an acyclic graph that routes several tokens onto one join's inputs
\* (a parallel split whose branches merge through an exclusive gateway, as in
\* MCParallelJoinSurplus and MCInclusiveJoinSurplus) can fire it again on the surplus tokens. That is
\* intended: such a graph is not 1-safe, a BPMN lack-of-synchronization, and
\* this invariant is how the checker reports it. Record such a model as an
\* expected violation only when it deliberately reproduces an engine defect or
\* an unsound graph (see check.sh).
JoinFiresAtMostOnce == Acyclic => \A j \in ParJoins \cup IncJoins : fireCount[j] <= 1

\* Every instance eventually completes, under the fairness above.
Termination == <>completed

-----------------------------------------------------------------------------
(* Refinement mapping to the Zeebe reference spec (#1240, slice 2).           *)
(*                                                                         *)
(* Nano is a strict superset of Camunda 8: on Camunda's surface there is NO   *)
(* tolerated divergence. `ZeebeTokenFlow.tla` is the Zeebe reference oracle,   *)
(* derived directly from `zeebe/engine`, and its gateway guards are exposed as *)
(* PURE predicates. This spec is trace-equivalent to it on the observable      *)
(* vocabulary iff the only nontrivial semantic surface — the join activation   *)
(* decision — coincides on every reachable state: every other action           *)
(* (pass-through, task activation, end, split routing) is structurally         *)
(* identical between the two, so the join guard is the sole refinement         *)
(* obligation. `RefinesZeebe` discharges it by asserting, over the whole MC    *)
(* corpus, that this spec's join-firing decision equals the independently      *)
(* re-derived Zeebe decision. Because the Zeebe guard lives ONLY in            *)
(* `ZeebeTokenFlow.tla`, an edit that drifts this spec's guard away from Zeebe *)
(* fails the invariant until the reference (and hence the claim of Camunda     *)
(* parity) is updated too.                                                     *)
(*                                                                         *)
(* Every historical nano-vs-Zeebe difference is a filed parity issue. The      *)
(* first — the arrival-time inclusive-join guard — is #1241 (closed): before   *)
(* it, a quiescence sweep re-evaluated waiting inclusive joins, so             *)
(* MCInclusiveDivergentPath / MCInclusiveJoinSurplus completed where Zeebe      *)
(* leaves them stuck. With #1241 landed, `RefinesZeebe` holds on the whole     *)
(* corpus, so there is no open divergence to file.                            *)
ZeebeRef == INSTANCE ZeebeTokenFlow WITH
    queue    <- pending,
    active   <- waiting,
    open     <- joinOpen,
    taken    <- joinTokens,
    fired    <- fireCount,
    earlyPar <- premature,
    done     <- completed

\* This spec's own join-firing decision at a join's current arrivals — the SAME
\* canonical `JoinFires` guard that `ArriveJoin` fires on, lifted to a state
\* predicate. Sharing the definition is deliberate: it is what makes the
\* equality below a real refinement obligation on the action, not a check of a
\* parallel re-derivation that could drift from the guard it claims to mirror.
NanoJoinReady(j) == JoinFires(j, joinTokens[j])

RefinesZeebe ==
    \A j \in ParJoins \cup IncJoins :
        NanoJoinReady(j) = ZeebeRef!ZeebeJoinReady(j, joinTokens[j], LiveSources)
=============================================================================
