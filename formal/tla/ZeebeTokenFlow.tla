--------------------------- MODULE ZeebeTokenFlow ---------------------------
(***************************************************************************)
(* Zeebe reference token flow — the *correct* gateway/join semantics on    *)
(* Camunda 8's surface (#1240, slice 1). Nano is a strict superset of       *)
(* Camunda 8: everything Camunda supports must behave EXACTLY as Zeebe does, *)
(* so this module is the oracle nano's `TokenFlow.tla` is refined against    *)
(* (slice 2, `RefinesZeebe`). It is derived directly from `zeebe/engine`,    *)
(* not from nano, so a nano-vs-Zeebe divergence surfaces as a refinement     *)
(* failure rather than being defined away.                                   *)
(*                                                                         *)
(* Vocabulary rule (#1240): the *observable* state here is drawn ONLY from     *)
(* what both engines expose — Zeebe's internal bookkeeping is observable        *)
(* solely through its exported records, so each observable variable maps to an  *)
(* exporter record. Alongside these the module carries a small, explicitly      *)
(* marked set of AUXILIARY variables — control state derivable from the records *)
(* (`queue`, `open`) and pure ghost bookkeeping (`fired`, `earlyPar`) — that    *)
(* exist only to drive the transition relation and state invariants, and are    *)
(* NOT part of the observable projection the shared-vocabulary claim covers:     *)
(*                                                                         *)
(*   Observable (one exporter record each):                                 *)
(*   - `taken`  : per sequence flow, the count of `SEQUENCE_FLOW_TAKEN`      *)
(*     records not yet consumed by a gateway. Zeebe records a taken flow on   *)
(*     a join in `takenSequenceFlows` and clears it on activation             *)
(*     (`cleanupSequenceFlowsTaken`). (`SequenceFlowTaken` /                  *)
(*     `takeSequenceFlow` in `BpmnStateTransitionBehavior`.)                  *)
(*   - `active` : task element instances in `ELEMENT_ACTIVATED`, parked as a  *)
(*     job wait state until a `COMPLETE` command (job completion).            *)
(*   - `done`   : the process instance `ELEMENT_COMPLETED`.                    *)
(*                                                                         *)
(*   Auxiliary — control state derivable from the exporter records, not a    *)
(*   fresh observable, carried so the transition relation is expressible:    *)
(*   - `queue`  : ACTIVATE_ELEMENT commands in flight — the process-instance  *)
(*     command stream Zeebe drains before the record settles. A flow taken    *)
(*     to a non-gateway activates its target (`ELEMENT_ACTIVATING`).          *)
(*   - `open`   : a gateway element instance that has been reached but cannot  *)
(*     yet activate (a deferred join) — i.e. taken flows recorded with no      *)
(*     `ELEMENT_ACTIVATED` yet, a projection of the records above.            *)
(*                                                                         *)
(*   Auxiliary — pure ghost bookkeeping, kept only to STATE an invariant:    *)
(*   - `fired`  : gateway `ELEMENT_ACTIVATED` count (itself observable as the  *)
(*     count of that record); lets us state `JoinFiresAtMostOnce`.            *)
(*   - `earlyPar`: parallel joins that activated before every incoming flow   *)
(*     was taken — the BPMN/Zeebe violation `canActivateParallelGateway`      *)
(*     forbids. Kept only to state `ParallelJoinWaitsForEveryFlow`.           *)
(*                                                                         *)
(* Gateway guards are the crux, and are cited one for one against Zeebe:      *)
(*                                                                         *)
(*   - Parallel join (`ParallelGatewayProcessor`,                            *)
(*     `ProcessInstanceStateTransitionGuard.canActivateParallelGateway`):     *)
(*     activate once *every distinct* incoming sequence flow has a taken      *)
(*     record. Zeebe counts distinct taken flows, not arrivals, so two tokens *)
(*     on one flow do not substitute for a missing flow (#1233). On firing it *)
(*     consumes exactly one taken record per incoming flow                    *)
(*     (`cleanupSequenceFlowsTaken`) and keeps any surplus — the "Tetris"     *)
(*     principle — so a not-1-safe graph fires it again on the surplus.       *)
(*   - Inclusive join (`InclusiveGatewayProcessor`,                          *)
(*     `canActivateInclusiveGateway`): readiness is decided ONLY when a flow  *)
(*     is taken into the gateway (#1241 — Zeebe does not re-evaluate a join    *)
(*     on unrelated progress). It activates if every incoming flow has a      *)
(*     taken record, OR some incoming flow has one and no other token can     *)
(*     still reach the gateway without crossing a flow that already holds a   *)
(*     taken record (`hasActivePathToTheGateway`). An inclusive join whose    *)
(*     partner branch diverges away therefore waits forever, exactly as in    *)
(*     Zeebe.                                                                 *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS
    Nodes,        \* element ids
    Kind,         \* [Nodes -> ElementKinds]
    Flows,        \* sequence-flow ids (distinct ids may share endpoints)
    Src,          \* [Flows -> Nodes] source element of each flow
    Tgt,          \* [Flows -> Nodes] target element of each flow
    Start         \* the (single) none start event

ElementKinds == {"start", "end", "task", "xor", "and", "or", "evt", "msg"}

ASSUME Kind \in [Nodes -> ElementKinds]
ASSUME Src \in [Flows -> Nodes] /\ Tgt \in [Flows -> Nodes]
ASSUME Start \in Nodes /\ Kind[Start] = "start"

\* Zeebe activates the start event via a process-instance ACTIVATE command that
\* takes no sequence flow; modelled as a pseudo-flow into Start.
InitFlow == "<create>"
ASSUME InitFlow \notin Flows
AllFlows == Flows \cup {InitFlow}

Target(f) == IF f = InitFlow THEN Start ELSE Tgt[f]

In(n)  == {f \in Flows : Tgt[f] = n}
Out(n) == {f \in Flows : Src[f] = n}

IsJoin(n) == Kind[n] \in {"and", "or"} /\ Cardinality(In(n)) > 1
ParJoins  == {n \in Nodes : Kind[n] = "and" /\ IsJoin(n)}
IncJoins  == {n \in Nodes : Kind[n] = "or" /\ IsJoin(n)}
Joins     == ParJoins \cup IncJoins
Tasks     == {n \in Nodes : Kind[n] = "task"}
\* Event-based gateways (#1246): a wait state like a task, but the external
\* command that resumes it (a message correlation) picks ONE outgoing flow —
\* the one leading to the intermediate catch event (`msg`) whose subscription
\* was triggered. The other subscriptions are closed with the gateway. A `msg`
\* catch event, reached only from such a gateway, is then a pass-through.
EvtGateways == {n \in Nodes : Kind[n] = "evt"}
WaitStates  == Tasks \cup EvtGateways

NonEmptySubsets(S) == (SUBSET S) \ {{}}

Succ(n, blocked) == {Tgt[g] : g \in Out(n) \ blocked}

-----------------------------------------------------------------------------
(* Zeebe's `hasActivePathToTheGateway` (InclusiveGatewayProcessor). A gateway *)
(* is ready only when no *other* live token can still reach it without        *)
(* crossing a flow that already holds a taken record. These operators are     *)
(* PURE (functions of the graph and the passed-in observable state), so       *)
(* `TokenFlow.tla`'s slice-2 refinement mapping can call them directly.       *)

\* `j` is reachable from `src` over sequence flows outside `blocked` — Zeebe
\* walks the process graph skipping the gateway's already-taken incoming flows.
ZeebePathReaches(src, j, blocked) ==
    LET R[k \in 0..Cardinality(Nodes)] ==
            IF k = 0 THEN {src}
            ELSE UNION {{n} \cup Succ(n, blocked) : n \in R[k-1]}
    IN  j \in R[Cardinality(Nodes)]

\* The already-taken incoming flows of `j` (Zeebe skips these in the search).
ZeebeBlocked(j, taken) == {g \in In(j) : taken[g] > 0}

\* `liveSources` is the set of element instances that can still drive a token
\* (activated tasks, other open gateways, and the targets of queued
\* activations). Zeebe ignores `j` itself as a source: its own queued
\* activation's flow is already counted in `taken`.
ZeebeHasActivePath(j, taken, liveSources) ==
    \E m \in liveSources \ {j} : ZeebePathReaches(m, j, ZeebeBlocked(j, taken))

\* `canActivateParallelGateway`: every distinct incoming flow has a taken record.
ZeebeParallelReady(j, taken) == \A g \in In(j) : taken[g] >= 1

\* `canActivateInclusiveGateway`: all flows taken, or some flow taken and no
\* live source can still reach `j` (#1241, arrival-time evaluation).
ZeebeInclusiveReady(j, taken, liveSources) ==
    \/ ZeebeParallelReady(j, taken)
    \/ (\E g \in In(j) : taken[g] > 0) /\ ~ZeebeHasActivePath(j, taken, liveSources)

\* The single reference decision for any join, dispatched by kind. This is the
\* oracle `TokenFlow.tla` must match on every reachable state (slice 2).
ZeebeJoinReady(j, taken, liveSources) ==
    IF j \in ParJoins THEN ZeebeParallelReady(j, taken)
                      ELSE ZeebeInclusiveReady(j, taken, liveSources)

-----------------------------------------------------------------------------
(* Operational model of one process instance, in the exporter vocabulary. *)

VARIABLES
    queue,        \* [AllFlows -> Nat]  ACTIVATE commands in flight, keyed by flow
    active,       \* [Nodes -> Nat]     ELEMENT_ACTIVATED tasks (job wait states)
    open,         \* [Nodes -> BOOLEAN] a deferred join element instance
    taken,        \* [Nodes -> [Flows -> Nat]] taken sequence-flow records per join
    fired,        \* ghost: [Nodes -> Nat] gateway ELEMENT_ACTIVATED count
    earlyPar,     \* ghost: parallel joins activated before every flow was taken
    done          \* process instance ELEMENT_COMPLETED

vars == <<queue, active, open, taken, fired, earlyPar, done>>

Add(p, S)  == [g \in DOMAIN p |-> IF g \in S THEN p[g] + 1 ELSE p[g]]
Take(p, f) == [p EXCEPT ![f] = @ - 1]
NoTaken    == [g \in Flows |-> 0]

\* Recording taken flows (`takeSequenceFlow`): a flow into a join records a
\* taken sequence flow on it; other flows just carry the activation in `queue`.
RecordTaken(tk, S) ==
    [n \in Nodes |-> [g \in Flows |->
        IF g \in S /\ Tgt[g] = n /\ IsJoin(n) THEN tk[n][g] + 1 ELSE tk[n][g]]]

TypeOK ==
    /\ queue    \in [AllFlows -> Nat]
    /\ active   \in [Nodes -> Nat]
    /\ open     \in [Nodes -> BOOLEAN]
    /\ taken    \in [Nodes -> [Flows -> Nat]]
    /\ fired    \in [Nodes -> Nat]
    /\ earlyPar \subseteq ParJoins
    /\ done     \in BOOLEAN

Init ==
    /\ queue    = [g \in AllFlows |-> IF g = InitFlow THEN 1 ELSE 0]
    /\ active   = [n \in Nodes |-> 0]
    /\ open     = [n \in Nodes |-> FALSE]
    /\ taken    = [n \in Nodes |-> NoTaken]
    /\ fired    = [n \in Nodes |-> 0]
    /\ earlyPar = {}
    /\ done     = FALSE

Quiescent == \A g \in AllFlows : queue[g] = 0
Settled   == Quiescent

\* The live sources of `hasActivePathToTheGateway`: activated tasks, open joins,
\* and the targets of queued activations still in transit.
LiveSources ==
    {m \in Nodes : active[m] > 0 \/ open[m]}
        \cup {Target(g) : g \in {h \in AllFlows : queue[h] > 0}}

-----------------------------------------------------------------------------
(* Draining one queued ACTIVATE command. *)

PassThrough(f, next) ==
    /\ queue' = Add(Take(queue, f), next)
    /\ taken' = RecordTaken(taken, next)
    /\ UNCHANGED <<active, open, fired, earlyPar>>

ActivateTask(f) ==
    /\ queue'  = Take(queue, f)
    /\ active' = [active EXCEPT ![Target(f)] = @ + 1]
    /\ UNCHANGED <<open, taken, fired, earlyPar>>

ActivateEnd(f) ==
    /\ queue' = Take(queue, f)
    /\ UNCHANGED <<active, open, taken, fired, earlyPar>>

\* A flow arriving at a join. Its taken record was added when the flow was
\* taken. `ZeebeJoinReady` decides activation; on firing, one taken record per
\* incoming flow is consumed (`cleanupSequenceFlowsTaken`), the surplus is kept
\* and reopens the gateway, and the chosen outgoing subset is taken.
ArriveJoin(f, S) ==
    LET j    == Target(f)
        tk   == taken[j]
        rest == [g \in Flows |-> IF tk[g] > 0 THEN tk[g] - 1 ELSE 0]
    IN  /\ IF ZeebeJoinReady(j, tk, LiveSources)
             THEN /\ queue'    = Add(Take(queue, f), S)
                  /\ open'     = [open EXCEPT ![j] = \E g \in Flows : rest[g] > 0]
                  /\ taken'    = RecordTaken([taken EXCEPT ![j] = rest], S)
                  /\ fired'    = [fired EXCEPT ![j] = @ + 1]
                  /\ earlyPar' = IF j \in ParJoins /\ \E g \in In(j) : tk[g] = 0
                                   THEN earlyPar \cup {j} ELSE earlyPar
             ELSE /\ queue'    = Take(queue, f)
                  /\ open'     = [open EXCEPT ![j] = @ \/ \E g \in Flows : tk[g] > 0]
                  /\ UNCHANGED <<taken, fired, earlyPar>>
        /\ UNCHANGED active

\* Routing choices on completion: an exclusive gateway takes one outgoing flow,
\* an inclusive gateway a non-empty subset, everything else all outgoing flows.
Choices(n) ==
    CASE Kind[n] = "xor" -> {{g} : g \in Out(n)}
      [] Kind[n] = "or"  -> NonEmptySubsets(Out(n))
      [] OTHER           -> {Out(n)}

DrainVia(f, S) ==
    LET n == Target(f) IN
    /\ queue[f] > 0
    /\ S \in Choices(n)
    /\ CASE Kind[n] \in {"task", "evt"} -> ActivateTask(f)
         [] Kind[n] = "end"  -> ActivateEnd(f)
         [] IsJoin(n)        -> ArriveJoin(f, S)
         [] OTHER            -> PassThrough(f, S)
    /\ UNCHANGED done

Drain(f) == \E S \in Choices(Target(f)) : DrainVia(f, S)

\* A drain choice that is a stuttering step in EVERY state: a pass-through
\* (non-join, non-wait, non-end) element routing straight back onto its own
\* incoming flow `f` takes and re-adds the same token and records nothing.
\* Every other choice changes `queue` (or `fired`), so `<<DrainVia(f, S)>>_vars` is
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


\* Job completion (a COMPLETE command): an activated task takes its outgoing
\* flows once the previous record has settled.
CompleteTask(t) ==
    /\ Settled
    /\ ~done
    /\ active[t] > 0
    /\ active' = [active EXCEPT ![t] = @ - 1]
    /\ queue'  = Add(queue, Out(t))
    /\ taken'  = RecordTaken(taken, Out(t))
    /\ UNCHANGED <<open, fired, earlyPar, done>>

\* A message correlation (#1246): a waiting event-based gateway completes on
\* the triggered subscription and ACTIVATES the catch event at the end of the
\* ONE flow `g` (`EventBasedGatewayProcessor` / `BpmnEventSubscriptionBehavior
\* .activateTriggeredEvent`). Zeebe does NOT take `g`: no SEQUENCE_FLOW_TAKEN
\* is written for it, so `g`'s queue entry here is the catch event's
\* activation, not a taken flow — a correlation is SILENT in the exporter
\* vocabulary (pinned against real Zeebe records by #1962,
\* formal/tla/conformance/ZeebeTokenFlowTrace.tla). The catch event is never
\* a join, so `RecordTaken` is a no-op on `g`.
CorrelateEvent(n, g) ==
    /\ Settled
    /\ ~done
    /\ active[n] > 0
    /\ active' = [active EXCEPT ![n] = @ - 1]
    /\ queue'  = Add(queue, {g})
    /\ taken'  = RecordTaken(taken, {g})
    /\ UNCHANGED <<open, fired, earlyPar, done>>

\* The instance completes once settled with no live element instance.
CompleteInstance ==
    /\ Settled
    /\ ~done
    /\ \A n \in Nodes : active[n] = 0 /\ ~open[n]
    /\ done' = TRUE
    /\ UNCHANGED <<queue, active, open, taken, fired, earlyPar>>

Done == done /\ UNCHANGED vars

Next ==
    \/ \E f \in AllFlows : Drain(f)
    \/ \E t \in Tasks    : CompleteTask(t)
    \/ \E n \in EvtGateways : \E g \in Out(n) : CorrelateEvent(n, g)
    \/ CompleteInstance
    \/ Done

Fairness ==
    /\ \A f \in AllFlows : DrainFair(f)
    /\ \A t \in Tasks    : SF_vars(CompleteTask(t))
    /\ \A n \in EvtGateways : \A g \in Out(n) : SF_vars(CorrelateEvent(n, g))
    /\ WF_vars(CompleteInstance)

Spec == Init /\ [][Next]_vars /\ Fairness

-----------------------------------------------------------------------------
(* Zeebe-parity properties (the reference verdicts nano must reproduce). *)

InTransitTo(n) == \E g \in In(n) : queue[g] > 0

\* Taken-record bookkeeping stays coherent: an open join holds a taken record,
\* a join holding one is open unless its activation is still queued, records
\* only sit on a join's own incoming flows, only joins ever hold records/open,
\* and a parallel join is never fully fed without an activation on its way.
TakenBookkeepingCoherent ==
    /\ \A n \in Nodes : open[n] => \E g \in Flows : taken[n][g] > 0
    /\ \A n \in Nodes : (\E g \in Flows : taken[n][g] > 0) /\ ~open[n] => InTransitTo(n)
    /\ \A n \in Nodes : \A g \in Flows \ In(n) : taken[n][g] = 0
    /\ \A n \in Nodes \ Joins : ~open[n]
    /\ \A j \in ParJoins : (\A g \in In(j) : taken[j][g] > 0) => InTransitTo(j)

\* `canActivateParallelGateway`: never fire a parallel join early.
ParallelJoinWaitsForEveryFlow == earlyPar = {}

\* A settled instance with no runnable task has no join still deferred.
NoStuckInstance ==
    (Settled /\ ~done /\ \A t \in WaitStates : active[t] = 0)
        => \A n \in Nodes : ~open[n]

\* Acyclicity, derived from the graph (never declared), guards JoinFiresAtMostOnce.
ReachingFrom(t) ==
    LET R[k \in 0..Cardinality(Nodes)] ==
            IF k = 0 THEN {Src[f] : f \in In(t)}
            ELSE LET prev == R[k-1]
                 IN  prev \cup {Src[f] : f \in {g \in Flows : Tgt[g] \in prev}}
    IN  R[Cardinality(Nodes)]
ReachingMap == [t \in Nodes |-> ReachingFrom(t)]
Reaching(t) == ReachingMap[t]
Acyclic == \A n \in Nodes : n \notin Reaching(n)

\* Without cycles a sound (1-safe) graph fires each join at most once. An
\* acyclic not-1-safe graph legitimately re-fires on surplus tokens.
JoinFiresAtMostOnce == Acyclic => \A j \in Joins : fired[j] <= 1

\* Every instance eventually completes, under `Fairness`.
Termination == <>done
=============================================================================
