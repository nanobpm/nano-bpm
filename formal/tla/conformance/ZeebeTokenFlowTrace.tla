------------------------- MODULE ZeebeTokenFlowTrace -------------------------
(***************************************************************************)
(* Trace validation of the REAL Zeebe engine against `ZeebeTokenFlow.tla`  *)
(* (#1962) — link A of the parity-proof chain:                             *)
(*                                                                         *)
(*   Zeebe Java --(A: this module)--> ZeebeTokenFlow.tla                    *)
(*                                                                         *)
(* `ZeebeTokenFlow.tla` is hand-transcribed from `zeebe/engine`. This       *)
(* module checks that a record stream the pinned Zeebe engine actually       *)
(* exported is a BEHAVIOUR of that transcription: TLC explores the reference *)
(* model with the observed trace as a constraint (the same mechanism #1226   *)
(* uses for engine-core), so a Zeebe trace the model rejects means the        *)
(* reference model is wrong.                                                 *)
(*                                                                         *)
(* The reference semantics are NOT restated here: the model is `INSTANCE`d   *)
(* (`Z`), and every step of `TraceNext` IS one of `Z`'s own actions. What    *)
(* this module adds is only the observable LABEL of each action — the        *)
(* exporter records it emits, in the shared spec vocabulary (#1259):         *)
(*                                                                         *)
(*   | Milestone      | Exporter record                                  |  *)
(*   |----------------|--------------------------------------------------|  *)
(*   | "flow:<f>"     | SEQUENCE_FLOW_TAKEN of flow f                    |  *)
(*   | "act:<n>"      | ELEMENT_ACTIVATED of a wait state (task / evt)   |  *)
(*   | "fire:<j>"     | ELEMENT_ACTIVATED of a join gateway              |  *)
(*   | "defer:<j>"    | (unobservable: a join arrival that waits)        |  *)
(*   | "done"         | ELEMENT_COMPLETED of the process instance        |  *)
(*                                                                         *)
(* A step consumes the next |E| trace entries, which must equal its emitted  *)
(* set E (order WITHIN one step is free: Zeebe writes one command's records  *)
(* contiguously, in an order the model abstracts). A step emitting nothing   *)
(* (a deferred join arrival, an end event) is a silent step. "defer:" is     *)
(* never in a trace; it is only printed for the coverage report.             *)
(*                                                                         *)
(* The trace is ACCEPTED iff some behaviour reaches `pos = Len(Trace)` in a  *)
(* settled state: the invariant `TraceUnmatched` is then violated, and TLC's *)
(* counterexample is the witness behaviour. If TLC exhausts the constrained  *)
(* state space without a violation, Zeebe exhibited a behaviour the          *)
(* reference model does not allow.                                           *)
(*                                                                         *)
(* `MatchLimit` caps the prefix that must be matched (the driver searches it *)
(* to report the longest accepted prefix of a rejected trace). With          *)
(* `PrintDecisions`, every join decision the explored behaviours take is     *)
(* printed — run over the UNCONSTRAINED model (`Unconstrained`), that is the *)
(* model-side half of the coverage check (#1962: every model-enabled join    *)
(* decision must be observed in Zeebe for some completion order).            *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS Nodes, Kind, Flows, Src, Tgt, Start,
          Trace,           \* Seq(STRING): the observed milestones
          MatchLimit,      \* the trace prefix length that counts as accepted
          Unconstrained,   \* TRUE: ignore Trace (coverage enumeration)
          PrintDecisions   \* TRUE: PrintT every join decision taken

VARIABLES queue, active, open, taken, fired, earlyPar, done, pos

Z == INSTANCE ZeebeTokenFlow

zvars == <<queue, active, open, taken, fired, earlyPar, done>>

\* The label of one drain step of flow `f` routed via `S`.
DrainEmits(f, S) ==
    LET n == Z!Target(f) IN
    CASE Kind[n] \in {"task", "evt"} -> {"act:" \o n}
      [] Kind[n] = "end"             -> {}
      [] Z!IsJoin(n)                 -> IF fired'[n] > fired[n]
                                          THEN {"fire:" \o n} \cup {"flow:" \o g : g \in S}
                                          ELSE {}
      [] OTHER                       -> {"flow:" \o g : g \in S}

\* The join decision a drain step takes (empty for a non-join).
DrainDecision(f) ==
    LET n == Z!Target(f) IN
    IF Z!IsJoin(n)
      THEN IF fired'[n] > fired[n] THEN {"fire:" \o n} ELSE {"defer:" \o n}
      ELSE {}

\* Consume the next |E| trace entries, which must be exactly E. A PREFIX
\* probe (`MatchLimit < Len(Trace)`) may instead stop WITHIN E: the entries
\* up to `MatchLimit` need only be distinct members of E (entries beyond the
\* limit are ignored), so the longest accepted prefix is found per entry and
\* the first wrong entry — not a valid sibling of its step — is blamed. Full
\* validation (`MatchLimit = Len(Trace)`) keeps the exact match.
Match(E) ==
    IF Unconstrained THEN pos' = pos
    ELSE LET k == Cardinality(E) IN
         \/ /\ pos + k <= Len(Trace)
            /\ {Trace[i] : i \in (pos + 1)..(pos + k)} = E
            /\ pos' = pos + k
         \/ /\ MatchLimit < Len(Trace)
            /\ pos < MatchLimit
            /\ pos + k > MatchLimit
            /\ LET W == {Trace[i] : i \in (pos + 1)..MatchLimit} IN
                 /\ W \subseteq E
                 /\ Cardinality(W) = MatchLimit - pos
            /\ pos' = MatchLimit

Decide(D) == IF PrintDecisions THEN \A d \in D : PrintT(<<"DECISION", d>>) ELSE TRUE

TraceInit == Z!Init /\ pos = 0

TraceNext ==
    \/ \E f \in Z!AllFlows : \E S \in Z!Choices(Z!Target(f)) :
         /\ Z!DrainVia(f, S)
         /\ Match(DrainEmits(f, S))
         /\ Decide(DrainDecision(f))
    \/ \E t \in Z!Tasks : Z!CompleteTask(t) /\ Match({"flow:" \o g : g \in Z!Out(t)})
    \/ \E n \in Z!EvtGateways : \E g \in Z!Out(n) :
         Z!CorrelateEvent(n, g) /\ Match({})
    \/ Z!CompleteInstance /\ Match({"done"})

\* Where Zeebe's trace ENDS, the model must be at rest too: settled (no
\* activation in flight) and not owing the instance completion — Zeebe
\* completes a process instance in the same command that ends its last
\* element, so a trace that stops short of `done` while the model could
\* complete is NOT a behaviour.
AtRest == Z!Settled /\ ~ENABLED Z!CompleteInstance

\* Violated exactly when the first `MatchLimit` trace entries are matched by a
\* behaviour (and, for the full trace, the model has come to rest as Zeebe had).
TraceUnmatched ==
    ~(/\ pos >= MatchLimit
      /\ MatchLimit < Len(Trace) \/ AtRest)
=============================================================================
