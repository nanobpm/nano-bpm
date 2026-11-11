---------------------------- MODULE VariableScopes ----------------------------
(***************************************************************************)
(* Variable-scope isolation under parallelism (#1522, epic #1224).          *)
(*                                                                           *)
(* TokenFlow / JobLease / RaftHandoff / SnapshotReplay carry no variables,   *)
(* so scope-isolation defects (#1518: a parallel multi-instance child's      *)
(* `outputElement` result overwritten by a sibling) were out of their reach. *)
(* This spec models the Zeebe-style hierarchical variable tree of one        *)
(* process instance running a PARALLEL multi-instance embedded sub-process:  *)
(*                                                                           *)
(*   root (process instance, scope `Root`)                                   *)
(*     `- MI body (scope `Body`, owns the output collection)                 *)
(*          |- child 1 (scope 1)   item=1, loopCounter=1 [, outputElement]   *)
(*          |- ...                                                           *)
(*          `- child N (scope N)                                             *)
(*                                                                           *)
(* Each child runs two service tasks in sequence (the #1518 shape): `Write`  *)
(* completes the first job with `{result: <item>, seen: <item>}`, `Finish`   *)
(* completes the second (no variables), which completes the child and        *)
(* collects its `outputElement` into `outputCollection[index]`. Children     *)
(* interleave arbitrarily — the parallelism under test.                      *)
(*                                                                           *)
(* Seeding rule (grounded in Zeebe at the pinned sha, see                   *)
(* formal/parity/zeebe-pin.json, 5a3c4a1ca7cd9a0c50be34227fe3b4d8bf731dcf:   *)
(* zeebe/engine/src/main/java/io/camunda/zeebe/engine/processing/bpmn/       *)
(* container/MultiInstanceBodyProcessor.java `setLoopVariables`): on child   *)
(* activation Zeebe sets, as LOCAL variables of the inner instance,          *)
(*   - the `inputElement` (bound to the item),                               *)
(*   - `loopCounter` (1-based),                                              *)
(*   - the root variable of an `outputElement` that is a bare variable or a  *)
(*     nested property of one, initialised to nil — EXCEPT when it names the *)
(*     same variable as the input element (already local) or `loopCounter`.  *)
(* `onActivate` initialises the body-local output collection;                *)
(* `beforeExecutionPathCompleted` (MultiInstanceOutputCollectionBehavior)    *)
(* writes the child's evaluated output element at its index; `onComplete`   *)
(* propagates the collection to the body's flow scope.                       *)
(*                                                                           *)
(* Propagation (Zeebe VariableBehavior#mergeDocument): a job's completion    *)
(* variable is written to the NEAREST scope, from the job's own scope        *)
(* upward, that already defines the name — else to the root.                 *)
(*                                                                           *)
(* `SeedOutput` switches the outputElement seeding on/off: the shipped model *)
(* seeds (Zeebe parity, #1519); the negative model does not and MUST violate *)
(* OutputIsolation — the #1518 counterexample, proving the invariant has     *)
(* teeth.                                                                    *)
(*                                                                           *)
(* Values: child i's item is the integer i; `Null` (0) is the nil seed and  *)
(* `Undef` (-1) means "not defined in this scope". Ghost `origin` records    *)
(* which child's scope subtree produced each stored value (0 = none: a seed *)
(* placed by the engine outside any child, or a value never written).        *)
(***************************************************************************)
EXTENDS Integers, Sequences

CONSTANTS
    N,           \* multi-instance cardinality (children 1..N, all parallel)
    OutName,     \* the outputElement's variable: "result", "item" or "loopCounter"
    SeedOutput   \* TRUE = Zeebe seeding (#1519); FALSE = the #1518 defect

ASSUME N \in Nat \ {0}
ASSUME OutName \in {"result", "item", "loopCounter"}
ASSUME SeedOutput \in BOOLEAN

Root     == 0
Body     == N + 1
Children == 1..N
Scopes   == {Root, Body} \cup Children
Null     == 0
Undef    == -1
Vals     == {Undef, Null} \cup Children
\* The variable names the model tracks. `item` is the inputElement.
Names    == {"item", "loopCounter", "result", "seen"}
\* The names a job completes with.
JobNames == {"result", "seen"}

\* Zeebe's setLoopVariables seeding exception: the outputElement is seeded
\* local only when it is neither the inputElement nor `loopCounter`.
SeedsOutputLocal == SeedOutput /\ OutName \notin {"item", "loopCounter"}
\* Names seeded LOCAL on every child scope.
LocalNames == {"item", "loopCounter"} \cup (IF SeedsOutputLocal THEN {OutName} ELSE {})

Parent(s) == IF s \in Children THEN Body ELSE Root
\* Scope chain from s up to the root, innermost first.
Chain(s) == IF s \in Children THEN <<s, Body, Root>>
            ELSE IF s = Body THEN <<Body, Root>> ELSE <<Root>>

VARIABLES
    phase,      \* "init" | "running" | "done"
    cstate,     \* [Children -> {"idle","active","written","completed"}]
    sv,         \* [Scopes -> [Names -> Vals]]: per-scope local variable maps
    origin,     \* ghost [Scopes -> [Names -> 0..N]]: producing child subtree
    collected,  \* [Children -> Vals]: the body's outputCollection by index
    corigin,    \* ghost [Children -> 0..N]: origin of each collected value
    rootResults,\* outputCollection propagated to the root (all Undef until done)
    overlapped  \* ghost: some child wrote while a sibling's write was pending

vars == <<phase, cstate, sv, origin, collected, corigin, rootResults, overlapped>>

EmptyMap  == [n \in Names |-> Undef]
NoOrigin  == [n \in Names |-> 0]

\* The scope that owns name n when resolved from scope s: the innermost
\* scope on the chain defining it, else the root (read and write target).
Owner(s, n) ==
    LET c == Chain(s)
        hits == {k \in 1..Len(c) : sv[c[k]][n] # Undef}
    IN IF hits = {} THEN Root ELSE c[CHOOSE k \in hits : \A j \in hits : k <= j]

Init ==
    /\ phase = "init"
    /\ cstate = [i \in Children |-> "idle"]
    /\ sv = [s \in Scopes |-> EmptyMap]
    /\ origin = [s \in Scopes |-> NoOrigin]
    /\ collected = [i \in Children |-> Undef]
    /\ corigin = [i \in Children |-> 0]
    /\ rootResults = [i \in Children |-> Undef]
    /\ overlapped = FALSE

\* The body activates and (parallel MI) activates every child at once, seeding
\* each child's locals per setLoopVariables.
Activate ==
    /\ phase = "init"
    /\ phase' = "running"
    /\ cstate' = [i \in Children |-> "active"]
    /\ sv' = [s \in Scopes |->
                IF s \in Children
                THEN [n \in Names |->
                        CASE n = "item" -> s
                          [] n = "loopCounter" -> s
                          [] n \in LocalNames -> Null
                          [] OTHER -> Undef]
                ELSE EmptyMap]
    /\ origin' = [s \in Scopes |->
                    IF s \in Children
                    THEN [n \in Names |-> IF n \in LocalNames THEN s ELSE 0]
                    ELSE NoOrigin]
    /\ UNCHANGED <<collected, corigin, rootResults, overlapped>>

\* Child i's first job completes with {result: i, seen: i}; each name is
\* propagated from the child's scope to its owner (else root).
Write(i) ==
    /\ phase = "running"
    /\ cstate[i] = "active"
    /\ cstate' = [cstate EXCEPT ![i] = "written"]
    /\ LET tgt == [n \in JobNames |-> Owner(i, n)]
       IN /\ sv' = [s \in Scopes |-> [n \in Names |->
                      IF n \in JobNames /\ tgt[n] = s THEN i ELSE sv[s][n]]]
          /\ origin' = [s \in Scopes |-> [n \in Names |->
                      IF n \in JobNames /\ tgt[n] = s THEN i ELSE origin[s][n]]]
    /\ overlapped' = (overlapped \/ \E j \in Children \ {i} : cstate[j] = "written")
    /\ UNCHANGED <<phase, collected, corigin, rootResults>>

\* Child i's second job completes: the child completes, its outputElement is
\* evaluated in its scope and collected at its index, then the child scope is
\* torn down.
Finish(i) ==
    /\ phase = "running"
    /\ cstate[i] = "written"
    /\ cstate' = [cstate EXCEPT ![i] = "completed"]
    /\ LET o == Owner(i, OutName)
       IN /\ collected' = [collected EXCEPT ![i] =
                             IF sv[o][OutName] = Undef THEN Null ELSE sv[o][OutName]]
          /\ corigin' = [corigin EXCEPT ![i] = origin[o][OutName]]
    /\ sv' = [sv EXCEPT ![i] = EmptyMap]
    /\ origin' = [origin EXCEPT ![i] = NoOrigin]
    /\ UNCHANGED <<phase, rootResults, overlapped>>

\* All children done: the body's output mapping propagates outputCollection
\* to the root and the body scope is torn down.
Complete ==
    /\ phase = "running"
    /\ \A i \in Children : cstate[i] = "completed"
    /\ phase' = "done"
    /\ rootResults' = collected
    /\ sv' = [sv EXCEPT ![Body] = EmptyMap]
    /\ origin' = [origin EXCEPT ![Body] = NoOrigin]
    /\ UNCHANGED <<cstate, collected, corigin, overlapped>>

\* The completed instance is terminal (stutter, not a deadlock).
Done == phase = "done" /\ UNCHANGED vars

Next == Activate \/ Complete \/ Done \/ \E i \in Children : Write(i) \/ Finish(i)

Spec == Init /\ [][Next]_vars /\ WF_vars(Next)

-----------------------------------------------------------------------------
TypeOK ==
    /\ phase \in {"init", "running", "done"}
    /\ cstate \in [Children -> {"idle", "active", "written", "completed"}]
    /\ sv \in [Scopes -> [Names -> Vals]]
    /\ origin \in [Scopes -> [Names -> 0..N]]
    /\ collected \in [Children -> Vals]
    /\ corigin \in [Children -> 0..N]
    /\ rootResults \in [Children -> Vals]
    /\ overlapped \in BOOLEAN

\* outputCollection[i] derives only from child i's own scope subtree.
OutputIsolation ==
    \A i \in Children : cstate[i] = "completed" => corigin[i] = i

\* A name seeded local on a still-running child is never overwritten by a
\* parallel sibling.
NoLostUpdate ==
    \A i \in Children : cstate[i] \in {"active", "written"} =>
        \A n \in LocalNames : sv[i][n] # Undef /\ origin[i][n] = i

\* Child-local names never reach an ancestor scope (only the mapped
\* outputCollection crosses scopes, via `collected` / `rootResults`).
NoLeak ==
    \A s \in {Root, Body}, n \in LocalNames : sv[s][n] = Undef

\* Liveness: the instance always completes.
EventuallyDone == <>(phase = "done")
=============================================================================
