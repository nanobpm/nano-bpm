------------------------------ MODULE MCInclusiveDivergentPath ------------------------------
(* GENERATED from formal/corpus/graphs/InclusiveDivergentPath.json by formal/corpus/generate.mjs — DO NOT EDIT BY HAND.
   Edit the graph source and re-run the generator (see formal/corpus/README.md).

   An inclusive join whose competing path can diverge elsewhere:
   S -> I (inclusive split) -> {A, B};  A -> J;  B -> X (xor) -> {J, E2};  J -> E
   If A arrives while B is live, J waits for B. When X then routes to E2, no
   token arrives at J again, so, exactly as in Zeebe, J is never re-evaluated
   and holds A's token forever (#1241).
*)
EXTENDS TokenFlow

MCNodes == {"S", "I", "A", "B", "X", "J", "E", "E2"}
MCKind   == [n \in MCNodes |->
              CASE n = "S" -> "start"
                [] n \in {"E", "E2"} -> "end"
                [] n \in {"I", "J"} -> "or"
                [] n = "X" -> "xor"
                [] OTHER -> "task"]
MCStart  == "S"
MCEdges  == [f1 |-> <<"S", "I">>,
             f2 |-> <<"I", "A">>,
             f3 |-> <<"I", "B">>,
             f4 |-> <<"A", "J">>,
             f5 |-> <<"B", "X">>,
             f6 |-> <<"X", "J">>,
             f7 |-> <<"X", "E2">>,
             f8 |-> <<"J", "E">>]
MCFlows  == DOMAIN MCEdges
MCSrc    == [f \in MCFlows |-> MCEdges[f][1]]
MCTgt    == [f \in MCFlows |-> MCEdges[f][2]]
=============================================================================
