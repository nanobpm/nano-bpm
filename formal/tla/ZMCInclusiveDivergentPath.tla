------------------------------ MODULE ZMCInclusiveDivergentPath ------------------------------
(* GENERATED from formal/corpus/graphs/InclusiveDivergentPath.json by formal/corpus/generate.mjs — DO NOT EDIT BY HAND.
   Edit the graph source and re-run the generator (see formal/corpus/README.md).

   Zeebe reference (#1240, slice 1) of the arrival-time inclusive-join guard
   (#1241). S -> I(or) -> {A, B}; A -> J(or); B -> X(xor) -> {J, E2}; J -> E.
   When A arrives while B is live, J waits; when X then routes to E2, no token
   reaches J again, so — as Zeebe never re-evaluates a join on unrelated
   progress (`canActivateInclusiveGateway` only runs on arrival) — J holds A's
   token forever. nano's `MCInclusiveDivergentPath` must reproduce this stuck
   verdict (`NoStuckInstance,Termination`).
*)
EXTENDS ZeebeTokenFlow

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
