------------------------------ MODULE MCParallelJoinMultiArrival ------------------------------
(* GENERATED from formal/corpus/graphs/ParallelJoinMultiArrival.json by formal/corpus/generate.mjs — DO NOT EDIT BY HAND.
   Edit the graph source and re-run the generator (see formal/corpus/README.md).

   Two tokens reach a parallel join over the SAME incoming flow:
   S -> P1 -> {P2, T};  P2 -> {X, Y} -> M (xor merge) -> J;  T -> J -> E
   BPMN/Zeebe: J waits until T has also arrived.
*)
EXTENDS TokenFlow

MCNodes == {"S", "P1", "P2", "X", "Y", "M", "T", "J", "E"}
MCKind   == [n \in MCNodes |->
              CASE n = "S" -> "start"
                [] n = "E" -> "end"
                [] n \in {"P1", "P2", "J"} -> "and"
                [] n = "M" -> "xor"
                [] OTHER -> "task"]
MCStart  == "S"
MCEdges  == [f1 |-> <<"S", "P1">>,
             f2 |-> <<"P1", "P2">>,
             f3 |-> <<"P1", "T">>,
             f4 |-> <<"P2", "X">>,
             f5 |-> <<"P2", "Y">>,
             f6 |-> <<"X", "M">>,
             f7 |-> <<"Y", "M">>,
             f8 |-> <<"M", "J">>,
             f9 |-> <<"T", "J">>,
             f10 |-> <<"J", "E">>]
MCFlows  == DOMAIN MCEdges
MCSrc    == [f \in MCFlows |-> MCEdges[f][1]]
MCTgt    == [f \in MCFlows |-> MCEdges[f][2]]
=============================================================================
