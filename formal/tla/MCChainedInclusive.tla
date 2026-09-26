------------------------------ MODULE MCChainedInclusive ------------------------------
(* GENERATED from formal/corpus/graphs/ChainedInclusive.json by formal/corpus/generate.mjs — DO NOT EDIT BY HAND.
   Edit the graph source and re-run the generator (see formal/corpus/README.md).

   Chained inclusive joins (J2 must wait while J1 can still reach it):
   S -> I1 -> {A, B, C};  A, B -> J1;  J1, C -> J2 -> E
*)
EXTENDS TokenFlow

MCNodes == {"S", "I1", "A", "B", "C", "J1", "J2", "E"}
MCKind   == [n \in MCNodes |->
              CASE n = "S" -> "start"
                [] n = "E" -> "end"
                [] n \in {"I1", "J1", "J2"} -> "or"
                [] OTHER -> "task"]
MCStart  == "S"
MCEdges  == [f1 |-> <<"S", "I1">>,
             f2 |-> <<"I1", "A">>,
             f3 |-> <<"I1", "B">>,
             f4 |-> <<"I1", "C">>,
             f5 |-> <<"A", "J1">>,
             f6 |-> <<"B", "J1">>,
             f7 |-> <<"J1", "J2">>,
             f8 |-> <<"C", "J2">>,
             f9 |-> <<"J2", "E">>]
MCFlows  == DOMAIN MCEdges
MCSrc    == [f \in MCFlows |-> MCEdges[f][1]]
MCTgt    == [f \in MCFlows |-> MCEdges[f][2]]
=============================================================================
