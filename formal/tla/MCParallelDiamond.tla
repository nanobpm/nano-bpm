------------------------------ MODULE MCParallelDiamond ------------------------------
(* GENERATED from formal/corpus/graphs/ParallelDiamond.json by formal/corpus/generate.mjs — DO NOT EDIT BY HAND.
   Edit the graph source and re-run the generator (see formal/corpus/README.md).

   S -> P1 -> {A, B} -> P2 -> E
*)
EXTENDS TokenFlow

MCNodes == {"S", "P1", "A", "B", "P2", "E"}
MCKind   == [n \in MCNodes |->
              CASE n = "S" -> "start"
                [] n = "E" -> "end"
                [] n \in {"P1", "P2"} -> "and"
                [] OTHER -> "task"]
MCStart  == "S"
MCEdges  == [f1 |-> <<"S", "P1">>,
             f2 |-> <<"P1", "A">>,
             f3 |-> <<"P1", "B">>,
             f4 |-> <<"A", "P2">>,
             f5 |-> <<"B", "P2">>,
             f6 |-> <<"P2", "E">>]
MCFlows  == DOMAIN MCEdges
MCSrc    == [f \in MCFlows |-> MCEdges[f][1]]
MCTgt    == [f \in MCFlows |-> MCEdges[f][2]]
=============================================================================
