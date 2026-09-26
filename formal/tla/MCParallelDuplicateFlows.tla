------------------------------ MODULE MCParallelDuplicateFlows ------------------------------
(* GENERATED from formal/corpus/graphs/ParallelDuplicateFlows.json by formal/corpus/generate.mjs — DO NOT EDIT BY HAND.
   Edit the graph source and re-run the generator (see formal/corpus/README.md).

   Two DISTINCT sequence flows share endpoints (fork -> join twice), plus a
   third branch through a task:  S -> P1 =(d1, d2)=> P2;  P1 -> B -> P2 -> E.
   P2 has three incoming flows, so it must wait for B.
*)
EXTENDS TokenFlow

MCNodes == {"S", "P1", "B", "P2", "E"}
MCKind   == [n \in MCNodes |->
              CASE n = "S" -> "start"
                [] n = "E" -> "end"
                [] n \in {"P1", "P2"} -> "and"
                [] OTHER -> "task"]
MCStart  == "S"
MCEdges  == [f1 |-> <<"S", "P1">>,
             f2 |-> <<"P1", "P2">>,
             f3 |-> <<"P1", "P2">>,
             f4 |-> <<"P1", "B">>,
             f5 |-> <<"B", "P2">>,
             f6 |-> <<"P2", "E">>]
MCFlows  == DOMAIN MCEdges
MCSrc    == [f \in MCFlows |-> MCEdges[f][1]]
MCTgt    == [f \in MCFlows |-> MCEdges[f][2]]
=============================================================================
