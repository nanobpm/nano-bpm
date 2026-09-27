------------------------------ MODULE MCInclusiveInParallel ------------------------------
(* GENERATED from formal/corpus/graphs/InclusiveInParallel.json by formal/corpus/generate.mjs — DO NOT EDIT BY HAND.
   Edit the graph source and re-run the generator (see formal/corpus/README.md).

   An inclusive diamond on one branch of a parallel diamond:
   S -> P1 -> {X, B};  X -> I1 -> {A1, A2} -> I2 -> P2;  B -> P2 -> E
*)
EXTENDS TokenFlow

MCNodes == {"S", "P1", "X", "I1", "A1", "A2", "I2", "B", "P2", "E"}
MCKind   == [n \in MCNodes |->
              CASE n = "S" -> "start"
                [] n = "E" -> "end"
                [] n \in {"P1", "P2"} -> "and"
                [] n \in {"I1", "I2"} -> "or"
                [] OTHER -> "task"]
MCStart  == "S"
MCEdges  == [f1 |-> <<"S", "P1">>,
             f2 |-> <<"P1", "X">>,
             f3 |-> <<"P1", "B">>,
             f4 |-> <<"X", "I1">>,
             f5 |-> <<"I1", "A1">>,
             f6 |-> <<"I1", "A2">>,
             f7 |-> <<"A1", "I2">>,
             f8 |-> <<"A2", "I2">>,
             f9 |-> <<"I2", "P2">>,
             f10 |-> <<"B", "P2">>,
             f11 |-> <<"P2", "E">>]
MCFlows  == DOMAIN MCEdges
MCSrc    == [f \in MCFlows |-> MCEdges[f][1]]
MCTgt    == [f \in MCFlows |-> MCEdges[f][2]]
=============================================================================
