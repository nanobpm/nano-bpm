------------------------------ MODULE MCInclusiveDiamond ------------------------------
(* GENERATED from formal/corpus/graphs/InclusiveDiamond.json by formal/corpus/generate.mjs — DO NOT EDIT BY HAND.
   Edit the graph source and re-run the generator (see formal/corpus/README.md).

   S -> I1 -> some non-empty subset of {A, B, C} -> I2 -> E
*)
EXTENDS TokenFlow

MCNodes == {"S", "I1", "A", "B", "C", "I2", "E"}
MCKind   == [n \in MCNodes |->
              CASE n = "S" -> "start"
                [] n = "E" -> "end"
                [] n \in {"I1", "I2"} -> "or"
                [] OTHER -> "task"]
MCStart  == "S"
MCEdges  == [f1 |-> <<"S", "I1">>,
             f2 |-> <<"I1", "A">>,
             f3 |-> <<"I1", "B">>,
             f4 |-> <<"I1", "C">>,
             f5 |-> <<"A", "I2">>,
             f6 |-> <<"B", "I2">>,
             f7 |-> <<"C", "I2">>,
             f8 |-> <<"I2", "E">>]
MCFlows  == DOMAIN MCEdges
MCSrc    == [f \in MCFlows |-> MCEdges[f][1]]
MCTgt    == [f \in MCFlows |-> MCEdges[f][2]]
=============================================================================
