------------------------------ MODULE MCParallelJoinSurplus ------------------------------
(* GENERATED from formal/corpus/graphs/ParallelJoinSurplus.json by formal/corpus/generate.mjs — DO NOT EDIT BY HAND.
   Edit the graph source and re-run the generator (see formal/corpus/README.md).

   Every incoming flow of a parallel join is taken twice:
   S -> F -> {PA, PB};  PA -> {A1, A2} -> XA (xor merge) -> J;
                        PB -> {B1, B2} -> XB (xor merge) -> J;  J -> E
   Zeebe keeps the surplus ("Tetris" principle): J fires once per complete set
   of incoming tokens, so it fires twice and the instance completes (#1233).
*)
EXTENDS TokenFlow

MCNodes == {"S", "F", "PA", "PB", "A1", "A2", "B1", "B2", "XA", "XB", "J", "E"}
MCKind   == [n \in MCNodes |->
              CASE n = "S" -> "start"
                [] n = "E" -> "end"
                [] n \in {"F", "PA", "PB", "J"} -> "and"
                [] n \in {"XA", "XB"} -> "xor"
                [] OTHER -> "task"]
MCStart  == "S"
MCEdges  == [f1 |-> <<"S", "F">>,
             f2 |-> <<"F", "PA">>,
             f3 |-> <<"F", "PB">>,
             f4 |-> <<"PA", "A1">>,
             f5 |-> <<"PA", "A2">>,
             f6 |-> <<"PB", "B1">>,
             f7 |-> <<"PB", "B2">>,
             f8 |-> <<"A1", "XA">>,
             f9 |-> <<"A2", "XA">>,
             f10 |-> <<"B1", "XB">>,
             f11 |-> <<"B2", "XB">>,
             f12 |-> <<"XA", "J">>,
             f13 |-> <<"XB", "J">>,
             f14 |-> <<"J", "E">>]
MCFlows  == DOMAIN MCEdges
MCSrc    == [f \in MCFlows |-> MCEdges[f][1]]
MCTgt    == [f \in MCFlows |-> MCEdges[f][2]]
=============================================================================
