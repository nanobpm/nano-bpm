------------------------------ MODULE ZMCParallelJoinSurplus ------------------------------
(* GENERATED from formal/corpus/graphs/ParallelJoinSurplus.json by formal/corpus/generate.mjs — DO NOT EDIT BY HAND.
   Edit the graph source and re-run the generator (see formal/corpus/README.md).

   Zeebe reference (#1240, slice 1) of the "Tetris" surplus principle: every
   incoming flow of the parallel join J is taken twice, so J fires twice,
   keeping the surplus taken record between firings
   (`cleanupSequenceFlowsTaken` consumes exactly one per flow). This is a
   not-1-safe graph (BPMN lack of synchronization), and re-firing is the
   correct verdict (`JoinFiresAtMostOnce` violated). nano's
   `MCParallelJoinSurplus` must match.
*)
EXTENDS ZeebeTokenFlow

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
