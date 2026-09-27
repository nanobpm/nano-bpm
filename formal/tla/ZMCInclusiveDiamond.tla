------------------------------ MODULE ZMCInclusiveDiamond ------------------------------
(* GENERATED from formal/corpus/graphs/InclusiveDiamond.json by formal/corpus/generate.mjs — DO NOT EDIT BY HAND.
   Edit the graph source and re-run the generator (see formal/corpus/README.md).

   Zeebe reference (#1240, slice 1) of the inclusive diamond: an inclusive
   split takes some non-empty subset of {A,B,C}, an inclusive join
   synchronises exactly those. `canActivateInclusiveGateway` fires once every
   activated branch has delivered and no live path remains — nano's
   `MCInclusiveDiamond` must match.
*)
EXTENDS ZeebeTokenFlow

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
