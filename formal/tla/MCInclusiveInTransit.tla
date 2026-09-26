------------------------------ MODULE MCInclusiveInTransit ------------------------------
(* GENERATED from formal/corpus/graphs/InclusiveInTransit.json by formal/corpus/generate.mjs — DO NOT EDIT BY HAND.
   Edit the graph source and re-run the generator (see formal/corpus/README.md).

   Sibling tokens still in transit toward an inclusive join:
   S -> I (inclusive split) -> {J, X, T};  X (xor) -> J;  T -> J;  J -> E
   When I takes I -> J and I -> X in one command, J's first arrival must wait
   for the token still queued on its way through X: an in-transit activation
   is a live source, like Zeebe's `activeSequenceFlowIds` (#1241). Firing
   early would fire J a second time on X's token.
*)
EXTENDS TokenFlow

MCNodes == {"S", "I", "X", "T", "J", "E"}
MCKind   == [n \in MCNodes |->
              CASE n = "S" -> "start"
                [] n = "E" -> "end"
                [] n \in {"I", "J"} -> "or"
                [] n = "X" -> "xor"
                [] OTHER -> "task"]
MCStart  == "S"
MCEdges  == [f1 |-> <<"S", "I">>,
             f2 |-> <<"I", "J">>,
             f3 |-> <<"I", "X">>,
             f4 |-> <<"I", "T">>,
             f5 |-> <<"X", "J">>,
             f6 |-> <<"T", "J">>,
             f7 |-> <<"J", "E">>]
MCFlows  == DOMAIN MCEdges
MCSrc    == [f \in MCFlows |-> MCEdges[f][1]]
MCTgt    == [f \in MCFlows |-> MCEdges[f][2]]
=============================================================================
