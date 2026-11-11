------------------------------ MODULE VS_MINoSeed ------------------------------
(* NEGATIVE model: no outputElement seeding (the #1518 engine defect). A    *)
(* sibling write overwrites the shared root `result` before collection,   *)
(* so this MUST violate OutputIsolation, proving the invariant has teeth. *)
EXTENDS VariableScopes

MCN          == 2
MCOutName    == "result"
MCSeedOutput == FALSE
=============================================================================
