------------------------------ MODULE VS_MISeeded ------------------------------
(* Shipped model: two parallel MI children, outputElement "=result" seeded  *)
(* local per child (Zeebe setLoopVariables parity, #1519). Must PASS.      *)
EXTENDS VariableScopes

MCN          == 2
MCOutName    == "result"
MCSeedOutput == TRUE
=============================================================================
