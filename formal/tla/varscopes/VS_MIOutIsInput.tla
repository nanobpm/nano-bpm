------------------------------ MODULE VS_MIOutIsInput ------------------------------
(* outputElement "=item" names the inputElement: Zeebe skips the nil seed  *)
(* (already local) and the job result propagates to the root. PASS.        *)
EXTENDS VariableScopes

MCN          == 2
MCOutName    == "item"
MCSeedOutput == TRUE
=============================================================================
