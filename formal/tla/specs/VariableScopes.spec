# Spec descriptor for the VariableScopes family (#1522, epic #1224): variable
# scope isolation under parallelism — a parallel multi-instance sub-process's
# per-child scopes (inputElement / loopCounter / outputElement seeding per
# Zeebe's MultiInstanceBodyProcessor#setLoopVariables at the pinned sha),
# nearest-scope propagation, outputCollection collection and scope teardown.
# Registered through the multi-spec harness (#1226); see formal/README.md
# ("Adding a spec family") and specs/TokenFlow.spec for the contract.
#
# Engine anchor: the SPEC_TRACE_MODELS below are dumped by gen-traces.sh (the
# TLC witness of the shortest completing behaviour in which sibling children
# interleave — SPEC_TRACE_DONE requires the `overlapped` ghost) into
# traces/VariableScopes/<Model>.json via trace/parse.mjs's VariableScopes
# vocabulary, and replayed step by step against engine-core by
# engine-core/tests/variable_scopes_trace_validation.rs, which asserts the
# engine's per-scope variable state equals the spec's after every step. The
# negative model is not trace-anchored: it models the #1518 defect, which the
# engine no longer has.

SPEC_NAME="VariableScopes"
SPEC_MODELS_DIR="varscopes"
SPEC_MODELS_GLOB="VS_*.tla"

SPEC_CONSTANTS=(
  "N          <- MCN"
  "OutName    <- MCOutName"
  "SeedOutput <- MCSeedOutput"
)

# TypeOK — the state stays well-typed.
# OutputIsolation — outputCollection[i] derives only from writes in child i's
#   scope subtree.
# NoLostUpdate — a name seeded local on a running child is never overwritten by
#   a parallel sibling.
# NoLeak — child-local names never reach the body or root scope (only the
#   mapped outputCollection crosses scopes).
SPEC_INVARIANTS=(TypeOK OutputIsolation NoLostUpdate NoLeak)
# EventuallyDone — the instance always completes (under WF).
SPEC_PROPERTIES=(EventuallyDone)

SPEC_EXPECTED=(
  "VS_MISeeded            pass"
  "VS_MIOutIsInput        pass"
  "VS_MIOutIsLoopCounter  pass"
  # Negative model reproducing the #1518 defect (no outputElement seeding): a
  # sibling's write reaches the shared root `result` before collection. Proves
  # OutputIsolation has teeth; the shipped VS_MISeeded must pass.
  "VS_MINoSeed            violates:OutputIsolation"
)

SPEC_TRACE_MODELS=(VS_MISeeded VS_MIOutIsInput VS_MIOutIsLoopCounter)
SPEC_TRACE_DONE='phase = "done" /\ overlapped'
SPEC_TRACE_GRAPH='[n |-> MCN, outputElement |-> MCOutName, seedOutput |-> MCSeedOutput]'
