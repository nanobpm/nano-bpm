/-!
# `Roundtrip.Registry` — the supported element-kind registry (mirror of `processos`)

This is a faithful mirror of the Rust single-source-of-truth
`ELEMENT_KIND_SPECS` in `processos/src/ir_spec.rs` — every element kind the
reversible IR (ADR 0001) supports, its IR keyword, and its declared attribute
schema (key, required?, value type).

**Anti-drift.** This file is *data*, deliberately kept dumb so it can be pinned
to the real Rust registry from both sides:

* the Rust test `lean_registry_matches_specs` (in `processos/src/ir_spec.rs`)
  parses this file's `registry` list and asserts it equals `ELEMENT_KIND_SPECS`
  keyword-for-keyword and attr-for-attr — so a change to the Rust registry that
  forgets this file fails the `processos (clippy + test)` CI job; and
* `Roundtrip.Ir` proves (by `rfl`, at `lake build` time) that its executable
  `Kind` model projects back onto this exact list — so a change to this file
  that forgets the model fails the `formal` CI job.

Together those two guards make the Lean round-trip model a checked derivation of
the live `processos` registry rather than an independent copy that can silently
rot.
-/

namespace Roundtrip

/-- The value type of an IR attribute — mirror of `processos` `AttrType`. -/
inductive AttrTy where
  /-- A double-quoted, escaped string (`"hello"`). -/
  | str
  /-- A bare identifier used for cross-element references (`attachedTo review`). -/
  | id
  /-- A non-negative integer with an `ms` suffix (`5000ms`). -/
  | duration
  /-- The literal `true` or `false`. -/
  | bool
  deriving DecidableEq, Repr, Inhabited

/-- One attribute an element kind may carry — mirror of `processos` `AttrSpec`
(without the human-facing `doc`, which is irrelevant to structural parity). -/
structure AttrDecl where
  /-- The IR attribute keyword (`jobType`, `attachedTo`, …). -/
  key : String
  /-- `true` when the attribute must be present (a non-`Option` engine field). -/
  required : Bool
  /-- The attribute's value type. -/
  ty : AttrTy
  deriving DecidableEq, Repr, Inhabited

/-- One element kind, its IR keyword, and its attribute schema — mirror of
`processos` `KindSpec` (again minus `doc`). -/
structure KindSpec where
  /-- The IR keyword (`serviceTask`, `exclusiveGateway`, …). -/
  keyword : String
  /-- The kind-specific attribute declarations, in registry order. -/
  attrs : List AttrDecl
  deriving DecidableEq, Repr, Inhabited

/-- The whole supported possibility space — every element kind in the exact
order of `processos`' `ELEMENT_KIND_SPECS`. -/
def registry : List KindSpec :=
  [ { keyword := "startEvent", attrs := [] }
  , { keyword := "endEvent", attrs := [] }
  , { keyword := "terminateEndEvent", attrs := [] }
  , { keyword := "serviceTask"
    , attrs :=
        [ { key := "jobType", required := true, ty := .str }
        , { key := "priority", required := false, ty := .str }
        , { key := "agentType", required := false, ty := .str } ] }
  , { keyword := "businessRuleTask"
    , attrs :=
        [ { key := "decisionId", required := true, ty := .str }
        , { key := "resultVariable", required := false, ty := .str } ] }
  , { keyword := "userTask"
    , attrs :=
        [ { key := "assignee", required := false, ty := .str }
        , { key := "candidateGroups", required := false, ty := .str }
        , { key := "candidateUsers", required := false, ty := .str }
        , { key := "dueDate", required := false, ty := .str }
        , { key := "followUpDate", required := false, ty := .str }
        , { key := "priority", required := false, ty := .str }
        , { key := "formId", required := false, ty := .str }
        , { key := "externalFormReference", required := false, ty := .str } ] }
  , { keyword := "exclusiveGateway", attrs := [] }
  , { keyword := "parallelGateway", attrs := [] }
  , { keyword := "inclusiveGateway", attrs := [] }
  , { keyword := "eventBasedGateway", attrs := [] }
  , { keyword := "errorBoundaryEvent"
    , attrs :=
        [ { key := "attachedTo", required := true, ty := .id }
        , { key := "errorCode", required := true, ty := .str } ] }
  , { keyword := "timerIntermediateCatchEvent"
    , attrs := [ { key := "duration", required := true, ty := .duration } ] }
  , { keyword := "timerBoundaryEvent"
    , attrs :=
        [ { key := "attachedTo", required := true, ty := .id }
        , { key := "duration", required := true, ty := .duration }
        , { key := "interrupting", required := true, ty := .bool }
        , { key := "repeating", required := true, ty := .bool } ] }
  , { keyword := "messageIntermediateCatchEvent"
    , attrs :=
        [ { key := "message", required := true, ty := .str }
        , { key := "correlationKey", required := true, ty := .str } ] }
  , { keyword := "messageBoundaryEvent"
    , attrs :=
        [ { key := "attachedTo", required := true, ty := .id }
        , { key := "message", required := true, ty := .str }
        , { key := "correlationKey", required := true, ty := .str }
        , { key := "interrupting", required := true, ty := .bool } ] }
  , { keyword := "messageStartEvent"
    , attrs := [ { key := "message", required := true, ty := .str } ] }
  , { keyword := "timerStartEvent"
    , attrs :=
        [ { key := "interval", required := true, ty := .duration }
        , { key := "repeating", required := true, ty := .bool } ] }
  , { keyword := "subProcess"
    , attrs := [ { key := "startEvent", required := true, ty := .id } ] }
  , { keyword := "eventSubProcess"
    , attrs :=
        [ { key := "startEvent", required := true, ty := .id }
        , { key := "trigger", required := true, ty := .str }
        , { key := "interrupting", required := true, ty := .bool } ] }
  , { keyword := "escalationThrowEvent"
    , attrs := [ { key := "escalationCode", required := false, ty := .str } ] }
  , { keyword := "escalationBoundaryEvent"
    , attrs :=
        [ { key := "attachedTo", required := true, ty := .id }
        , { key := "escalationCode", required := false, ty := .str }
        , { key := "interrupting", required := true, ty := .bool } ] }
  , { keyword := "intermediateThrowEvent", attrs := [] }
  , { keyword := "linkIntermediateThrowEvent"
    , attrs := [ { key := "link", required := true, ty := .str } ] }
  , { keyword := "linkIntermediateCatchEvent"
    , attrs := [ { key := "link", required := true, ty := .str } ] }
  , { keyword := "task", attrs := [] }
  , { keyword := "scriptTask"
    , attrs :=
        [ { key := "expression", required := true, ty := .str }
        , { key := "resultVariable", required := true, ty := .str } ] }
  , { keyword := "callActivity"
    , attrs :=
        [ { key := "calledElement", required := true, ty := .str }
        , { key := "propagateAllParentVariables", required := true, ty := .bool }
        , { key := "propagateAllChildVariables", required := true, ty := .bool } ] }
  , { keyword := "signalIntermediateCatchEvent"
    , attrs := [ { key := "signal", required := true, ty := .str } ] }
  , { keyword := "signalBoundaryEvent"
    , attrs :=
        [ { key := "attachedTo", required := true, ty := .id }
        , { key := "signal", required := true, ty := .str }
        , { key := "interrupting", required := true, ty := .bool } ] }
  , { keyword := "conditionalIntermediateCatchEvent"
    , attrs := [ { key := "condition", required := true, ty := .str } ] }
  , { keyword := "conditionalBoundaryEvent"
    , attrs :=
        [ { key := "attachedTo", required := true, ty := .id }
        , { key := "condition", required := true, ty := .str }
        , { key := "interrupting", required := true, ty := .bool } ] }
  , { keyword := "compensationBoundaryEvent"
    , attrs :=
        [ { key := "attachedTo", required := true, ty := .id }
        , { key := "handler", required := true, ty := .id } ] }
  , { keyword := "compensationThrowEvent", attrs := [] }
  ]

/-- Just the keywords of `registry`, in order — the supported element-kind set. -/
def registryKeywords : List String := registry.map (·.keyword)

end Roundtrip
