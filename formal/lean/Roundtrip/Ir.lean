import Roundtrip.Registry

/-!
# `Roundtrip.Ir` — the executable IR element model

`processos`' reversible IR (ADR 0001) projects an engine `ProcessDefinition`
onto a compact textual notation: each element renders as a keyword followed by
its typed attributes (`processos/src/model_ir.rs`). This module models that IR
as an *executable, closed* datatype:

* `Kind` — one constructor per supported element kind (the closed possibility
  space);
* `Kind.keyword` / `Kind.schema` — the IR keyword and attribute schema each kind
  renders to, mirroring `kind_keyword` / `render_kind_attrs`;
* `AttrVal` — a typed IR attribute value (`AttrType` on the Rust side);
* `IrElement` — a rendered element: its kind, id, and attribute values.

The `Kind`/`schema` model is pinned to the `Roundtrip.Registry` mirror of the
Rust `ELEMENT_KIND_SPECS` by the two `theorem`s at the bottom, both discharged
by `rfl` at `lake build` time. Because `Registry` is in turn pinned to the live
Rust registry by a `processos` test, `Kind` cannot drift from the engine's real
element set without failing CI.
-/

namespace Roundtrip

/-- A typed IR attribute value — the inhabitant of an `AttrTy`. -/
inductive AttrVal where
  /-- A string value. -/
  | str (s : String)
  /-- An identifier value (cross-element reference). -/
  | id (s : String)
  /-- A millisecond duration/interval. -/
  | duration (ms : Nat)
  /-- A boolean value. -/
  | bool (b : Bool)
  deriving DecidableEq, Repr, Inhabited

/-- The closed set of supported element kinds — one constructor per
`ELEMENT_KIND_SPECS` entry, in registry order. -/
inductive Kind where
  | startEvent
  | endEvent
  | terminateEndEvent
  | serviceTask
  | businessRuleTask
  | userTask
  | exclusiveGateway
  | parallelGateway
  | inclusiveGateway
  | eventBasedGateway
  | errorBoundaryEvent
  | timerIntermediateCatchEvent
  | timerBoundaryEvent
  | messageIntermediateCatchEvent
  | messageBoundaryEvent
  | messageStartEvent
  | timerStartEvent
  | subProcess
  | eventSubProcess
  | escalationThrowEvent
  | escalationBoundaryEvent
  | intermediateThrowEvent
  | linkIntermediateThrowEvent
  | linkIntermediateCatchEvent
  | task
  | scriptTask
  | callActivity
  | signalIntermediateCatchEvent
  | signalBoundaryEvent
  | conditionalIntermediateCatchEvent
  | conditionalBoundaryEvent
  | compensationBoundaryEvent
  | compensationThrowEvent
  deriving DecidableEq, Repr, Inhabited

/-- Every `Kind`, in registry order. The `kinds_complete` theorem certifies this
list omits nothing. -/
def Kind.all : List Kind :=
  [ .startEvent, .endEvent, .terminateEndEvent, .serviceTask, .businessRuleTask
  , .userTask, .exclusiveGateway, .parallelGateway, .inclusiveGateway
  , .eventBasedGateway, .errorBoundaryEvent, .timerIntermediateCatchEvent
  , .timerBoundaryEvent, .messageIntermediateCatchEvent, .messageBoundaryEvent
  , .messageStartEvent, .timerStartEvent, .subProcess, .eventSubProcess
  , .escalationThrowEvent
  , .escalationBoundaryEvent, .intermediateThrowEvent, .linkIntermediateThrowEvent
  , .linkIntermediateCatchEvent, .task, .scriptTask, .callActivity
  , .signalIntermediateCatchEvent, .signalBoundaryEvent
  , .conditionalIntermediateCatchEvent, .conditionalBoundaryEvent
  , .compensationBoundaryEvent, .compensationThrowEvent ]

/-- The IR keyword each kind renders to — mirror of `model_ir::kind_keyword`. -/
def Kind.keyword : Kind → String
  | .startEvent => "startEvent"
  | .endEvent => "endEvent"
  | .terminateEndEvent => "terminateEndEvent"
  | .serviceTask => "serviceTask"
  | .businessRuleTask => "businessRuleTask"
  | .userTask => "userTask"
  | .exclusiveGateway => "exclusiveGateway"
  | .parallelGateway => "parallelGateway"
  | .inclusiveGateway => "inclusiveGateway"
  | .eventBasedGateway => "eventBasedGateway"
  | .errorBoundaryEvent => "errorBoundaryEvent"
  | .timerIntermediateCatchEvent => "timerIntermediateCatchEvent"
  | .timerBoundaryEvent => "timerBoundaryEvent"
  | .messageIntermediateCatchEvent => "messageIntermediateCatchEvent"
  | .messageBoundaryEvent => "messageBoundaryEvent"
  | .messageStartEvent => "messageStartEvent"
  | .timerStartEvent => "timerStartEvent"
  | .subProcess => "subProcess"
  | .eventSubProcess => "eventSubProcess"
  | .escalationThrowEvent => "escalationThrowEvent"
  | .escalationBoundaryEvent => "escalationBoundaryEvent"
  | .intermediateThrowEvent => "intermediateThrowEvent"
  | .linkIntermediateThrowEvent => "linkIntermediateThrowEvent"
  | .linkIntermediateCatchEvent => "linkIntermediateCatchEvent"
  | .task => "task"
  | .scriptTask => "scriptTask"
  | .callActivity => "callActivity"
  | .signalIntermediateCatchEvent => "signalIntermediateCatchEvent"
  | .signalBoundaryEvent => "signalBoundaryEvent"
  | .conditionalIntermediateCatchEvent => "conditionalIntermediateCatchEvent"
  | .conditionalBoundaryEvent => "conditionalBoundaryEvent"
  | .compensationBoundaryEvent => "compensationBoundaryEvent"
  | .compensationThrowEvent => "compensationThrowEvent"

/-- The attribute schema each kind renders — mirror of the per-kind `attrs` in
`ELEMENT_KIND_SPECS` / `render_kind_attrs`. Authored as an explicit match (kept
in lock-step with `registry` by the `specs_match_registry` `rfl` theorem below,
which fails the build on any divergence). -/
def Kind.schema : Kind → List AttrDecl
  | .startEvent => []
  | .endEvent => []
  | .terminateEndEvent => []
  | .serviceTask =>
      [ { key := "jobType", required := true, ty := .str }
      , { key := "priority", required := false, ty := .str }
      , { key := "agentType", required := false, ty := .str } ]
  | .businessRuleTask =>
      [ { key := "decisionId", required := true, ty := .str }
      , { key := "resultVariable", required := false, ty := .str } ]
  | .userTask =>
      [ { key := "assignee", required := false, ty := .str }
      , { key := "candidateGroups", required := false, ty := .str }
      , { key := "candidateUsers", required := false, ty := .str }
      , { key := "dueDate", required := false, ty := .str }
      , { key := "followUpDate", required := false, ty := .str }
      , { key := "priority", required := false, ty := .str }
      , { key := "formId", required := false, ty := .str }
      , { key := "externalFormReference", required := false, ty := .str } ]
  | .exclusiveGateway => []
  | .parallelGateway => []
  | .inclusiveGateway => []
  | .eventBasedGateway => []
  | .errorBoundaryEvent =>
      [ { key := "attachedTo", required := true, ty := .id }
      , { key := "errorCode", required := true, ty := .str } ]
  | .timerIntermediateCatchEvent =>
      [ { key := "duration", required := true, ty := .duration } ]
  | .timerBoundaryEvent =>
      [ { key := "attachedTo", required := true, ty := .id }
      , { key := "duration", required := true, ty := .duration }
      , { key := "interrupting", required := true, ty := .bool }
      , { key := "repeating", required := true, ty := .bool } ]
  | .messageIntermediateCatchEvent =>
      [ { key := "message", required := true, ty := .str }
      , { key := "correlationKey", required := true, ty := .str } ]
  | .messageBoundaryEvent =>
      [ { key := "attachedTo", required := true, ty := .id }
      , { key := "message", required := true, ty := .str }
      , { key := "correlationKey", required := true, ty := .str }
      , { key := "interrupting", required := true, ty := .bool } ]
  | .messageStartEvent =>
      [ { key := "message", required := true, ty := .str } ]
  | .timerStartEvent =>
      [ { key := "interval", required := true, ty := .duration }
      , { key := "repeating", required := true, ty := .bool } ]
  | .subProcess =>
      [ { key := "startEvent", required := true, ty := .id } ]
  | .eventSubProcess =>
      [ { key := "startEvent", required := true, ty := .id }
      , { key := "trigger", required := true, ty := .str }
      , { key := "interrupting", required := true, ty := .bool } ]
  | .escalationThrowEvent =>
      [ { key := "escalationCode", required := false, ty := .str } ]
  | .escalationBoundaryEvent =>
      [ { key := "attachedTo", required := true, ty := .id }
      , { key := "escalationCode", required := false, ty := .str }
      , { key := "interrupting", required := true, ty := .bool } ]
  | .intermediateThrowEvent => []
  | .linkIntermediateThrowEvent =>
      [ { key := "link", required := true, ty := .str } ]
  | .linkIntermediateCatchEvent =>
      [ { key := "link", required := true, ty := .str } ]
  | .task => []
  | .scriptTask =>
      [ { key := "expression", required := true, ty := .str }
      , { key := "resultVariable", required := true, ty := .str } ]
  | .callActivity =>
      [ { key := "calledElement", required := true, ty := .str }
      , { key := "propagateAllParentVariables", required := true, ty := .bool }
      , { key := "propagateAllChildVariables", required := true, ty := .bool } ]
  | .signalIntermediateCatchEvent =>
      [ { key := "signal", required := true, ty := .str } ]
  | .signalBoundaryEvent =>
      [ { key := "attachedTo", required := true, ty := .id }
      , { key := "signal", required := true, ty := .str }
      , { key := "interrupting", required := true, ty := .bool } ]
  | .conditionalIntermediateCatchEvent =>
      [ { key := "condition", required := true, ty := .str } ]
  | .conditionalBoundaryEvent =>
      [ { key := "attachedTo", required := true, ty := .id }
      , { key := "condition", required := true, ty := .str }
      , { key := "interrupting", required := true, ty := .bool } ]
  | .compensationBoundaryEvent =>
      [ { key := "attachedTo", required := true, ty := .id }
      , { key := "handler", required := true, ty := .id } ]
  | .compensationThrowEvent => []

/-- The full spec (keyword + schema) a kind projects to. -/
def Kind.spec (k : Kind) : KindSpec := { keyword := k.keyword, attrs := k.schema }

/-- A rendered IR element: its kind, element id, and attribute values. -/
structure IrElement where
  /-- The element's kind (always a supported kind — `Kind` is closed). -/
  kind : Kind
  /-- The element's BPMN id. -/
  id : String
  /-- The element's rendered attribute values (key/value pairs). -/
  attrs : List (String × AttrVal)
  deriving DecidableEq, Repr, Inhabited

/-- `Kind.all` is complete: every kind appears in it. -/
theorem kinds_complete : ∀ k : Kind, k ∈ Kind.all := by
  intro k; cases k <;> decide

/-- **Anti-drift (keywords).** The model's keyword projection is exactly the
supported keyword set of the `processos` registry mirror. Discharged by `rfl` at
build time: add/rename/reorder a `Kind` (or its keyword) without matching
`registry` and this fails `lake build`. -/
theorem keywords_match_registry : Kind.all.map Kind.keyword = registryKeywords := by
  rfl

/-- **Anti-drift (full schema).** The model's `(keyword, attrs)` projection is
exactly the `processos` registry mirror — pins the per-kind attribute schema,
not just the keyword. -/
theorem specs_match_registry : Kind.all.map Kind.spec = registry := by
  rfl

end Roundtrip
