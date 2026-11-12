import Roundtrip.Ir

/-!
# `Roundtrip.Bpmn` — the BPMN projection and its inverse

The other lossless projection of an engine element is BPMN XML
(`processos/src/bpmn_model.rs`). Where the IR names a kind with a single
keyword, BPMN names it *structurally*: a generic element `localName` (many kinds
share one, e.g. every boundary event is `<boundaryEvent>`, every timed catch is
`<intermediateCatchEvent>`) **disambiguated by a child event definition**
(`<timerEventDefinition>`, `<errorEventDefinition>`, …) and a couple of
degenerate cases (a terminate end is `<endEvent>` + `<terminateEventDefinition>`).

Two kinds — `escalationThrowEvent` and `compensationThrowEvent` — have *two*
supported BPMN shapes: an intermediate `<intermediateThrowEvent>` (when the
element has an outgoing flow) and a terminal `<endEvent>` (when it does not).
That intermediate/terminal flavour is not carried by the element kind; it is
derived from the outgoing-flow layer. `toBpmn` therefore picks the intermediate
shape as the single canonical projection, while `fromBpmn` accepts *both* shapes
for those kinds — exactly mirroring the Rust parser/serializer.

This module models that projection as `toBpmn : Kind → BpmnShape` and its
inverse `fromBpmn : BpmnShape → Option Kind`, and proves the inverse *is* a left
inverse on the supported set (`fromBpmn_toBpmn`). That theorem is the crux of the
round-trip: it certifies that the `(localName, eventDefinition)` pair BPMN uses
is a faithful (injective) re-encoding of the IR keyword, so no element kind
collapses into another when it crosses to BPMN and back.
-/

namespace Roundtrip

/-- A BPMN element `localName` — the shared tag several kinds project onto. -/
inductive BpmnTag where
  | startEvent
  | endEvent
  | serviceTask
  | businessRuleTask
  | userTask
  | exclusiveGateway
  | parallelGateway
  | inclusiveGateway
  | eventBasedGateway
  | boundaryEvent
  | intermediateCatchEvent
  | intermediateThrowEvent
  | subProcess
  | eventSubProcess
  | task
  | scriptTask
  | callActivity
  deriving DecidableEq, Repr, Inhabited

/-- The child event definition that disambiguates a shared `BpmnTag`. `none`
means the element carries no event definition (a plain task/gateway, or a none
start/end/throw event). -/
inductive EventDef where
  | none
  | terminate
  | error
  | timer
  | message
  | escalation
  | link
  | signal
  | conditional
  | compensate
  deriving DecidableEq, Repr, Inhabited

/-- A BPMN element's structural identity: its `localName` plus its child event
definition. This is exactly the information BPMN uses to tell one element kind
from another. -/
structure BpmnShape where
  /-- The BPMN element `localName`. -/
  tag : BpmnTag
  /-- The disambiguating child event definition (`none` if absent). -/
  eventDef : EventDef
  deriving DecidableEq, Repr, Inhabited

/-- Project a kind to its BPMN structural identity — mirror of the element
emission in `bpmn_model.rs`. -/
def toBpmn : Kind → BpmnShape
  | .startEvent => ⟨.startEvent, .none⟩
  | .endEvent => ⟨.endEvent, .none⟩
  | .terminateEndEvent => ⟨.endEvent, .terminate⟩
  | .serviceTask => ⟨.serviceTask, .none⟩
  | .businessRuleTask => ⟨.businessRuleTask, .none⟩
  | .userTask => ⟨.userTask, .none⟩
  | .exclusiveGateway => ⟨.exclusiveGateway, .none⟩
  | .parallelGateway => ⟨.parallelGateway, .none⟩
  | .inclusiveGateway => ⟨.inclusiveGateway, .none⟩
  | .eventBasedGateway => ⟨.eventBasedGateway, .none⟩
  | .errorBoundaryEvent => ⟨.boundaryEvent, .error⟩
  | .timerIntermediateCatchEvent => ⟨.intermediateCatchEvent, .timer⟩
  | .timerBoundaryEvent => ⟨.boundaryEvent, .timer⟩
  | .messageIntermediateCatchEvent => ⟨.intermediateCatchEvent, .message⟩
  | .messageBoundaryEvent => ⟨.boundaryEvent, .message⟩
  | .messageStartEvent => ⟨.startEvent, .message⟩
  | .timerStartEvent => ⟨.startEvent, .timer⟩
  | .subProcess => ⟨.subProcess, .none⟩
  | .eventSubProcess => ⟨.eventSubProcess, .none⟩
  | .escalationThrowEvent => ⟨.intermediateThrowEvent, .escalation⟩
  | .escalationBoundaryEvent => ⟨.boundaryEvent, .escalation⟩
  | .intermediateThrowEvent => ⟨.intermediateThrowEvent, .none⟩
  | .linkIntermediateThrowEvent => ⟨.intermediateThrowEvent, .link⟩
  | .linkIntermediateCatchEvent => ⟨.intermediateCatchEvent, .link⟩
  | .task => ⟨.task, .none⟩
  | .scriptTask => ⟨.scriptTask, .none⟩
  | .callActivity => ⟨.callActivity, .none⟩
  | .signalIntermediateCatchEvent => ⟨.intermediateCatchEvent, .signal⟩
  | .signalBoundaryEvent => ⟨.boundaryEvent, .signal⟩
  | .conditionalIntermediateCatchEvent => ⟨.intermediateCatchEvent, .conditional⟩
  | .conditionalBoundaryEvent => ⟨.boundaryEvent, .conditional⟩
  | .compensationBoundaryEvent => ⟨.boundaryEvent, .compensate⟩
  | .compensationThrowEvent => ⟨.intermediateThrowEvent, .compensate⟩

/-- Recover the kind from a BPMN structural identity. Total: a `(tag,
eventDef)` pair outside the supported projection maps to `none` (an unsupported
or malformed BPMN element). -/
def fromBpmn : BpmnShape → Option Kind
  | ⟨.startEvent, .none⟩ => some .startEvent
  | ⟨.startEvent, .message⟩ => some .messageStartEvent
  | ⟨.startEvent, .timer⟩ => some .timerStartEvent
  | ⟨.endEvent, .none⟩ => some .endEvent
  | ⟨.endEvent, .terminate⟩ => some .terminateEndEvent
  | ⟨.serviceTask, .none⟩ => some .serviceTask
  | ⟨.businessRuleTask, .none⟩ => some .businessRuleTask
  | ⟨.userTask, .none⟩ => some .userTask
  | ⟨.exclusiveGateway, .none⟩ => some .exclusiveGateway
  | ⟨.parallelGateway, .none⟩ => some .parallelGateway
  | ⟨.inclusiveGateway, .none⟩ => some .inclusiveGateway
  | ⟨.eventBasedGateway, .none⟩ => some .eventBasedGateway
  | ⟨.boundaryEvent, .error⟩ => some .errorBoundaryEvent
  | ⟨.boundaryEvent, .timer⟩ => some .timerBoundaryEvent
  | ⟨.boundaryEvent, .message⟩ => some .messageBoundaryEvent
  | ⟨.boundaryEvent, .escalation⟩ => some .escalationBoundaryEvent
  | ⟨.boundaryEvent, .signal⟩ => some .signalBoundaryEvent
  | ⟨.boundaryEvent, .conditional⟩ => some .conditionalBoundaryEvent
  | ⟨.boundaryEvent, .compensate⟩ => some .compensationBoundaryEvent
  | ⟨.intermediateCatchEvent, .timer⟩ => some .timerIntermediateCatchEvent
  | ⟨.intermediateCatchEvent, .message⟩ => some .messageIntermediateCatchEvent
  | ⟨.intermediateCatchEvent, .link⟩ => some .linkIntermediateCatchEvent
  | ⟨.intermediateCatchEvent, .signal⟩ => some .signalIntermediateCatchEvent
  | ⟨.intermediateCatchEvent, .conditional⟩ => some .conditionalIntermediateCatchEvent
  | ⟨.intermediateThrowEvent, .none⟩ => some .intermediateThrowEvent
  | ⟨.intermediateThrowEvent, .escalation⟩ => some .escalationThrowEvent
  | ⟨.intermediateThrowEvent, .link⟩ => some .linkIntermediateThrowEvent
  | ⟨.intermediateThrowEvent, .compensate⟩ => some .compensationThrowEvent
  -- Terminal (`<endEvent>`) flavour of the escalation/compensation throws. The
  -- Rust parser (`bpmn.rs`) maps `<endEvent><escalationEventDefinition>` and
  -- `<endEvent><compensateEventDefinition>` to the very same
  -- `EscalationThrowEvent`/`CompensationThrowEvent` kinds as their
  -- `<intermediateThrowEvent>` forms, and `bpmn_model.rs` re-emits the terminal
  -- tag when the element has no outgoing flow. That intermediate/terminal
  -- flavour is NOT part of the element kind — it is derived from the presence of
  -- an outgoing sequence flow, which lives in the flow layer, outside this
  -- element-kind projection. So BPMN → IR must accept BOTH shapes for these two
  -- kinds; `toBpmn` picks the intermediate shape as the single canonical one
  -- (the terminal flavour is reconstructed from the absent outgoing flow).
  | ⟨.endEvent, .escalation⟩ => some .escalationThrowEvent
  | ⟨.endEvent, .compensate⟩ => some .compensationThrowEvent
  | ⟨.subProcess, .none⟩ => some .subProcess
  | ⟨.eventSubProcess, .none⟩ => some .eventSubProcess
  | ⟨.task, .none⟩ => some .task
  | ⟨.scriptTask, .none⟩ => some .scriptTask
  | ⟨.callActivity, .none⟩ => some .callActivity
  | _ => none

/-- **`fromBpmn` is a left inverse of `toBpmn` on the supported set.** The BPMN
`(localName, eventDefinition)` re-encoding of a kind is injective — every kind
round-trips through BPMN and back unchanged. Proof: exhaustive on the 32 kinds,
each arm `rfl` (structural, no strings). -/
theorem fromBpmn_toBpmn (k : Kind) : fromBpmn (toBpmn k) = some k := by
  cases k <;> rfl

/-- A `BpmnShape` is *canonical* when it is the projection of some supported
kind — i.e. it is in the image of `toBpmn`. -/
def BpmnShape.Canonical (s : BpmnShape) : Prop := ∃ k : Kind, s = toBpmn k

/-- **`toBpmn` is a left inverse of `fromBpmn` on canonical shapes.** Together
with `fromBpmn_toBpmn` this makes `toBpmn`/`fromBpmn` a genuine bijection between
the supported kinds and the canonical BPMN shapes. -/
theorem toBpmn_fromBpmn {s : BpmnShape} (h : s.Canonical) :
    (fromBpmn s).map toBpmn = some s := by
  obtain ⟨k, rfl⟩ := h
  rw [fromBpmn_toBpmn]
  rfl

end Roundtrip
