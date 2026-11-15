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
(plus the `triggeredByEvent` attribute, which alone tells an event sub-process
from an embedded one — both are `<subProcess>`) is a faithful (injective)
re-encoding of the IR keyword, so no element kind
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
  /-- The `triggeredByEvent` attribute. BPMN has no `<eventSubProcess>` tag: an
  event sub-process is a `<subProcess triggeredByEvent="true">`
  (`processos/src/bpmn_model.rs`), so this flag — not the tag — is the
  discriminator between an embedded and an event sub-process. `false` for
  every other element. -/
  triggeredByEvent : Bool
  deriving DecidableEq, Repr, Inhabited

/-- Project a kind to its BPMN structural identity — mirror of the element
emission in `bpmn_model.rs`. -/
def toBpmn : Kind → BpmnShape
  | .startEvent => ⟨.startEvent, .none, false⟩
  | .endEvent => ⟨.endEvent, .none, false⟩
  | .terminateEndEvent => ⟨.endEvent, .terminate, false⟩
  | .errorEndEvent => ⟨.endEvent, .error, false⟩
  | .serviceTask => ⟨.serviceTask, .none, false⟩
  | .businessRuleTask => ⟨.businessRuleTask, .none, false⟩
  | .userTask => ⟨.userTask, .none, false⟩
  | .exclusiveGateway => ⟨.exclusiveGateway, .none, false⟩
  | .parallelGateway => ⟨.parallelGateway, .none, false⟩
  | .inclusiveGateway => ⟨.inclusiveGateway, .none, false⟩
  | .eventBasedGateway => ⟨.eventBasedGateway, .none, false⟩
  | .errorBoundaryEvent => ⟨.boundaryEvent, .error, false⟩
  | .timerIntermediateCatchEvent => ⟨.intermediateCatchEvent, .timer, false⟩
  | .timerBoundaryEvent => ⟨.boundaryEvent, .timer, false⟩
  | .messageIntermediateCatchEvent => ⟨.intermediateCatchEvent, .message, false⟩
  | .messageBoundaryEvent => ⟨.boundaryEvent, .message, false⟩
  | .messageStartEvent => ⟨.startEvent, .message, false⟩
  | .timerStartEvent => ⟨.startEvent, .timer, false⟩
  | .subProcess => ⟨.subProcess, .none, false⟩
  | .eventSubProcess => ⟨.subProcess, .none, true⟩
  | .escalationThrowEvent => ⟨.intermediateThrowEvent, .escalation, false⟩
  | .escalationBoundaryEvent => ⟨.boundaryEvent, .escalation, false⟩
  | .intermediateThrowEvent => ⟨.intermediateThrowEvent, .none, false⟩
  | .linkIntermediateThrowEvent => ⟨.intermediateThrowEvent, .link, false⟩
  | .linkIntermediateCatchEvent => ⟨.intermediateCatchEvent, .link, false⟩
  | .task => ⟨.task, .none, false⟩
  | .scriptTask => ⟨.scriptTask, .none, false⟩
  | .callActivity => ⟨.callActivity, .none, false⟩
  | .signalIntermediateCatchEvent => ⟨.intermediateCatchEvent, .signal, false⟩
  | .signalBoundaryEvent => ⟨.boundaryEvent, .signal, false⟩
  | .conditionalIntermediateCatchEvent => ⟨.intermediateCatchEvent, .conditional, false⟩
  | .conditionalBoundaryEvent => ⟨.boundaryEvent, .conditional, false⟩
  | .compensationBoundaryEvent => ⟨.boundaryEvent, .compensate, false⟩
  | .compensationThrowEvent => ⟨.intermediateThrowEvent, .compensate, false⟩

/-- Recover the kind from a BPMN structural identity. Total: a `(tag,
eventDef)` pair outside the supported projection maps to `none` (an unsupported
or malformed BPMN element). -/
def fromBpmn : BpmnShape → Option Kind
  | ⟨.startEvent, .none, false⟩ => some .startEvent
  | ⟨.startEvent, .message, false⟩ => some .messageStartEvent
  | ⟨.startEvent, .timer, false⟩ => some .timerStartEvent
  | ⟨.endEvent, .none, false⟩ => some .endEvent
  | ⟨.endEvent, .terminate, false⟩ => some .terminateEndEvent
  | ⟨.endEvent, .error, false⟩ => some .errorEndEvent
  | ⟨.serviceTask, .none, false⟩ => some .serviceTask
  | ⟨.businessRuleTask, .none, false⟩ => some .businessRuleTask
  | ⟨.userTask, .none, false⟩ => some .userTask
  | ⟨.exclusiveGateway, .none, false⟩ => some .exclusiveGateway
  | ⟨.parallelGateway, .none, false⟩ => some .parallelGateway
  | ⟨.inclusiveGateway, .none, false⟩ => some .inclusiveGateway
  | ⟨.eventBasedGateway, .none, false⟩ => some .eventBasedGateway
  | ⟨.boundaryEvent, .error, false⟩ => some .errorBoundaryEvent
  | ⟨.boundaryEvent, .timer, false⟩ => some .timerBoundaryEvent
  | ⟨.boundaryEvent, .message, false⟩ => some .messageBoundaryEvent
  | ⟨.boundaryEvent, .escalation, false⟩ => some .escalationBoundaryEvent
  | ⟨.boundaryEvent, .signal, false⟩ => some .signalBoundaryEvent
  | ⟨.boundaryEvent, .conditional, false⟩ => some .conditionalBoundaryEvent
  | ⟨.boundaryEvent, .compensate, false⟩ => some .compensationBoundaryEvent
  | ⟨.intermediateCatchEvent, .timer, false⟩ => some .timerIntermediateCatchEvent
  | ⟨.intermediateCatchEvent, .message, false⟩ => some .messageIntermediateCatchEvent
  | ⟨.intermediateCatchEvent, .link, false⟩ => some .linkIntermediateCatchEvent
  | ⟨.intermediateCatchEvent, .signal, false⟩ => some .signalIntermediateCatchEvent
  | ⟨.intermediateCatchEvent, .conditional, false⟩ => some .conditionalIntermediateCatchEvent
  | ⟨.intermediateThrowEvent, .none, false⟩ => some .intermediateThrowEvent
  | ⟨.intermediateThrowEvent, .escalation, false⟩ => some .escalationThrowEvent
  | ⟨.intermediateThrowEvent, .link, false⟩ => some .linkIntermediateThrowEvent
  | ⟨.intermediateThrowEvent, .compensate, false⟩ => some .compensationThrowEvent
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
  | ⟨.endEvent, .escalation, false⟩ => some .escalationThrowEvent
  | ⟨.endEvent, .compensate, false⟩ => some .compensationThrowEvent
  | ⟨.subProcess, .none, false⟩ => some .subProcess
  | ⟨.subProcess, .none, true⟩ => some .eventSubProcess
  | ⟨.task, .none, false⟩ => some .task
  | ⟨.scriptTask, .none, false⟩ => some .scriptTask
  | ⟨.callActivity, .none, false⟩ => some .callActivity
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
