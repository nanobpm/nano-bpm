# mcp-process-with-inputs.bpmn

Vendored from camunda/camunda, `qa/c8-orchestration-cluster-e2e-test-suite/resources/mcp-process-with-inputs.bpmn`,
at upstream `main` HEAD `0f534be57c24dda30616a9883eaff8287d5cf981` (2026-11-10).

Camunda marks a BPMN process as an MCP tool via `zeebe:properties`/`zeebe:property`
extension elements on the message start event (`io.camunda.tool:name`,
`io.camunda.tool:purpose`, `io.camunda.tool:input_N_{name,type,description,required}`, …).
Nano's parser test (`bpmn::tests`) asserts these markers survive parsing onto the
start event's `Element::properties`.
