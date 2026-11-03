/*
 * Part of the nano Zeebe differential-trace harness (formal/traces, #1391).
 */
package io.nanobpm.traces;

import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.nio.file.Path;
import org.junit.Test;

/**
 * Unit tests of the scenario DI contract in {@link Scenarios#requireDi}: the diagram must cover
 * every flow node with a shape and every sequence flow with an edge — partial DI is refused, as
 * the Rust loader refuses it.
 */
public class ScenariosTest {

  private static final Path DIR = Path.of("scenario-x");

  private static final String MODEL =
      """
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:bpmndi="http://www.omg.org/spec/BPMN/20100524/DI">
        <bpmn:process id="p" isExecutable="true">
          <bpmn:startEvent id="start" />
          <bpmn:serviceTask id="task" />
          <bpmn:endEvent id="end" />
          <bpmn:sequenceFlow id="f1" sourceRef="start" targetRef="task" />
          <bpmn:sequenceFlow id="f2" sourceRef="task" targetRef="end" />
        </bpmn:process>
        <bpmndi:BPMNDiagram id="d1">
          <bpmndi:BPMNPlane id="plane1" bpmnElement="p">
            <bpmndi:BPMNShape id="start_di" bpmnElement="start" />
            <bpmndi:BPMNShape id="task_di" bpmnElement="task" />
            <bpmndi:BPMNShape id="end_di" bpmnElement="end" />
            <bpmndi:BPMNEdge id="f1_di" bpmnElement="f1" />
            <bpmndi:BPMNEdge id="f2_di" bpmnElement="f2" />
          </bpmndi:BPMNPlane>
        </bpmndi:BPMNDiagram>
      </bpmn:definitions>
      """;

  @Test
  public void completeDiIsAccepted() {
    Scenarios.requireDi(MODEL, DIR);
  }

  @Test
  public void missingDiagramIsRejected() {
    final String cut = MODEL.substring(0, MODEL.indexOf("<bpmndi:BPMNDiagram"));
    final Exception e =
        assertThrows(
            IllegalArgumentException.class, () -> Scenarios.requireDi(cut, DIR));
    assertTrue(e.getMessage(), e.getMessage().contains("plane=false"));
  }

  @Test
  public void oneMissingShapeIsRejected() {
    final String partial = MODEL.replace("bpmnElement=\"task\"", "bpmnElement=\"elsewhere\"");
    final Exception e =
        assertThrows(
            IllegalArgumentException.class, () -> Scenarios.requireDi(partial, DIR));
    assertTrue(e.getMessage(), e.getMessage().contains("task"));
  }

  @Test
  public void oneMissingEdgeIsRejected() {
    final String partial = MODEL.replace("bpmnElement=\"f2\"", "bpmnElement=\"elsewhere\"");
    final Exception e =
        assertThrows(
            IllegalArgumentException.class, () -> Scenarios.requireDi(partial, DIR));
    assertTrue(e.getMessage(), e.getMessage().contains("f2"));
  }

  @Test
  public void singleQuotedAndSpacedAttributesAreRecognised() {
    final String styled =
        MODEL.replace("bpmnElement=\"task\"", "bpmnElement = 'task'")
            .replace("id=\"f1\"", "id =\n          'f1'");
    Scenarios.requireDi(styled, DIR);
  }

  @Test
  public void aCommentedOutShapeIsNotCoverage() {
    final String commented =
        MODEL.replace(
            "<bpmndi:BPMNShape id=\"task_di\" bpmnElement=\"task\" />",
            "<!-- <bpmndi:BPMNShape id=\"task_di\" bpmnElement=\"task\" /> -->");
    final Exception e =
        assertThrows(
            IllegalArgumentException.class, () -> Scenarios.requireDi(commented, DIR));
    assertTrue(e.getMessage(), e.getMessage().contains("task"));
  }
}
