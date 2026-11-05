/*
 * Part of the nano Zeebe differential-trace harness (formal/traces, #1391).
 */
package io.nanobpm.traces;

import static org.junit.Assert.assertEquals;

import java.util.function.Supplier;
import org.junit.Test;

/** Unit tests of the raising-site -> cell-id mapping (the shared failure:/rejection: grammar). */
public class CellIdsTest {

  private static final String JOB_BEHAVIOR =
      "io.camunda.zeebe.engine.processing.bpmn.behavior.BpmnJobBehavior";

  @Test
  public void namedMethodSite() {
    assertEquals(
        "failure:BpmnJobBehavior.evalTypeExp:EXTRACT_VALUE_ERROR",
        CellIds.failure(JOB_BEHAVIOR, "evalTypeExp", "EXTRACT_VALUE_ERROR"));
  }

  @Test
  public void lambdaIsAttributedToTheEnclosingNamedMethod() {
    assertEquals(
        "failure:BpmnJobBehavior.evalTypeExp:EXTRACT_VALUE_ERROR",
        CellIds.failure(JOB_BEHAVIOR, "lambda$evalTypeExp$3", "EXTRACT_VALUE_ERROR"));
  }

  /** A real javac lambda frame (not a hand-written name) maps to its enclosing method. */
  @Test
  public void realLambdaFrameMapsToEnclosingMethod() {
    final StackWalker.StackFrame frame = evalTypeExp().get();
    assertEquals(
        "rejection:CellIdsTest.evalTypeExp:NOT_FOUND",
        CellIds.rejection(frame.getClassName(), frame.getMethodName(), "NOT_FOUND"));
  }

  private static Supplier<StackWalker.StackFrame> evalTypeExp() {
    return () -> StackWalker.getInstance().walk(s -> s.findFirst()).orElseThrow();
  }

  @Test
  public void nestedAndAnonymousClassesUseTheTopLevelClass() {
    assertEquals(
        "rejection:JobCompleteProcessor.onCommand:INVALID_STATE",
        CellIds.rejection(
            "io.camunda.zeebe.engine.processing.job.JobCompleteProcessor$Inner$1",
            "onCommand",
            "INVALID_STATE"));
  }

  @Test
  public void initializersHaveNoMethodAndConstructorsUseTheirClassName() {
    assertEquals("Foo.", CellIds.site("a.b.Foo", "<clinit>"));
    assertEquals("Foo.", CellIds.site("a.b.Foo", "lambda$static$0"));
    assertEquals("Foo.", CellIds.site("a.b.Foo", "lambda$new$1"));
    assertEquals("Foo.Bar", CellIds.site("a.b.Foo$Bar", "<init>"));
  }

  @Test
  public void messageOnlyFailureHasEmptyErrorType() {
    assertEquals("failure:Foo.bar:", CellIds.failure("x.Foo", "bar", ""));
  }

  @Test
  public void coarseCells() {
    assertEquals("element:ServiceTask", CellIds.elementCell("SERVICE_TASK"));
    assertEquals(
        "intent:ProcessInstance:ELEMENT_ACTIVATED",
        CellIds.intentCell("ProcessInstanceIntent", "ELEMENT_ACTIVATED"));
    assertEquals("incident:JOB_NO_RETRIES", CellIds.incidentCell("JOB_NO_RETRIES"));
  }

  /**
   * The process instance itself touches no {@code element:} cell: {@code extract.mjs} emits no
   * {@code element:Process} (the BPMN {@code <process>} is the container, not a flow element).
   */
  @Test
  public void processInstanceRecordsTouchNoElementCell() {
    assertEquals(null, CellIds.elementCellOrNull("PROCESS"));
    assertEquals(null, CellIds.elementCellOrNull("MULTI_INSTANCE_BODY"));
    assertEquals("element:ServiceTask", CellIds.elementCellOrNull("SERVICE_TASK"));
  }
}
