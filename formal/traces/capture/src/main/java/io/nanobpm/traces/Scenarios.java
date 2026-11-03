/*
 * Part of the nano Zeebe differential-trace harness (formal/traces, #1391).
 */
package io.nanobpm.traces;

import java.nio.file.Path;
import java.util.regex.Pattern;

/** Scenario-format checks shared in intent with the Rust loader (both reject a model without DI). */
final class Scenarios {
  private static final Pattern SHAPE = Pattern.compile("<(?:\\w+:)?BPMNShape\\b");
  private static final Pattern PLANE = Pattern.compile("<(?:\\w+:)?BPMNPlane\\b");

  private Scenarios() {}

  /** AGENTS.md: all BPMN models need DI. A scenario without a diagram is refused, not captured. */
  static void requireDi(final String xml, final Path scenario) {
    if (!PLANE.matcher(xml).find() || !SHAPE.matcher(xml).find()) {
      throw new IllegalArgumentException(
          scenario + ": process.bpmn has no BPMN DI (BPMNDiagram/BPMNPlane/BPMNShape)");
    }
  }
}
