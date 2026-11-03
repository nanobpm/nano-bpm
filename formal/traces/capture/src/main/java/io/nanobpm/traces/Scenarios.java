/*
 * Part of the nano Zeebe differential-trace harness (formal/traces, #1391).
 */
package io.nanobpm.traces;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Scenario-format checks shared in intent with the Rust loader (both reject a model without DI). */
final class Scenarios {

  private static final Pattern TAG_START = Pattern.compile("<([\\w.-]+:)?(\\w+)[\\s/>]");
  private static final Pattern COMMENT = Pattern.compile("<!--.*?-->", Pattern.DOTALL);
  private static final Pattern CDATA = Pattern.compile("<!\\[CDATA\\[.*?\\]\\]>", Pattern.DOTALL);
  private static final Pattern ATTR =
      Pattern.compile("([\\w:.-]+)\\s*=\\s*(\"([^\"]*)\"|'([^']*)')");

  /** BPMN tags whose elements are flow nodes needing a {@code BPMNShape}. */
  private static final Set<String> FLOW_NODES =
      Set.of(
          "startEvent", "endEvent", "intermediateCatchEvent", "intermediateThrowEvent",
          "boundaryEvent", "task", "serviceTask", "userTask", "sendTask", "receiveTask",
          "scriptTask", "businessRuleTask", "manualTask", "callActivity", "subProcess",
          "adHocSubProcess", "exclusiveGateway", "parallelGateway", "inclusiveGateway",
          "eventBasedGateway");

  private Scenarios() {}

  /**
   * AGENTS.md: all BPMN models need DI. A scenario whose diagram is missing, or that misses the
   * shape of even one flow node or the edge of even one sequence flow, is refused, not captured —
   * the Rust loader in {@code load_scenario} applies the same rule, so a partial diagram must not
   * be publishable as a golden.
   */
  static void requireDi(final String xml, final Path scenario) {
    // Strip comments and CDATA sections first: a commented-out tag is not DI coverage, and a
    // tag mentioned inside CDATA (e.g. documentation showing example XML) is character data,
    // not a real element — the Rust loader's tokenizer treats both as text.
    final String doc = CDATA.matcher(COMMENT.matcher(xml).replaceAll("")).replaceAll("");
    boolean plane = false;
    final Set<String> shapes = new TreeSet<>();
    final Set<String> edges = new TreeSet<>();
    final Set<String> needsShape = new TreeSet<>();
    final Set<String> needsEdge = new TreeSet<>();
    for (final String[] tag : startTags(doc)) {
      final String name = tag[0];
      final String id = tag[1];
      final String bpmnElement = tag[2];
      switch (name) {
        case "BPMNPlane" -> plane = true;
        case "BPMNShape" -> {
          if (bpmnElement != null) {
            shapes.add(bpmnElement);
          }
        }
        case "BPMNEdge" -> {
          if (bpmnElement != null) {
            edges.add(bpmnElement);
          }
        }
        case "sequenceFlow" -> {
          if (id != null) {
            needsEdge.add(id);
          }
        }
        default -> {
          if (FLOW_NODES.contains(name) && id != null) {
            needsShape.add(id);
          }
        }
      }
    }
    final Set<String> missing = new TreeSet<>(needsShape);
    missing.removeAll(shapes);
    final Set<String> missingEdges = new TreeSet<>(needsEdge);
    missingEdges.removeAll(edges);
    missing.addAll(missingEdges);
    if (!plane || !missing.isEmpty()) {
      throw new IllegalArgumentException(
          scenario
              + ": process.bpmn lacks BPMN DI (AGENTS.md: all BPMN models need DI); plane="
              + plane
              + ", elements without a shape/edge: "
              + missing);
    }
  }

  /**
   * Every start tag in document order as {@code [localName, id, bpmnElement]} (either attribute
   * null when absent). Handles single- or double-quoted values and whitespace around {@code =},
   * mirroring the engine's XML tokenizer contract.
   */
  private static List<String[]> startTags(final String xml) {
    final List<String[]> out = new ArrayList<>();
    final Matcher m = TAG_START.matcher(xml);
    while (m.find()) {
      final String name = m.group(2);
      final int end = tagEnd(xml, m.end() - 1);
      if (end < 0) {
        break;
      }
      String id = null;
      String bpmnElement = null;
      final Matcher attrs = ATTR.matcher(xml.substring(m.end() - 1, end));
      while (attrs.find()) {
        final String key = attrs.group(1);
        final String value = attrs.group(3) != null ? attrs.group(3) : attrs.group(4);
        if ("id".equals(key)) {
          id = value;
        } else if ("bpmnElement".equals(key)) {
          bpmnElement = value;
        }
      }
      out.add(new String[] {name, id, bpmnElement});
      m.region(end, xml.length());
    }
    return out;
  }

  /** Index of the {@code >} closing the tag that starts before {@code from}, or -1 if unclosed. */
  private static int tagEnd(final String xml, final int from) {
    char quote = 0;
    for (int i = from; i < xml.length(); i++) {
      final char c = xml.charAt(i);
      if (quote != 0) {
        if (c == quote) {
          quote = 0;
        }
      } else if (c == '"' || c == '\'') {
        quote = c;
      } else if (c == '>') {
        return i;
      }
    }
    return -1;
  }
}
