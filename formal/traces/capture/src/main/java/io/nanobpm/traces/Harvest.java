/*
 * Part of the nano Zeebe differential-trace harness (formal/traces, #1391 step 2, #1249).
 */
package io.nanobpm.traces;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.camunda.zeebe.protocol.record.Record;
import io.camunda.zeebe.protocol.record.RecordType;
import io.camunda.zeebe.protocol.record.ValueType;
import io.camunda.zeebe.protocol.record.value.DeploymentRecordValue;
import io.camunda.zeebe.protocol.record.value.deployment.DeploymentResource;
import io.camunda.zeebe.test.util.record.RecordingExporter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.junit.runner.Description;
import org.junit.runner.JUnitCore;
import org.junit.runner.Request;
import org.junit.runner.notification.Failure;
import org.junit.runner.notification.RunListener;

/**
 * DYNAMIC harvester: runs the pinned Zeebe engine tests themselves (the compiled {@code
 * zeebe/engine} test classes, exactly as Zeebe's CI runs them) and records, per passing test
 * method, the process models it deployed and the client commands it issued — read back from the
 * {@link RecordingExporter} record stream rather than parsed out of the Java test source. That
 * captures models built through helper methods, lambdas and parameterised builders (e.g. {@code
 * JobWorkerElementIncidentTest}) which a static source parser cannot reproduce.
 *
 * <p>Each harvested test whose deployment + commands map onto the scenario script vocabulary
 * becomes {@code scenarios/h-<Class>-<method>/} ({@code process.bpmn} + {@code script.json}); every
 * other test is recorded out-of-scope with a reason in {@code harvest/out-of-scope.json}. Goldens
 * are then captured by {@link Capture} from the SAME committed (DI-bearing) XML, so Zeebe and nano
 * run identical models.
 *
 * <p>Usage: {@code Harvest <tracesDir> <testClassesDir> [classSimpleName...]}.
 */
public final class Harvest {

  /** Prefix of every harvested scenario directory (hand-written scenarios never use it). */
  static final String PREFIX = "h-";

  private static final Pattern UNSAFE = Pattern.compile("[^A-Za-z0-9]+");

  private Harvest() {}

  public static void main(final String[] args) throws Exception {
    final Path dir = Path.of(args[0]);
    final Path testClasses = Path.of(args[1]);
    final List<String> only = List.of(args).subList(2, args.length);
    final JsonNode config = Capture.JSON.readTree(dir.resolve("harvest/harvest.json").toFile());
    final List<String> classes = new ArrayList<>();
    for (final JsonNode pkg : config.get("packages")) {
      final Path root = testClasses.resolve(pkg.asText().replace('.', '/'));
      try (Stream<Path> s = Files.walk(root)) {
        s.map(p -> testClasses.relativize(p).toString())
            .filter(p -> p.endsWith("Test.class") && !p.contains("$"))
            .map(p -> p.substring(0, p.length() - ".class".length()).replace('/', '.'))
            .sorted()
            .forEach(classes::add);
      }
    }
    // skipClasses: {simpleName: reason} — classes that cannot run in-process here.
    final Map<String, String> skip = new HashMap<>();
    config.path("skipClasses").fields().forEachRemaining(e -> skip.put(e.getKey(), e.getValue().asText()));

    final Map<String, ObjectNode> harvested = new TreeMap<>();
    final Map<String, String> outOfScope = new TreeMap<>();
    final Map<String, Integer> classCounts = new TreeMap<>();
    for (final String cls : classes) {
      final String simple = cls.substring(cls.lastIndexOf('.') + 1);
      if (!only.isEmpty() && !only.contains(simple)) {
        continue;
      }
      if (skip.containsKey(simple)) {
        outOfScope.put(simple + "#*", "class skipped: " + skip.get(simple));
        continue;
      }
      System.out.println("harvest " + cls);
      final Listener listener = new Listener();
      final JUnitCore core = new JUnitCore();
      core.addListener(listener);
      try {
        core.run(Request.aClass(Class.forName(cls)));
      } catch (final Throwable t) {
        outOfScope.put(simple + "#*", "class failed to run: " + t);
        continue;
      }
      for (final var e : listener.tests.entrySet()) {
        final String test = e.getKey();
        final Harvested h = e.getValue();
        if (h.failure != null) {
          outOfScope.put(test, "zeebe test did not pass in the harvest run: " + h.failure);
          continue;
        }
        try {
          harvested.put(test, convert(test, h.records));
        } catch (final OutOfScope oos) {
          outOfScope.put(test, oos.getMessage());
        }
      }
      classCounts.put(simple, listener.tests.size());
    }
    write(dir, harvested, outOfScope, classCounts);
    System.exit(0);
  }

  /** A test method that cannot be turned into a scenario, with the reason. */
  static final class OutOfScope extends Exception {
    OutOfScope(final String reason) {
      super(reason);
    }
  }

  static final class Harvested {
    List<Record<?>> records = List.of();
    String failure;
  }

  /** Collects each test method's record stream (reset at start, snapshotted at finish). */
  static final class Listener extends RunListener {
    final Map<String, Harvested> tests = new LinkedHashMap<>();

    private static String id(final Description d) {
      final String cls = d.getClassName().substring(d.getClassName().lastIndexOf('.') + 1);
      return cls + "#" + d.getMethodName();
    }

    @Override
    public void testStarted(final Description d) {
      RecordingExporter.reset();
      tests.put(id(d), new Harvested());
    }

    @Override
    public void testFailure(final Failure f) {
      final Harvested h = tests.computeIfAbsent(id(f.getDescription()), k -> new Harvested());
      final String msg = String.valueOf(f.getMessage());
      h.failure = msg.length() > 160 ? msg.substring(0, 160) + "…" : msg;
    }

    @Override
    public void testAssumptionFailure(final Failure f) {
      testFailure(f);
    }

    @Override
    public void testIgnored(final Description d) {
      final Harvested h = new Harvested();
      h.failure = "@Ignore";
      tests.put(id(d), h);
    }

    @Override
    public void testFinished(final Description d) {
      final Harvested h = tests.get(id(d));
      if (h != null) {
        h.records = new ArrayList<>(RecordingExporter.getRecords());
      }
    }
  }

  /** Scenario name for a test id: {@code h-<Class>-<method>} with unsafe characters folded. */
  static String scenarioName(final String test) {
    final String folded = UNSAFE.matcher(test.replace("#", "-")).replaceAll("-");
    final String trimmed = folded.replaceAll("-+$", "");
    return PREFIX + trimmed;
  }

  /**
   * Maps one test's record stream onto a scenario: exactly one deployment of one BPMN resource and
   * the client commands (records written by the test, not by the engine: {@code
   * sourceRecordPosition < 0}) translated to script ops.
   */
  static ObjectNode convert(final String test, final List<Record<?>> records)
      throws OutOfScope {
    final List<Record<?>> client = new ArrayList<>();
    final Map<Long, Record<?>> rejectedBySource = new HashMap<>();
    final Map<Long, String> jobTypes = new HashMap<>();
    final Map<Long, String> incidentElements = new HashMap<>();
    final Map<Long, String> elementIds = new HashMap<>();
    final Map<Long, Long> timerDue = new HashMap<>();
    for (final Record<?> r : records) {
      if (r.getRecordType() == RecordType.COMMAND && r.getSourceRecordPosition() < 0) {
        client.add(r);
      }
      if (r.getRecordType() == RecordType.COMMAND_REJECTION) {
        rejectedBySource.put(r.getSourceRecordPosition(), r);
      }
    }
    String xml = null;
    String processId = null;
    long instanceKey = -1;
    long clock = -1;
    final ArrayNode steps = Capture.NODES.arrayNode();
    // Keys are unique per stream: learn job types, incident elements, element ids and timer due
    // dates from the whole stream once.
    for (final Record<?> r : records) {
      learn(r, jobTypes, incidentElements, elementIds, timerDue);
    }
    for (final Record<?> c : client) {
      final ObjectNode value = json(c);
      final String vt = c.getValueType().name();
      final String intent = c.getIntent().name();
      final boolean rejected = rejectedBySource.containsKey(c.getPosition());
      final ObjectNode step = Capture.NODES.objectNode();
      switch (vt + ":" + intent) {
        case "DEPLOYMENT:CREATE" -> {
          if (xml != null) {
            throw new OutOfScope("more than one deployment");
          }
          final List<DeploymentResource> res =
              ((DeploymentRecordValue) c.getValue()).getResources();
          if (res.size() != 1) {
            throw new OutOfScope("deployment of " + res.size() + " resources");
          }
          final String name = res.get(0).getResourceName();
          if (!name.endsWith(".bpmn") && !name.endsWith(".xml")) {
            throw new OutOfScope("non-BPMN resource " + name);
          }
          if (rejected) {
            throw new OutOfScope("deployment rejected (deploy-time validation test)");
          }
          xml = new String(res.get(0).getResource(), StandardCharsets.UTF_8);
          continue;
        }
        case "PROCESS_INSTANCE_CREATION:CREATE" -> {
          if (instanceKey >= 0 || processId != null) {
            throw new OutOfScope("more than one process instance");
          }
          if (value.path("startInstructions").size() > 0) {
            throw new OutOfScope("process instance created with start instructions");
          }
          processId = value.get("bpmnProcessId").asText();
          instanceKey = 0;
          clock = c.getTimestamp();
          step.put("op", "createInstance");
          step.put("bpmnProcessId", processId);
          putVariables(step, value);
        }
        case "PROCESS_INSTANCE:CANCEL" -> step.put("op", "cancelInstance");
        case "USAGE_METRIC:EXPORT", "MESSAGE:EXPIRE", "JOB_METRICS_BATCH:EXPORT" -> {
          continue; // engine-scheduled housekeeping, not a test command
        }
        case "JOB_BATCH:ACTIVATE" -> {
          continue; // completeJob / failJob activate the job themselves
        }
        case "JOB:COMPLETE" -> {
          step.put("op", "completeJob");
          step.put("jobType", known(jobTypes, c.getKey(), "job"));
          putVariables(step, value);
        }
        case "JOB:FAIL" -> {
          step.put("op", "failJob");
          step.put("jobType", known(jobTypes, c.getKey(), "job"));
          step.put("retries", value.get("retries").asInt());
          if (!value.path("errorMessage").asText("").isEmpty()) {
            step.put("errorMessage", value.get("errorMessage").asText());
          }
          if (value.path("retryBackoff").asLong(0) != 0) {
            throw new OutOfScope("job failed with a retry backoff");
          }
        }
        case "INCIDENT:RESOLVE" -> {
          step.put("op", "resolveIncident");
          step.put("elementId", known(incidentElements, c.getKey(), "incident"));
        }
        case "VARIABLE_DOCUMENT:UPDATE" -> {
          step.put("op", "updateVariables");
          final long scope = value.get("scopeKey").asLong();
          final String scopeElement = elementIds.get(scope);
          if (scopeElement == null) {
            throw new OutOfScope("variable update on an unknown scope");
          }
          if (scope != instanceKeyOf(records)) {
            step.put("scopeElementId", scopeElement);
          }
          if (!"PROPAGATE".equals(value.path("updateSemantics").asText("PROPAGATE"))) {
            throw new OutOfScope("variable update with LOCAL semantics");
          }
          putVariables(step, value);
        }
        case "MESSAGE:PUBLISH" -> {
          step.put("op", "publishMessage");
          step.put("name", value.get("name").asText());
          step.put("correlationKey", value.get("correlationKey").asText());
          step.put("timeToLiveMillis", value.get("timeToLive").asLong());
          putVariables(step, value);
        }
        case "USER_TASK:COMPLETE" -> {
          step.put("op", "completeUserTask");
          step.put("elementId", value.get("elementId").asText());
          putVariables(step, value);
        }
        case "TIMER:TRIGGER" -> {
          final Long due = timerDue.get(c.getKey());
          if (due == null || clock < 0) {
            throw new OutOfScope("timer triggered before it was observed");
          }
          final long millis = Math.max(0, due - clock);
          clock += millis;
          step.put("op", "advanceClock");
          step.put("millis", millis);
        }
        default -> throw new OutOfScope("unsupported client command " + vt + ":" + intent);
      }
      if (rejected) {
        step.put("expectRejection", true);
      }
      steps.add(step);
    }
    if (xml == null) {
      throw new OutOfScope("no process deployed by the test (class-level deployment)");
    }
    if (processId == null) {
      throw new OutOfScope("no process instance created");
    }
    final ObjectNode out = Capture.NODES.objectNode();
    out.put("description", "Harvested from Zeebe engine test " + test + " (formal/traces/harvest).");
    out.put("harvestedFrom", test);
    out.put("bpmn", xml);
    out.set("steps", steps);
    return out;
  }

  private static long instanceKeyOf(final List<Record<?>> records) {
    for (final Record<?> r : records) {
      if (r.getValueType() == ValueType.PROCESS_INSTANCE_CREATION
          && r.getRecordType() == RecordType.EVENT) {
        return json(r).get("processInstanceKey").asLong();
      }
    }
    return -1;
  }

  private static void learn(
      final Record<?> r,
      final Map<Long, String> jobTypes,
      final Map<Long, String> incidentElements,
      final Map<Long, String> elementIds,
      final Map<Long, Long> timerDue) {
    if (r.getRecordType() != RecordType.EVENT) {
      return;
    }
    final String intent = r.getIntent().name();
    switch (r.getValueType()) {
      case JOB -> {
        if (intent.equals("CREATED")) {
          jobTypes.put(r.getKey(), json(r).get("type").asText());
        }
      }
      case INCIDENT -> {
        if (intent.equals("CREATED")) {
          incidentElements.put(r.getKey(), json(r).get("elementId").asText());
        }
      }
      case PROCESS_INSTANCE -> elementIds.put(r.getKey(), json(r).get("elementId").asText());
      case TIMER -> {
        if (intent.equals("CREATED")) {
          timerDue.put(r.getKey(), json(r).get("dueDate").asLong());
        }
      }
      default -> {}
    }
  }

  private static String known(final Map<Long, String> map, final long key, final String what)
      throws OutOfScope {
    final String v = map.get(key);
    if (v == null) {
      throw new OutOfScope("command on a " + what + " not created by this test's instance");
    }
    return v;
  }

  private static ObjectNode json(final Record<?> r) {
    try {
      return (ObjectNode) Capture.JSON.readTree(r.toJson()).get("value");
    } catch (final IOException e) {
      throw new IllegalStateException(e);
    }
  }

  private static void putVariables(final ObjectNode step, final ObjectNode value) {
    final JsonNode v = value.get("variables");
    if (v != null && v.isObject() && v.size() > 0) {
      step.set("variables", v);
    } else if (v != null && v.isTextual()) {
      try {
        final JsonNode parsed = Capture.JSON.readTree(v.asText());
        if (parsed.isObject() && parsed.size() > 0) {
          step.set("variables", parsed);
        }
      } catch (final IOException e) {
        throw new IllegalStateException(e);
      }
    }
  }

  private static void write(
      final Path dir,
      final Map<String, ObjectNode> harvested,
      final Map<String, String> outOfScope,
      final Map<String, Integer> classCounts)
      throws IOException, InterruptedException {
    final int tests = harvested.size() + outOfScope.size();
    final Path scenarios = dir.resolve("scenarios");
    // Drop every previously harvested scenario (and its goldens): the set is derived, so a test
    // that disappeared or became out-of-scope on a pin bump must not linger.
    try (Stream<Path> s = Files.list(scenarios)) {
      for (final Path p : s.filter(p -> p.getFileName().toString().startsWith(PREFIX)).toList()) {
        deleteTree(p);
      }
    }
    try (Stream<Path> s = Files.list(dir.resolve("goldens"))) {
      for (final Path p : s.filter(p -> p.getFileName().toString().startsWith(PREFIX)).toList()) {
        Files.delete(p);
      }
    }
    int written = 0;
    int laidOut = 0;
    for (final var e : harvested.entrySet()) {
      final String test = e.getKey();
      final ObjectNode scenario = e.getValue();
      final Path target = scenarios.resolve(scenarioName(test));
      String xml = scenario.remove("bpmn").asText();
      try {
        Scenarios.requireDi(xml, target);
      } catch (final IllegalArgumentException missing) {
        // The fluent builder emitted no / partial DI: lay it out with processos.
        try {
          xml = layout(xml);
          Scenarios.requireDi(xml, target);
          laidOut++;
        } catch (final IllegalArgumentException | IOException failed) {
          outOfScope.put(test, "cannot lay out (no DI): " + failed.getMessage());
          continue;
        }
      }
      Files.createDirectories(target);
      Files.writeString(target.resolve("process.bpmn"), xml.strip() + "\n");
      Files.writeString(
          target.resolve("script.json"), Capture.JSON.writeValueAsString(scenario) + "\n");
      written++;
    }
    final ObjectNode oos = Capture.NODES.objectNode();
    outOfScope.forEach(oos::put);
    Capture.write(dir.resolve("harvest/out-of-scope.json"), oos);
    final ObjectNode summary = Capture.NODES.objectNode();
    summary.put("tests", tests);
    summary.put("harvested", written);
    summary.put("laidOutByProcessos", laidOut);
    summary.put("outOfScope", outOfScope.size());
    final ObjectNode cc = summary.putObject("testsPerClass");
    classCounts.forEach(cc::put);
    Capture.write(dir.resolve("harvest/summary.json"), summary);
    System.out.println(
        "harvest: " + written + " scenarios written, " + outOfScope.size() + " out of scope");
  }

  /** Runs the harvested XML through processos' layout/DI emitter ({@code PROCESSOS_BIN}). */
  private static String layout(final String xml) throws IOException, InterruptedException {
    final String bin = System.getenv("PROCESSOS_BIN");
    if (bin == null || bin.isBlank()) {
      throw new IOException("PROCESSOS_BIN not set");
    }
    final Path tmp = Files.createTempDirectory("nano-harvest-layout");
    try {
      final Path in = tmp.resolve("in.bpmn");
      final Path ann = tmp.resolve("ann.json");
      final Path out = tmp.resolve("out.bpmn");
      Files.writeString(in, xml);
      Files.writeString(ann, "{}");
      final Process p =
          new ProcessBuilder(
                  bin,
                  "layout",
                  in.toString(),
                  ann.toString(),
                  "--out",
                  out.toString(),
                  "--debug-svg",
                  tmp.resolve("d.svg").toString())
              .redirectErrorStream(true)
              .start();
      final String log = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
      if (p.waitFor() != 0 || !Files.exists(out)) {
        throw new IOException("processos layout failed: " + log.strip());
      }
      return Files.readString(out);
    } finally {
      deleteTree(tmp);
    }
  }

  static void deleteTree(final Path root) throws IOException {
    if (!Files.exists(root)) {
      return;
    }
    try (Stream<Path> s = Files.walk(root)) {
      for (final Path p : s.sorted(java.util.Comparator.reverseOrder()).toList()) {
        Files.delete(p);
      }
    }
  }
}
