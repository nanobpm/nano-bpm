/*
 * Part of the nano Zeebe differential-trace harness (formal/traces, #1391).
 */
package io.nanobpm.traces;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.camunda.zeebe.engine.util.EngineRule;
import io.camunda.zeebe.protocol.record.Record;
import io.camunda.zeebe.protocol.record.RecordType;
import io.camunda.zeebe.protocol.record.ValueType;
import io.camunda.zeebe.test.util.record.RecordingExporter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Stream;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * Runs every scenario under {@code formal/traces/scenarios/} on the pinned Zeebe engine (in-memory
 * {@link EngineRule}, no cluster) and writes, per scenario, the NORMALISED golden record stream
 * ({@code <scenario>.trace.json}) and the touched-cells file ({@code <scenario>.cells.json}).
 *
 * <p>Usage: {@code Capture <tracesDir> <pinSha>} (driven by {@code formal/traces/capture.sh}).
 */
public final class Capture {

  static final ObjectMapper JSON =
      new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
  static final JsonNodeFactory NODES = JsonNodeFactory.instance;

  /** Ceiling for one client wait on the recording exporter (see {@code Run.execute}). */
  static final long CLIENT_WAIT_MILLIS = Duration.ofSeconds(60).toMillis();

  private Capture() {}

  public static void main(final String[] args) throws Throwable {
    if (args.length < 2) {
      throw new IllegalArgumentException("usage: Capture <tracesDir> <pinSha> [scenario...]");
    }
    Sites.install();
    preloadEngineClasses();
    final Path dir = Path.of(args[0]);
    final String pin = args[1];
    final List<String> only = List.of(args).subList(2, args.length);
    final Normaliser spec = new Normaliser(JSON.readTree(dir.resolve("normaliser.json").toFile()));
    final Path out = dir.resolve("goldens");
    Files.createDirectories(out);
    final List<Path> scenarios;
    try (Stream<Path> s = Files.list(dir.resolve("scenarios"))) {
      scenarios = s.filter(Files::isDirectory).sorted().toList();
    }
    for (final Path scenario : scenarios) {
      final String name = scenario.getFileName().toString();
      if (!only.isEmpty() && !only.contains(name)) {
        continue;
      }
      System.out.println("capture " + name);
      final Run run = new Run(spec, scenario);
      final EngineRule engine = EngineRule.singlePartition();
      final Statement body =
          new Statement() {
            @Override
            public void evaluate() throws Throwable {
              run.execute(engine);
            }
          };
      engine.apply(body, Description.createTestDescription(Capture.class, name)).evaluate();
      write(out.resolve(name + ".trace.json"), run.trace(name, pin));
      write(out.resolve(name + ".cells.json"), run.cells(name, pin));
    }
    write(out.resolve("PIN"), null, pin + "\n");
    System.exit(0);
  }

  /**
   * Load — and so let the {@link Sites} agent instrument — every Zeebe engine class BEFORE the
   * first {@link EngineRule} starts. Zeebe's test stream processor must open within a hard-coded
   * 15 s ({@code TestStreams.buildStreamProcessor}); loading and instrumenting the engine's
   * processors lazily inside that window made a capture on a loaded host die with a
   * {@code TimeoutException} (#1521: the generative sweep captures many cases on shared workers).
   * A class that cannot be loaded (an optional dependency absent from the test classpath) is skipped:
   * it then loads lazily exactly as before.
   */
  static void preloadEngineClasses() throws IOException {
    final Path root;
    try {
      root =
          Path.of(
              io.camunda.zeebe.engine.Engine.class
                  .getProtectionDomain()
                  .getCodeSource()
                  .getLocation()
                  .toURI());
    } catch (final java.net.URISyntaxException e) {
      throw new IOException(e);
    }
    if (!Files.isDirectory(root)) {
      return;
    }
    final List<Path> classes;
    try (Stream<Path> files = Files.walk(root)) {
      classes = files.filter(f -> f.toString().endsWith(".class")).toList();
    }
    final ClassLoader loader = Capture.class.getClassLoader();
    for (final Path f : classes) {
      final String rel = root.relativize(f).toString();
      final String name =
          rel.substring(0, rel.length() - ".class".length()).replace(java.io.File.separatorChar, '.');
      if (name.endsWith("module-info") || name.endsWith("package-info")) {
        continue;
      }
      try {
        Class.forName(name, false, loader);
      } catch (final ClassNotFoundException | LinkageError e) {
        // optional dependency missing: the class keeps loading lazily
      }
    }
  }

  static void write(final Path path, final JsonNode node) throws IOException {
    write(path, node, JSON.writeValueAsString(node) + "\n");
  }

  static void write(final Path path, final JsonNode ignored, final String text)
      throws IOException {
    Files.writeString(path, text, StandardCharsets.UTF_8);
  }

  /** One scenario execution against one engine. */
  static final class Run {
    private final Normaliser spec;
    private final Path scenario;
    private final JsonNode script;
    private final ArrayNode steps = NODES.arrayNode();
    private final TreeSet<String> cells = new TreeSet<>();
    private final TreeMap<String, String> sites = new TreeMap<>();
    private final Map<Long, String> elementIds = new HashMap<>();
    private final Map<String, Long> jobs = new HashMap<>();
    private final Map<String, Long> userTasks = new HashMap<>();
    private final Map<String, Long> incidents = new HashMap<>();
    /** Canonical-key map for the WHOLE scenario (spec doc step 5) — never reset between steps. */
    private final Map<Long, Long> canonicalIds = new HashMap<>();
    private long instanceKey = -1;
    private int seen;

    Run(final Normaliser spec, final Path scenario) throws IOException {
      this.spec = spec;
      this.scenario = scenario;
      script = JSON.readTree(scenario.resolve("script.json").toFile());
    }

    void execute(final EngineRule engine) throws Exception {
      RecordingExporter.reset();
      // The client waits (deploy, create, ...) default to 5 s; on a loaded host the first
      // deployment alone (lazy FEEL/BPMN-model initialisation) can exceed that (#1521). A longer
      // ceiling only bounds a wait for records that DO arrive — it never changes a trace.
      RecordingExporter.setMaximumWaitTime(CLIENT_WAIT_MILLIS);
      final byte[] xml = Files.readAllBytes(scenario.resolve("process.bpmn"));
      Scenarios.requireDi(new String(xml, StandardCharsets.UTF_8), scenario);
      engine.deployment().withXmlResource(xml, "process.bpmn").deploy();
      if (!settle(engine)) {
        throw new IllegalStateException(
            scenario.getFileName()
                + ": the engine never became quiescent after deploy — refusing to write"
                + " goldens from an incomplete record stream");
      }
      seen = RecordingExporter.getRecords().size();
      Sites.clear();
      int index = 0;
      for (final JsonNode step : script.get("steps")) {
        apply(engine, step);
        if (!settle(engine)) {
          throw new IllegalStateException(
              scenario.getFileName()
                  + " step "
                  + index
                  + " ("
                  + step.get("op").asText()
                  + "): the engine never became quiescent — refusing to write"
                  + " goldens from an incomplete record stream");
        }
        final List<Record<?>> all = new ArrayList<>(RecordingExporter.getRecords());
        final List<Record<?>> fresh = all.subList(seen, all.size());
        seen = all.size();
        for (final Record<?> r : fresh) {
          // first pass: a variable can be written before its scope's lifecycle record
          if (r.getValueType() == ValueType.PROCESS_INSTANCE
              && r.getValue() instanceof
                  io.camunda.zeebe.protocol.record.value.ProcessInstanceRecordValue v) {
            elementIds.put(r.getKey(), v.getElementId());
          }
        }
        final ArrayNode raw = NODES.arrayNode();
        for (final Record<?> r : fresh) {
          raw.add(rawRecord(r));
        }
        final ObjectNode out = NODES.objectNode();
        out.put("step", index++);
        out.put("op", step.get("op").asText());
        out.set("records", spec.normalise(raw, canonicalIds));
        steps.add(out);
        Sites.clear();
      }
    }

    /** Zeebe record JSON plus the derived fields the normaliser spec may select. */
    private ObjectNode rawRecord(final Record<?> r) throws IOException {
      final ObjectNode node = (ObjectNode) JSON.readTree(r.toJson());
      final ObjectNode value = (ObjectNode) node.get("value");
      final String vt = r.getValueType().name();
      final String intent = r.getIntent().name();
      if (r.getValueType() == ValueType.PROCESS_INSTANCE && value.has("elementId")) {
        elementIds.put(r.getKey(), value.get("elementId").asText());
        if (r.getRecordType() == RecordType.EVENT) {
          final String elementCell = CellIds.elementCellOrNull(value.get("bpmnElementType").asText());
          if (elementCell != null) {
            cells.add(elementCell);
          }
        }
        if (instanceKey < 0 && value.has("processInstanceKey")) {
          instanceKey = value.get("processInstanceKey").asLong();
        }
      }
      if (r.getValueType() == ValueType.VARIABLE) {
        final long scope = value.get("scopeKey").asLong();
        value.put("scopeElementId", elementIds.getOrDefault(scope, "?" + scope));
        value.set("value", JSON.readTree(value.get("value").asText()));
      }
      if (r.getRecordType() == RecordType.EVENT) {
        if (r.getValueType() == ValueType.JOB && intent.equals("CREATED")) {
          jobs.put(value.get("type").asText(), r.getKey());
        }
        if (r.getValueType() == ValueType.USER_TASK && intent.equals("CREATED")) {
          userTasks.put(value.get("elementId").asText(), r.getKey());
        }
        if (r.getValueType() == ValueType.INCIDENT && intent.equals("CREATED")) {
          incidents.put(value.get("elementId").asText(), r.getKey());
          final String errorType = value.get("errorType").asText();
          cells.add(CellIds.incidentCell(errorType));
          Sites.take("failure", errorType, value.get("errorMessage").asText())
              .ifPresent(this::attribute);
        }
      }
      if (r.getRecordType() == RecordType.COMMAND_REJECTION) {
        final String type = r.getRejectionType().name();
        Sites.take("rejection", type, r.getRejectionReason()).ifPresent(this::attribute);
      }
      if (r.getRecordType() != RecordType.COMMAND) {
        cells.add(CellIds.intentCell(r.getIntent().getClass().getSimpleName(), intent));
      }
      node.put("valueType", vt);
      return node;
    }

    private void attribute(final Sites.Pending p) {
      cells.add(p.cell());
      sites.put(p.cell(), p.frame());
    }

    private void apply(final EngineRule engine, final JsonNode step) {
      final String op = step.get("op").asText();
      final boolean reject = step.path("expectRejection").asBoolean(false);
      final Map<String, Object> vars = variables(step);
      switch (op) {
        case "createInstance" -> {
          final var c =
              engine.processInstance().ofBpmnProcessId(step.get("bpmnProcessId").asText());
          final var withVars = vars.isEmpty() ? c : c.withVariables(vars);
          if (reject) {
            withVars.expectRejection().create();
          } else {
            instanceKey = withVars.create();
          }
        }
        case "completeJob" -> {
          final String type = step.get("jobType").asText();
          if (!reject) {
            engine.jobs().withType(type).activate();
          }
          final var c = engine.job().withKey(jobKey(type)).withVariables(vars);
          (reject ? c.expectRejection() : c).complete();
        }
        case "failJob" -> {
          final String type = step.get("jobType").asText();
          engine.jobs().withType(type).activate();
          final var c =
              engine
                  .job()
                  .withKey(jobKey(type))
                  .withRetries(step.get("retries").asInt())
                  .withErrorMessage(step.path("errorMessage").asText(""));
          (reject ? c.expectRejection() : c).fail();
        }
        case "completeUserTask" -> {
          final var c =
              engine
                  .userTask()
                  .withKey(require(userTasks, step.get("elementId").asText(), "user task"))
                  .withVariables(vars);
          (reject ? c.expectRejection() : c).complete();
        }
        case "publishMessage" -> {
          final var c =
              engine
                  .message()
                  .withName(step.get("name").asText())
                  .withCorrelationKey(step.get("correlationKey").asText())
                  .withTimeToLive(step.path("timeToLiveMillis").asLong(0))
                  .withVariables(vars);
          (reject ? c.expectRejection() : c).publish();
        }
        case "advanceClock" -> engine.increaseTime(Duration.ofMillis(step.get("millis").asLong()));
        case "resolveIncident" -> {
          final var c =
              engine
                  .incident()
                  .ofInstance(instanceKey)
                  .withKey(require(incidents, step.get("elementId").asText(), "incident"));
          (reject ? c.expectRejection() : c).resolve();
        }
        case "updateVariables" -> {
          final long scope =
              step.has("scopeElementId")
                  ? latestElementInstance(step.get("scopeElementId").asText())
                  : instanceKey;
          final var c = engine.variables().ofScope(scope).withDocument(vars);
          (reject ? c.expectRejection() : c).update();
        }
        case "cancelInstance" -> {
          final var c = engine.processInstance().withInstanceKey(instanceKey);
          if (reject) {
            c.expectRejection().cancel();
          } else {
            c.cancel();
          }
        }
        default -> throw new IllegalArgumentException("unknown op " + op + " in " + scenario);
      }
    }

    /** The newest element instance of a BPMN element (keys grow monotonically). */
    private long latestElementInstance(final String elementId) {
      return elementIds.entrySet().stream()
          .filter(e -> e.getValue().equals(elementId))
          .mapToLong(Map.Entry::getKey)
          .max()
          .orElseThrow(
              () -> new IllegalStateException("no element instance of '" + elementId + "' yet"));
    }

    private long jobKey(final String type) {
      return require(jobs, type, "job of type");
    }

    private static long require(final Map<String, Long> keys, final String id, final String what) {
      final Long key = keys.get(id);
      if (key == null) {
        throw new IllegalStateException("no " + what + " '" + id + "' has been created yet");
      }
      return key;
    }

    private static Map<String, Object> variables(final JsonNode step) {
      final Map<String, Object> out = new TreeMap<>();
      final JsonNode v = step.get("variables");
      if (v != null) {
        final Iterator<Map.Entry<String, JsonNode>> it = v.fields();
        while (it.hasNext()) {
          final var e = it.next();
          out.put(e.getKey(), JSON.convertValue(e.getValue(), Object.class));
        }
      }
      return out;
    }

    /**
     * Wait until the engine has processed everything and no new record appears.
     *
     * @return false when the poll budget ran out before the engine became quiescent — the caller
     *     must then fail the capture rather than treat a partial record stream as a complete step.
     */
    private static boolean settle(final EngineRule engine) throws InterruptedException {
      int last = -1;
      int stable = 0;
      for (int i = 0; i < 400 && stable < 3; i++) {
        Thread.sleep(50);
        final int now = RecordingExporter.getRecords().size();
        if (engine.hasReachedEnd() && now == last) {
          stable++;
        } else {
          stable = 0;
        }
        last = now;
      }
      return stable >= 3;
    }

    ObjectNode trace(final String name, final String pin) {
      final ObjectNode out = NODES.objectNode();
      out.put("scenario", name);
      out.put("zeebePin", pin);
      out.set("steps", steps);
      return out;
    }

    ObjectNode cells(final String name, final String pin) {
      final ObjectNode out = NODES.objectNode();
      out.put("scenario", name);
      out.put("zeebePin", pin);
      final ArrayNode list = out.putArray("cells");
      cells.forEach(list::add);
      final ObjectNode at = out.putObject("sites");
      sites.forEach(at::put);
      return out;
    }
  }
}
