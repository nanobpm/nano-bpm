/*
 * Harvests BPMN XML from the Zeebe engine-test fluent-builder models (#1249).
 *
 * The harvester (formal/harvest/harvest.mjs) statically parses each
 * `Bpmn.createExecutableProcess(...)...done()` chain in the pinned Zeebe engine
 * tests into a small intermediate representation (IR): an ordered list of the
 * builder calls. This program replays that IR through the REAL Zeebe model API
 * (`io.camunda.zeebe.model.bpmn.Bpmn`) and serialises the resulting
 * `BpmnModelInstance` to BPMN 2.0 XML — so the harvested model is exactly what
 * Zeebe's own builder produces, not a hand-rolled XML reconstruction.
 *
 * Input (stdin): a JSON array of IR programs:
 *   [ { "id": "<harvest-id>",
 *       "processId": "<bpmn process id>",
 *       "calls": [ {"m": "startEvent", "args": ["start"]}, ... ] } ]
 * Output (stdout): a JSON array, one entry per program, in the same order:
 *   [ { "id": "<harvest-id>", "ok": true,  "xml": "<bpmn xml>" }
 *   | { "id": "<harvest-id>", "ok": false, "error": "<message>" } ]
 *
 * Only the builder methods the harvester can statically resolve are replayed;
 * anything else is rejected by harvest.mjs before this program runs.
 */

import io.camunda.zeebe.model.bpmn.Bpmn;
import io.camunda.zeebe.model.bpmn.BpmnModelInstance;
import io.camunda.zeebe.model.bpmn.builder.AbstractFlowNodeBuilder;
import io.camunda.zeebe.model.bpmn.builder.ProcessBuilder;
import io.camunda.zeebe.model.bpmn.builder.StartEventBuilder;
import java.io.ByteArrayOutputStream;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public final class HarvestExtract {

  // ---- minimal JSON reader (no external dep: the extractor classpath is just
  // the pinned zeebe-bpmn-model + camunda-xml-model + slf4j-api) --------------
  static final class Json {
    private final String s;
    private int i;

    Json(String s) {
      this.s = s;
    }

    static Object parse(String s) {
      Json j = new Json(s);
      j.ws();
      Object v = j.value();
      j.ws();
      return v;
    }

    void ws() {
      while (i < s.length() && Character.isWhitespace(s.charAt(i))) i++;
    }

    char peek() {
      return i < s.length() ? s.charAt(i) : '\0';
    }

    Object value() {
      ws();
      char c = peek();
      switch (c) {
        case '{': {
          i++;
          java.util.LinkedHashMap<String, Object> m = new java.util.LinkedHashMap<>();
          ws();
          if (peek() == '}') { i++; return m; }
          for (;;) {
            ws();
            String k = (String) value();
            ws();
            i++; // ':'
            m.put(k, value());
            ws();
            char d = s.charAt(i++);
            if (d == '}') return m;
          }
        }
        case '[': {
          i++;
          List<Object> l = new ArrayList<>();
          ws();
          if (peek() == ']') { i++; return l; }
          for (;;) {
            l.add(value());
            ws();
            char d = s.charAt(i++);
            if (d == ']') return l;
          }
        }
        case '"':
          return string();
        case 't':
          i += 4; return Boolean.TRUE;
        case 'f':
          i += 5; return Boolean.FALSE;
        case 'n':
          i += 4; return null;
        default: {
          int st = i;
          while (i < s.length() && "-+0123456789.eE".indexOf(s.charAt(i)) >= 0) i++;
          String num = s.substring(st, i);
          return num.contains(".") || num.contains("e") || num.contains("E")
              ? Double.valueOf(num)
              : Long.valueOf(num);
        }
      }
    }

    String string() {
      StringBuilder b = new StringBuilder();
      i++; // opening quote
      while (true) {
        char c = s.charAt(i++);
        if (c == '"') return b.toString();
        if (c == '\\') {
          char e = s.charAt(i++);
          switch (e) {
            case 'n': b.append('\n'); break;
            case 't': b.append('\t'); break;
            case 'r': b.append('\r'); break;
            case 'b': b.append('\b'); break;
            case 'f': b.append('\f'); break;
            case 'u':
              b.append((char) Integer.parseInt(s.substring(i, i + 4), 16));
              i += 4;
              break;
            default: b.append(e);
          }
        } else {
          b.append(c);
        }
      }
    }
  }

  // ---- minimal JSON writer --------------------------------------------------
  static String toJson(Object v) {
    if (v == null) return "null";
    if (v instanceof Boolean || v instanceof Number) return v.toString();
    if (v instanceof String) {
      StringBuilder b = new StringBuilder("\"");
      for (char c : ((String) v).toCharArray()) {
        switch (c) {
          case '"': b.append("\\\""); break;
          case '\\': b.append("\\\\"); break;
          case '\n': b.append("\\n"); break;
          case '\r': b.append("\\r"); break;
          case '\t': b.append("\\t"); break;
          default:
            if (c < 0x20) b.append(String.format("\\u%04x", (int) c));
            else b.append(c);
        }
      }
      return b.append('"').toString();
    }
    if (v instanceof List) {
      StringBuilder b = new StringBuilder("[");
      boolean first = true;
      for (Object o : (List<?>) v) {
        if (!first) b.append(',');
        first = false;
        b.append(toJson(o));
      }
      return b.append(']').toString();
    }
    if (v instanceof java.util.Map) {
      StringBuilder b = new StringBuilder("{");
      boolean first = true;
      for (var e : ((java.util.Map<String, Object>) v).entrySet()) {
        if (!first) b.append(',');
        first = false;
        b.append(toJson(e.getKey())).append(':').append(toJson(e.getValue()));
      }
      return b.append('}').toString();
    }
    throw new IllegalArgumentException("cannot serialise " + v.getClass());
  }

  // ---- the replay -----------------------------------------------------------
  // The builder is fluent: each call returns a builder whose type changes as the
  // chain progresses (ProcessBuilder -> StartEventBuilder -> ... -> the generic
  // AbstractFlowNodeBuilder). We invoke reflectively by NAME so one code path
  // covers every concrete builder class without enumerating them.
  @SuppressWarnings("unchecked")
  static Object invoke(Object target, String name, List<Object> args) throws Exception {
    Class<?>[] paramTypes = new Class<?>[args.size()];
    Object[] argv = new Object[args.size()];
    for (int k = 0; k < args.size(); k++) {
      Object a = args.get(k);
      argv[k] = a;
      paramTypes[k] = a == null ? String.class : (a instanceof String ? String.class
          : a instanceof Boolean ? boolean.class : a instanceof Number ? int.class : Object.class);
    }
    Class<?> c = target.getClass();
    while (c != null) {
      // getDeclaredMethods() order is unspecified and differs between JVM runs,
      // so overloads (e.g. foo(String) vs foo(int)) must be resolved by exact
      // argument compatibility, never by "first one found" — that made the
      // harvested corpus nondeterministic across hosts. Sort for a stable order
      // and skip compiler bridges.
      Method[] declared = c.getDeclaredMethods();
      java.util.Arrays.sort(declared, java.util.Comparator.comparing(Method::toGenericString));
      for (Method m : declared) {
        if (m.isBridge() || m.isSynthetic()) continue;
        if (!m.getName().equals(name) || m.getParameterCount() != args.size()) continue;
        // Only replay no-arg and String-arg builder methods — the harvester only
        // ever emits those. A Consumer/lambda overload is never statically
        // resolvable and is rejected upstream. A model-object argument (the
        // Message/Signal built by invokeNestedEventBuilder) matches by
        // assignability so message(Message)/signal(Message) resolve.
        boolean ok = true;
        Class<?>[] pts = m.getParameterTypes();
        for (int k = 0; k < pts.length; k++) {
          Class<?> pt = pts[k];
          Object a = args.get(k);
          if (pt == String.class && (a == null || a instanceof String)) continue;
          if (pt == boolean.class && a instanceof Boolean) continue;
          if ((pt == int.class || pt == long.class || pt == double.class || pt == float.class
              || pt == short.class) && a instanceof Number) {
            argv[k] = coerceNumber((Number) a, pt);
            continue;
          }
          if (a != null && pt.isInstance(a)) continue; // model-object arg
          if (args.size() > 0) { ok = false; break; }
        }
        if (!ok) continue;
        m.setAccessible(true);
        return m.invoke(target, argv);
      }
      c = c.getSuperclass();
    }
    throw new NoSuchMethodException(
        target.getClass().getSimpleName() + "." + name + "(" + args.size() + " args)");
  }

  static Object coerceNumber(Number n, Class<?> pt) {
    if (pt == int.class) return n.intValue();
    if (pt == long.class) return n.longValue();
    if (pt == double.class) return n.doubleValue();
    if (pt == float.class) return n.floatValue();
    return n.shortValue();
  }

  // A no-arg `message()` / `signal()` emitted for a Consumer-form call
  // (`message(m -> m.name(…)…)`). There is no no-arg `message()` on the activity
  // builder — the Consumer form creates a Message and hands a MessageBuilder to
  // the lambda. Reproduce it: create an empty Message via the activity's model,
  // pass it to `message(Message)` (which attaches it to the element), then
  // return a MessageBuilder over it so the flattened `name`/`zeebeCorrelationKey`
  // calls land on the message. `signal()` likewise via SignalBuilder.
  @SuppressWarnings("unchecked")
  static Object invokeNestedEventBuilder(Object activityBuilder, String name) throws Exception {
    Class<?> msgClass = Class.forName(name.equals("signal")
        ? "io.camunda.zeebe.model.bpmn.instance.Signal"
        : "io.camunda.zeebe.model.bpmn.instance.Message");
    // every activity builder carries its BpmnModelInstance in a field on the
    // AbstractBpmnModelElementBuilder base — find it reflectively.
    Object model = null;
    Class<?> c = activityBuilder.getClass();
    while (c != null && model == null) {
      for (java.lang.reflect.Field f : c.getDeclaredFields()) {
        if (BpmnModelInstance.class.isAssignableFrom(f.getType())) {
          f.setAccessible(true);
          model = f.get(activityBuilder);
          break;
        }
      }
      c = c.getSuperclass();
    }
    if (model == null) throw new NoSuchMethodException("no BpmnModelInstance on " + activityBuilder.getClass());
    Object msg = ((BpmnModelInstance) model).newInstance(
        (Class<? extends org.camunda.bpm.model.xml.instance.ModelElementInstance>) msgClass);
    // Register the Message/Signal under <definitions> so the element's
    // messageRef can resolve it ("element is not part of model" otherwise).
    Object definitions = ((BpmnModelInstance) model).getDefinitions();
    Class<?> definitionsClass = Class.forName("io.camunda.zeebe.model.bpmn.instance.Definitions");
    java.lang.reflect.Method addChild = definitionsClass.getMethod(
        "addChildElement", org.camunda.bpm.model.xml.instance.ModelElementInstance.class);
    addChild.invoke(definitions, msg);
    // attach via message(Message) / signal(Message) on the activity builder
    invoke(activityBuilder, name, java.util.List.of(msg));
    String builderClass = name.equals("signal")
        ? "io.camunda.zeebe.model.bpmn.builder.zeebe.SignalBuilder"
        : "io.camunda.zeebe.model.bpmn.builder.zeebe.MessageBuilder";
    return Class.forName(builderClass)
        .getDeclaredConstructor(BpmnModelInstance.class, msgClass)
        .newInstance(model, msg);
  }

  @SuppressWarnings("unchecked")
  static java.util.Map<String, Object> run(java.util.Map<String, Object> prog) {
    java.util.Map<String, Object> res = new java.util.LinkedHashMap<>();
    String id = (String) prog.get("id");
    res.put("id", id);
    try {
      String processId = (String) prog.get("processId");
      Object builder = Bpmn.createExecutableProcess(processId);
      // A stack of enclosing builders. A builder method whose argument was a
      // nested config lambda (multiInstance, message, …) returns a NESTED
      // builder (the MI builder, the MessageBuilder); the harvester flattens the
      // nested lambda's calls into the IR and emits a `pop` marker where the
      // lambda ends. We push the enclosing builder before the nested calls run
      // and `pop` restores it, so the chain continues on the activity builder.
      java.util.Deque<Object> stack = new java.util.ArrayDeque<>();
      for (Object callObj : (List<Object>) prog.get("calls")) {
        java.util.Map<String, Object> call = (java.util.Map<String, Object>) callObj;
        String m = (String) call.get("m");
        List<Object> args = (List<Object>) call.get("args");
        if (m.equals("done")) {
          break;
        }
        if (m.equals("pop")) {
          // Return to the enclosing builder after a nested config lambda.
          if (!stack.isEmpty()) builder = stack.pop();
          continue;
        }
        // A method that opens a nested config lambda pushes the current builder
        // so `pop` can return to it. We detect it by the return type: if the
        // invoke returns a builder of a DIFFERENT family (the nested builder),
        // the next calls target it until `pop`.
        Object next;
        if ((m.equals("message") || m.equals("signal")) && args.isEmpty()) {
          // Consumer-form message(m -> …)/signal(s -> …): no no-arg overload
          // exists, so build the nested Message/Signal builder explicitly.
          next = invokeNestedEventBuilder(builder, m);
        } else {
          next = invoke(builder, m, args);
        }
        if (opensNestedScope(m) || returnsSubProcessBuilder(next)) {
          stack.push(builder);
        }
        builder = next;
      }
      BpmnModelInstance model = ((AbstractFlowNodeBuilder<?, ?>) builder).done();
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      Bpmn.writeModelToStream(baos, model);
      res.put("ok", Boolean.TRUE);
      res.put("xml", baos.toString(StandardCharsets.UTF_8));
    } catch (Throwable t) {
      res.put("ok", Boolean.FALSE);
      String msg = t.getMessage();
      if (msg == null && t.getCause() != null) msg = t.getCause().getMessage();
      res.put("error", t.getClass().getSimpleName() + (msg == null ? "" : ": " + msg));
    }
    return res;
  }

  // Builder methods that take a nested config lambda and return a NESTED builder
  // (the harvester flattens that lambda's calls into the IR, then emits `pop`).
  // Only these push the enclosing builder; every other call's return value simply
  // becomes the current builder (the normal fluent chain).
  private static boolean opensNestedScope(String m) {
    return m.equals("multiInstance") || m.equals("message") || m.equals("signal");
  }

  // `subProcess(id, sp -> …)` returns a SubProcessBuilder — a nested builder the
  // flattened Consumer body runs on. The harvester emits a `pop` after that body
  // (harvest.mjs opensNestedScope), so the enclosing builder must be on the
  // stack. Detecting the return type (rather than the method name) also covers
  // the plain `subProcess(id)` form when a `pop` follows, and is harmless when
  // none does (the chain simply continues on the returned builder). Without the
  // push, `pop` underflows and the outer chain continues inside the sub-process
  // scope — an outer `.endEvent()` then draws an outgoing flow from the body's
  // last end event, an invalid model rejected at deployment (#1714 review).
  private static boolean returnsSubProcessBuilder(Object next) {
    return next
        instanceof io.camunda.zeebe.model.bpmn.builder.SubProcessBuilder;
  }

  public static void main(String[] argv) throws Exception {
    String input = new String(System.in.readAllBytes(), StandardCharsets.UTF_8);
    List<Object> programs = (List<Object>) Json.parse(input);
    List<Object> out = new ArrayList<>();
    for (Object p : programs) {
      out.add(run((java.util.Map<String, Object>) p));
    }
    System.out.print(toJson(out));
  }
}
