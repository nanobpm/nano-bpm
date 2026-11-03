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
      for (Method m : c.getDeclaredMethods()) {
        if (!m.getName().equals(name) || m.getParameterCount() != args.size()) continue;
        // Only replay no-arg and String-arg builder methods — the harvester only
        // ever emits those. A Consumer/lambda overload is never statically
        // resolvable and is rejected upstream.
        boolean ok = true;
        for (Class<?> pt : m.getParameterTypes()) {
          if (pt != String.class && !pt.isPrimitive() && args.size() > 0) { ok = false; break; }
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

  @SuppressWarnings("unchecked")
  static java.util.Map<String, Object> run(java.util.Map<String, Object> prog) {
    java.util.Map<String, Object> res = new java.util.LinkedHashMap<>();
    String id = (String) prog.get("id");
    res.put("id", id);
    try {
      String processId = (String) prog.get("processId");
      Object builder = Bpmn.createExecutableProcess(processId);
      for (Object callObj : (List<Object>) prog.get("calls")) {
        java.util.Map<String, Object> call = (java.util.Map<String, Object>) callObj;
        String m = (String) call.get("m");
        List<Object> args = (List<Object>) call.get("args");
        if (m.equals("done")) {
          break;
        }
        builder = invoke(builder, m, args);
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
