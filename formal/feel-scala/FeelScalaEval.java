// feel-scala reference evaluator for the FEEL differential fuzz (#1391 step 5).
//
// Reads the Lean-generated corpus (`expression \t context \t lean-outcome`) on
// stdin and prints one canonical outcome per row (same order) as evaluated by
// feel-scala — the FEEL engine Zeebe embeds, at the exact version the pinned
// Zeebe declares (resolved by fetch-feel-scala.sh). The canonical outcome form
// is byte-identical to `Feel.Outcome.canon` / feel_diff.rs `canon_outcome`:
// `err`, `ok:null`, `ok:bool:<b>`, `ok:num:<n>`, `ok:str:<s>`.
//
// Error vs null is classified exactly as Zeebe does (FeelExpressionLanguage):
// an `EvaluationResult.isFailure()` is an evaluation failure (`err`); a success
// whose value is null is `ok:null`, even when feel-scala recorded suppressed
// failures (Zeebe ignores those, it only surfaces them as warnings).
//
// Run as a single-file source program (JEP 330), no build tool:
//   java -cp feel-engine-<v>-complete.jar FeelScalaEval.java < corpus.tsv
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import org.camunda.feel.api.EvaluationResult;
import org.camunda.feel.api.FeelEngineApi;
import org.camunda.feel.api.FeelEngineBuilder;

public class FeelScalaEval {
  static Map<String, Object> parseCtx(String enc) {
    Map<String, Object> ctx = new HashMap<>();
    if (enc.isEmpty()) return ctx;
    for (String piece : enc.split(";", -1)) {
      int eq = piece.indexOf('=');
      if (eq < 0) throw new IllegalArgumentException("bad context piece " + piece);
      String name = piece.substring(0, eq);
      String rhs = piece.substring(eq + 1);
      Object v;
      if (rhs.equals("null")) v = null;
      else if (rhs.equals("bool:true")) v = Boolean.TRUE;
      else if (rhs.equals("bool:false")) v = Boolean.FALSE;
      else if (rhs.startsWith("int:")) v = Long.parseLong(rhs.substring(4));
      else if (rhs.startsWith("str:")) v = rhs.substring(4);
      else throw new IllegalArgumentException("bad context value " + rhs);
      ctx.put(name, v);
    }
    return ctx;
  }

  static String canonNumber(BigDecimal d) {
    BigDecimal s = d.stripTrailingZeros();
    return s.scale() <= 0 ? s.toBigInteger().toString() : s.toPlainString();
  }

  static String canonValue(Object v) {
    if (v == null) return "null";
    if (v instanceof Boolean b) return "bool:" + b;
    if (v instanceof String s) return "str:" + s;
    if (v instanceof BigDecimal d) return "num:" + canonNumber(d);
    if (v instanceof scala.math.BigDecimal d) return "num:" + canonNumber(d.bigDecimal());
    if (v instanceof Number n) return "num:" + canonNumber(new BigDecimal(n.toString()));
    return "other:" + v.getClass().getName();
  }

  public static void main(String[] args) throws Exception {
    FeelEngineApi engine = FeelEngineBuilder.forJava().build();
    BufferedReader in =
        new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
    PrintStream out = new PrintStream(System.out, false, StandardCharsets.UTF_8);
    String line;
    while ((line = in.readLine()) != null) {
      if (line.isEmpty()) continue;
      String[] f = line.split("\t", -1);
      EvaluationResult r = engine.evaluateExpression(f[0], parseCtx(f.length > 1 ? f[1] : ""));
      out.println(r.isFailure() ? "err" : "ok:" + canonValue(r.result()));
    }
    out.flush();
  }
}
