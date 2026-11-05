/*
 * Part of the nano Zeebe differential-trace harness (formal/traces, #1391).
 */
package io.nanobpm.traces;

/**
 * Maps a captured Zeebe raising site (a stack frame) to the per-site cell ids shared with {@code
 * formal/parity/extract.mjs}. This is the SHARED CONTRACT, word for word:
 *
 * <ul>
 *   <li>{@code failure:<Class>.<method>:<ErrorType>}
 *   <li>{@code rejection:<Class>.<method>:<RejectionType>}
 * </ul>
 *
 * {@code <Class>} is the simple (unqualified) name of the enclosing <em>top-level</em> Java class
 * of the construction site; {@code <method>} the enclosing named method (a construction inside a
 * lambda is attributed to the enclosing named method; a field initializer / static block has no
 * method and yields the empty string; a constructor is named after its class, as in source);
 * {@code <ErrorType>}/{@code <RejectionType>} the Zeebe enum constant.
 */
public final class CellIds {

  private CellIds() {}

  /** Simple name of the enclosing top-level class of a (possibly nested) binary class name. */
  public static String topLevelClass(final String binaryClassName) {
    final String simple = binaryClassName.substring(binaryClassName.lastIndexOf('.') + 1);
    final int nested = simple.indexOf('$');
    return nested < 0 ? simple : simple.substring(0, nested);
  }

  /** Simple name of the innermost class of a binary class name (a constructor's source name). */
  static String innermostClass(final String binaryClassName) {
    final String simple = binaryClassName.substring(binaryClassName.lastIndexOf('.') + 1);
    return simple.substring(simple.lastIndexOf('$') + 1);
  }

  /**
   * The enclosing <em>named</em> method of a JVM frame method name. javac names a lambda body
   * {@code lambda$<enclosingMethod>$<n>} (also when nested in another lambda of the same method),
   * so the lambda case resolves to {@code <enclosingMethod>}. A lambda in a field initializer is
   * {@code lambda$new$n} / {@code lambda$static$n} and, like {@code <clinit>}, has no enclosing
   * method (empty string). A constructor ({@code <init>}) is named after its class.
   */
  public static String enclosingMethod(final String binaryClassName, final String frameMethod) {
    String m = frameMethod;
    if (m.startsWith("lambda$")) {
      final String rest = m.substring("lambda$".length());
      final int end = rest.lastIndexOf('$');
      m = end < 0 ? rest : rest.substring(0, end);
      if (m.equals("new") || m.equals("static")) {
        return "";
      }
    }
    if (m.equals("<clinit>")) {
      return "";
    }
    if (m.equals("<init>")) {
      return innermostClass(binaryClassName);
    }
    return m;
  }

  /** {@code <Class>.<method>} of a frame. */
  public static String site(final String binaryClassName, final String frameMethod) {
    return topLevelClass(binaryClassName) + "." + enclosingMethod(binaryClassName, frameMethod);
  }

  public static String failure(
      final String binaryClassName, final String frameMethod, final String errorType) {
    return "failure:" + site(binaryClassName, frameMethod) + ":" + errorType;
  }

  public static String rejection(
      final String binaryClassName, final String frameMethod, final String rejectionType) {
    return "rejection:" + site(binaryClassName, frameMethod) + ":" + rejectionType;
  }

  /** {@code SERVICE_TASK} -> {@code ServiceTask} (the {@code element:} cell grammar). */
  public static String elementCell(final String bpmnElementType) {
    final StringBuilder out = new StringBuilder("element:");
    for (final String part : bpmnElementType.split("_")) {
      if (!part.isEmpty()) {
        out.append(part.charAt(0)).append(part.substring(1).toLowerCase(java.util.Locale.ROOT));
      }
    }
    return out.toString();
  }

  /**
   * The {@code element:} cell a PROCESS_INSTANCE record touches, or null when the record is the
   * process instance itself: {@code extract.mjs} (the shared grammar's source of truth) emits no
   * {@code element:Process} cell — BPMN {@code <process>} is the container, not a flow element — so
   * recording one here would be grammar drift the parity guard rejects. Likewise {@code
   * MULTI_INSTANCE_BODY}: a runtime wrapper Zeebe puts around a multi-instance activity, not a
   * BPMN flow element ({@code FlowElementValidator} has no such class) — the activity's own cell
   * is touched by its children.
   */
  public static String elementCellOrNull(final String bpmnElementType) {
    return "PROCESS".equals(bpmnElementType) || "MULTI_INSTANCE_BODY".equals(bpmnElementType)
        ? null
        : elementCell(bpmnElementType);
  }

  /**
   * {@code ProcessInstanceIntent}/{@code ELEMENT_ACTIVATED} -> {@code
   * intent:ProcessInstance:ELEMENT_ACTIVATED} (the {@code intent:} cell grammar: the intent enum's
   * simple class name without its {@code Intent} suffix).
   */
  public static String intentCell(final String intentClassSimpleName, final String intent) {
    final String stem =
        intentClassSimpleName.endsWith("Intent")
            ? intentClassSimpleName.substring(0, intentClassSimpleName.length() - "Intent".length())
            : intentClassSimpleName;
    return "intent:" + stem + ":" + intent;
  }

  /** {@code incident:<ErrorType>} (the coarse incident cell grammar). */
  public static String incidentCell(final String errorType) {
    return "incident:" + errorType;
  }
}
