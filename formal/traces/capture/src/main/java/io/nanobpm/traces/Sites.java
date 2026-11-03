/*
 * Part of the nano Zeebe differential-trace harness (formal/traces, #1391).
 */
package io.nanobpm.traces;

import static net.bytebuddy.matcher.ElementMatchers.hasParameters;
import static net.bytebuddy.matcher.ElementMatchers.hasType;
import static net.bytebuddy.matcher.ElementMatchers.isAbstract;
import static net.bytebuddy.matcher.ElementMatchers.isConstructor;
import static net.bytebuddy.matcher.ElementMatchers.isInterface;
import static net.bytebuddy.matcher.ElementMatchers.isMethod;
import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.nameStartsWith;
import static net.bytebuddy.matcher.ElementMatchers.not;
import static net.bytebuddy.matcher.ElementMatchers.whereAny;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import net.bytebuddy.agent.ByteBuddyAgent;
import net.bytebuddy.agent.builder.AgentBuilder;
import net.bytebuddy.asm.Advice;

/**
 * Raising-site attribution AT CAPTURE TIME. A ByteBuddy agent instruments, inside the JVM running
 * the pinned Zeebe engine:
 *
 * <ul>
 *   <li>every {@code Failure} constructor — the raising site of an incident is the frame that
 *       constructed the {@code Failure} (exactly the {@code new Failure(..)} site {@code
 *       formal/parity/extract.mjs} keys {@code failure:} cells on);
 *   <li>every engine method/constructor taking a {@code RejectionType} argument — the raising
 *       site of a rejection is the caller of the <em>outermost</em> such call on the stack, i.e.
 *       the frame that names {@code RejectionType.X} and hands it to the rejection plumbing.
 * </ul>
 *
 * A captured incident/rejection record is then matched to the earliest pending site with the same
 * type and message (a processor that defers its rejection through a {@code CommandControl} is
 * recorded before the writer that later appends it, so earliest-wins picks the processor).
 */
public final class Sites {

  static final String FAILURE = "io.camunda.zeebe.engine.processing.common.Failure";
  static final String REJECTION_TYPE = "io.camunda.zeebe.protocol.record.RejectionType";
  static final String ERROR_TYPE = "io.camunda.zeebe.protocol.record.value.ErrorType";

  /** A site observed while a type was raised. */
  public record Pending(String kind, String type, String message, String cls, String method,
      int line, long thread) {
    String cell() {
      return kind.equals("failure")
          ? CellIds.failure(cls, method, type)
          : CellIds.rejection(cls, method, type);
    }

    String frame() {
      return cls + "." + method + ":" + line;
    }
  }

  private static final List<Pending> PENDING = new ArrayList<>();
  private static final ThreadLocal<int[]> DEPTH = ThreadLocal.withInitial(() -> new int[1]);

  private Sites() {}

  public static void install() {
    new AgentBuilder.Default()
        .disableClassFormatChanges()
        .with(AgentBuilder.RedefinitionStrategy.RETRANSFORMATION)
        .with(AgentBuilder.Listener.StreamWriting.toSystemError().withErrorsOnly())
        .ignore(not(nameStartsWith("io.camunda.zeebe.engine.")))
        .type(named(FAILURE))
        .transform(
            (builder, type, loader, module, domain) ->
                builder.visit(Advice.to(FailureAdvice.class).on(isConstructor())))
        .type(nameStartsWith("io.camunda.zeebe.engine.").and(not(isInterface()))
            .and(not(named(FAILURE))))
        .transform(
            (builder, type, loader, module, domain) ->
                builder
                    .visit(
                        Advice.to(RejectionAdvice.class)
                            .on(
                                isMethod()
                                    .and(not(isAbstract()))
                                    .and(hasParameters(whereAny(hasType(named(REJECTION_TYPE)))))))
                    .visit(
                        // constructors cannot catch throwables: exit advice without onThrowable
                        Advice.to(RejectionCtorAdvice.class)
                            .on(
                                isConstructor()
                                    .and(hasParameters(whereAny(hasType(named(REJECTION_TYPE))))))))
        .installOn(ByteBuddyAgent.install());
  }

  /** The frame {@code skip} levels above the instrumented code's caller chain. */
  private static StackWalker.StackFrame caller(final int skip) {
    return StackWalker.getInstance()
        .walk(s -> s.skip(skip).findFirst())
        .orElseThrow(() -> new IllegalStateException("no caller frame"));
  }

  static synchronized void add(final Pending p) {
    PENDING.add(p);
  }

  /** Called from the inlined Failure-constructor advice. */
  public static void failure(final Object[] args) {
    String errorType = "";
    String message = null;
    for (final Object a : args) {
      if (a instanceof Enum<?> e && e.getDeclaringClass().getName().equals(ERROR_TYPE)) {
        errorType = e.name();
      } else if (a instanceof String s) {
        message = s;
      }
    }
    // [0]=Sites.caller, [1]=Sites.failure, [2]=Failure.<init> (advice inlined), [3]=the site.
    final StackWalker.StackFrame f = caller(3);
    add(new Pending("failure", errorType, message, f.getClassName(), f.getMethodName(),
        f.getLineNumber(), Thread.currentThread().threadId()));
  }

  /** Called from the inlined RejectionType-parameter advice on entry. */
  public static void rejectionEnter(final Object[] args) {
    final int[] depth = DEPTH.get();
    if (depth[0]++ > 0) {
      return; // only the outermost RejectionType-taking call names the site
    }
    String type = null;
    String message = null;
    for (final Object a : args) {
      if (a instanceof Enum<?> e && e.getDeclaringClass().getName().equals(REJECTION_TYPE)) {
        type = e.name();
      } else if (a instanceof String s && message == null) {
        message = s;
      }
    }
    if (type == null || type.equals("NULL_VAL") || type.equals("SBE_UNKNOWN")) {
      return;
    }
    // [0]=Sites.caller, [1]=Sites.rejectionEnter, [2]=the instrumented method, [3]=its caller.
    final StackWalker.StackFrame f = caller(3);
    add(new Pending("rejection", type, message, f.getClassName(), f.getMethodName(),
        f.getLineNumber(), Thread.currentThread().threadId()));
  }

  public static void rejectionExit() {
    DEPTH.get()[0]--;
  }

  /**
   * Consume the pending site of {@code kind} with this {@code type} whose message equals {@code
   * message}, plus any LATER entries with the same type+message recorded on the SAME thread: those
   * are the plumbing that forwarded the same rejection/failure (a processor records its site
   * before the writer appends the record, so forwarding strictly follows the original site on that
   * thread). Entries from another thread are kept: they were raised independently (e.g. the same
   * invalid expression evaluated on two parallel branches) and must stay attributable to their own
   * records.
   *
   * <p>When no entry carries the record's exact message, the record's message was rewritten by
   * the plumbing. In that case the earliest entry of the type is consumed only when it is the
   * ONLY pending entry of that type+kind; with several candidates there is no honest correlation
   * and the record goes unattributed rather than stealing an independent site's attribution.
   */
  public static synchronized Optional<Pending> take(
      final String kind, final String type, final String message) {
    int hit = -1;
    for (int i = 0; i < PENDING.size(); i++) {
      final Pending p = PENDING.get(i);
      if (p.kind().equals(kind) && p.type().equals(type) && java.util.Objects.equals(p.message(),
          message)) {
        hit = i;
        break;
      }
    }
    if (hit < 0) {
      int candidate = -1;
      int count = 0;
      for (int i = 0; i < PENDING.size(); i++) {
        final Pending p = PENDING.get(i);
        if (p.kind().equals(kind) && p.type().equals(type)) {
          if (count == 0) {
            candidate = i;
          }
          count++;
        }
      }
      if (count != 1) {
        return Optional.empty();
      }
      hit = candidate;
    }
    final Pending found = PENDING.remove(hit);
    // Forwarding plumbing follows the site that raised the record: drop later same-type+message
    // entries — but only those recorded on THIS thread. An entry from another thread (a sibling
    // branch evaluating the same invalid expression in parallel) is an independent raising site
    // and must survive for its own record.
    final long thread = found.thread();
    PENDING.subList(hit, PENDING.size())
        .removeIf(
            p -> p.thread() == thread && p.kind().equals(kind) && p.type().equals(type)
                && java.util.Objects.equals(p.message(), found.message()));
    return Optional.of(found);
  }

  /** Forget unconsumed sites (validations that raised no record) at a step boundary. */
  public static synchronized void clear() {
    PENDING.clear();
  }

  public static final class FailureAdvice {
    @Advice.OnMethodExit
    static void exit(@Advice.AllArguments final Object[] args) {
      Sites.failure(args);
    }
  }

  public static final class RejectionAdvice {
    @Advice.OnMethodEnter
    static void enter(@Advice.AllArguments final Object[] args) {
      Sites.rejectionEnter(args);
    }

    @Advice.OnMethodExit(onThrowable = Throwable.class)
    static void exit() {
      Sites.rejectionExit();
    }
  }

  public static final class RejectionCtorAdvice {
    @Advice.OnMethodEnter
    static void enter(@Advice.AllArguments final Object[] args) {
      Sites.rejectionEnter(args);
    }

    @Advice.OnMethodExit
    static void exit() {
      Sites.rejectionExit();
    }
  }
}
