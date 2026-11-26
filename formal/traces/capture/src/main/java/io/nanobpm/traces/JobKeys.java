/*
 * Part of the nano Zeebe differential-trace harness (formal/traces, #1391).
 */
package io.nanobpm.traces;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * The jobs of a scenario, per job type, for addressing a {@code completeJob}/{@code failJob} step.
 *
 * <p>Several jobs of one type can be outstanding at once (two parallel branches reaching the same
 * service task type, #1964). A step targets the OLDEST still-outstanding job of its type, so N
 * completions of a type retire N distinct jobs. Only when none is outstanding does it fall back to
 * the newest created job (so a scenario expecting a rejection on an already-completed job still
 * addresses that job).
 */
final class JobKeys {
  private final Map<String, Set<Long>> outstanding = new HashMap<>();
  private final Map<String, Deque<Long>> created = new HashMap<>();

  /** A JOB CREATED event. */
  void created(final String type, final long key) {
    outstanding.computeIfAbsent(type, t -> new LinkedHashSet<>()).add(key);
    created.computeIfAbsent(type, t -> new ArrayDeque<>()).addLast(key);
  }

  /** A JOB event that ends the job (COMPLETED, CANCELED, ERROR_THROWN). */
  void ended(final String type, final long key) {
    final Set<Long> open = outstanding.get(type);
    if (open != null) {
      open.remove(key);
    }
  }

  /** Whether a JOB intent ends the job. */
  static boolean isTerminal(final String intent) {
    return intent.equals("COMPLETED") || intent.equals("CANCELED") || intent.equals("ERROR_THROWN");
  }

  /** The key a step on {@code type} targets. */
  long target(final String type) {
    final Set<Long> open = outstanding.get(type);
    if (open != null && !open.isEmpty()) {
      return open.iterator().next();
    }
    final Deque<Long> all = created.get(type);
    if (all == null || all.isEmpty()) {
      throw new IllegalStateException("no job of type '" + type + "' has been created yet");
    }
    return all.peekLast();
  }
}
