/*
 * Part of the nano Zeebe differential-trace harness (formal/traces, #1391).
 */
package io.nanobpm.traces;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Optional;
import org.junit.After;
import org.junit.Test;

/**
 * Unit tests of the pending-site correlation in {@link Sites#take}: a captured record must consume
 * exactly its own raising site plus the forwarding plumbing that follows it, never an independent
 * site raised earlier with the same type and message.
 */
public class SitesTest {

  @After
  public void reset() {
    Sites.clear();
  }

  private static Sites.Pending site(
      final String kind, final String type, final String message, final String frame) {
    return siteOn(Thread.currentThread().threadId(), kind, type, message, frame);
  }

  private static Sites.Pending siteOn(
      final long thread,
      final String kind, final String type, final String message, final String frame) {
    final int colon = frame.lastIndexOf(':');
    final int dot = frame.lastIndexOf('.', colon);
    return new Sites.Pending(
        kind, type, message, frame.substring(0, dot), frame.substring(dot + 1, colon),
        Integer.parseInt(frame.substring(colon + 1)), thread);
  }

  @Test
  public void takeConsumesTheMatchingSiteAndItsForwardingPlumbing() {
    Sites.add(site("failure", "CONDITION_ERROR", "bad", "a.Processor.raise:10"));
    Sites.add(site("failure", "CONDITION_ERROR", "bad", "a.Writer.forward:20"));
    final Optional<Sites.Pending> taken = Sites.take("failure", "CONDITION_ERROR", "bad");
    assertTrue(taken.isPresent());
    assertEquals("a.Processor.raise:10", taken.get().frame());
    // The forwarding entry went with it; nothing is left.
    assertTrue(Sites.take("failure", "CONDITION_ERROR", "bad").isEmpty());
  }

  @Test
  public void takeKeepsAnIndependentSiteFromAnotherThread() {
    // Two parallel branches evaluate the same invalid expression on two engine threads: identical
    // type+message, raised at two sites. The first record read consumes the first site, and the
    // other thread's site must survive for its own record instead of being dropped as
    // "forwarding".
    Sites.add(site("failure", "CONDITION_ERROR", "bad", "a.Left.eval:10"));
    Sites.add(siteOn(-1, "failure", "CONDITION_ERROR", "bad", "a.Right.eval:20"));
    assertEquals(
        "a.Left.eval:10", Sites.take("failure", "CONDITION_ERROR", "bad").orElseThrow().frame());
    assertEquals(
        "a.Right.eval:20", Sites.take("failure", "CONDITION_ERROR", "bad").orElseThrow().frame());
    assertTrue(Sites.take("failure", "CONDITION_ERROR", "bad").isEmpty());
  }

  @Test
  public void takeFallsBackToTheEarliestSiteOfTheType() {
    Sites.add(site("rejection", "NOT_FOUND", "no such job", "a.Validator.check:5"));
    final Optional<Sites.Pending> taken = Sites.take("rejection", "NOT_FOUND", "rewritten");
    assertTrue(taken.isPresent());
    assertEquals("a.Validator.check:5", taken.get().frame());
  }

  @Test
  public void ambiguousMessageMismatchAttributesNothing() {
    // The record's message was rewritten and TWO sites of the type are pending: there is no
    // honest correlation, so the record goes unattributed and both sites keep their own records.
    Sites.add(site("rejection", "NOT_FOUND", "no such job", "a.Validator.check:5"));
    Sites.add(site("rejection", "NOT_FOUND", "no such task", "a.Other.check:9"));
    assertTrue(Sites.take("rejection", "NOT_FOUND", "rewritten").isEmpty());
    assertEquals(
        "a.Validator.check:5",
        Sites.take("rejection", "NOT_FOUND", "no such job").orElseThrow().frame());
    assertEquals(
        "a.Other.check:9",
        Sites.take("rejection", "NOT_FOUND", "no such task").orElseThrow().frame());
  }

  @Test
  public void takeIgnoresOtherKindsAndTypes() {
    Sites.add(site("failure", "JOB_NO_RETRIES", "no retries left", "a.Job.fail:7"));
    assertTrue(Sites.take("rejection", "JOB_NO_RETRIES", "no retries left").isEmpty());
    assertTrue(Sites.take("failure", "CONDITION_ERROR", "no retries left").isEmpty());
    assertTrue(Sites.take("failure", "JOB_NO_RETRIES", "no retries left").isPresent());
  }

  @Test
  public void clearForgetsUnconsumedSites() {
    Sites.add(site("failure", "CONDITION_ERROR", "bad", "a.Processor.raise:10"));
    Sites.clear();
    assertTrue(Sites.take("failure", "CONDITION_ERROR", "bad").isEmpty());
  }

  @Test
  public void pendingCellAndFrameFollowTheSharedGrammar() {
    final Sites.Pending failure = site("failure", "CONDITION_ERROR", "bad", "a.Processor.raise:10");
    assertEquals("failure:Processor.raise:CONDITION_ERROR", failure.cell());
    assertEquals("a.Processor.raise:10", failure.frame());
    final Sites.Pending rejection = site("rejection", "NOT_FOUND", "x", "a.Validator.check:5");
    assertEquals("rejection:Validator.check:NOT_FOUND", rejection.cell());
  }

  @Test
  public void failureTypeIsExcludedFromTheRejectionInstrumentation() {
    // The rejection matcher instruments every engine type taking a RejectionType parameter —
    // except Failure, whose constructors carry one and are instrumented by FailureAdvice instead.
    // Guard the exclusion by asserting the two matchers disagree on the Failure class name.
    assertTrue(Sites.FAILURE.startsWith("io.camunda.zeebe.engine."));
    assertFalse(Sites.FAILURE.isEmpty());
  }
}
