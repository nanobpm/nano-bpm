/*
 * Part of the nano Zeebe differential-trace harness (formal/traces, #1391).
 */
package io.nanobpm.traces;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

/** Job addressing for completeJob/failJob steps, incl. concurrent same-type jobs (#1964). */
public class JobKeysTest {

  @Test
  public void concurrentSameTypeJobsAreCompletedDistinctly() {
    final JobKeys jobs = new JobKeys();
    jobs.created("T1", 10);
    jobs.created("T1", 20);
    assertEquals(10, jobs.target("T1"));
    jobs.ended("T1", 10);
    assertEquals(20, jobs.target("T1"));
  }

  @Test
  public void loopReCreationTargetsTheNewJob() {
    final JobKeys jobs = new JobKeys();
    jobs.created("T", 1);
    jobs.ended("T", 1);
    jobs.created("T", 2);
    assertEquals(2, jobs.target("T"));
  }

  @Test
  public void noneOutstandingFallsBackToNewestCreated() {
    final JobKeys jobs = new JobKeys();
    jobs.created("T", 1);
    jobs.created("T", 2);
    jobs.ended("T", 1);
    jobs.ended("T", 2);
    assertEquals(2, jobs.target("T"));
  }

  @Test(expected = IllegalStateException.class)
  public void unknownTypeIsAnError() {
    new JobKeys().target("X");
  }

  @Test
  public void terminalIntents() {
    assertEquals(true, JobKeys.isTerminal("COMPLETED"));
    assertEquals(true, JobKeys.isTerminal("CANCELED"));
    assertEquals(true, JobKeys.isTerminal("ERROR_THROWN"));
    assertEquals(false, JobKeys.isTerminal("FAILED"));
  }
}
