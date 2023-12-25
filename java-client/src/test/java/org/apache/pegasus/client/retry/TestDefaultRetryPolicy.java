package org.apache.pegasus.client.retry;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

public class TestDefaultRetryPolicy {

  @Test
  public void testShouldRetry() {
    DefaultRetryPolicy policy = new DefaultRetryPolicy(null);
    long now = System.nanoTime();
    RetryPolicy.RetryAction action = policy.shouldRetry(1, now - 100, Duration.ofMillis(100));
    assertEquals(RetryPolicy.RetryDecision.FAIL, action.getDecision());

    now = System.nanoTime();
    action = policy.shouldRetry(1, now + TimeUnit.MINUTES.toNanos(1), Duration.ofMillis(300));
    assertEquals(RetryPolicy.RetryDecision.RETRY, action.getDecision());
    assertEquals(100, action.getDelay().toMillis());

    now = System.nanoTime();
    action = policy.shouldRetry(1, now + TimeUnit.SECONDS.toNanos(1), Duration.ofSeconds(10));
    assertEquals(RetryPolicy.RetryDecision.RETRY, action.getDecision());
    // should not have a delay which makes the nanos greater than deadline
    assertThat(action.getDelay().toMillis(), lessThanOrEqualTo(TimeUnit.SECONDS.toMillis(1)));
  }
}
