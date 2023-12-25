package org.apache.pegasus.client.retry;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.apache.pegasus.client.ClientOptions;

/**
 * The default retry policy, which is the only policy before we introduce the retry policy
 * mechanism. It only considers the timeout value to calculate the retry delay, that's why\ we need
 * to pass the {@code timeout} value in the {@link #shouldRetry(int, long, Duration)} method.
 */
public class DefaultRetryPolicy implements RetryPolicy {
  public DefaultRetryPolicy(ClientOptions opts) {
    // do nothing, just keep the constructor.
  }

  @Override
  public RetryAction shouldRetry(int retries, long deadlineNanos, Duration timeout) {
    long now = System.nanoTime();
    if (now >= deadlineNanos) {
      return new RetryAction(RetryDecision.FAIL, Duration.ZERO, "request deadline reached");
    }
    long timeoutMs = timeout.toMillis();
    long retryDelayMs =
        Math.min(
            timeoutMs > 3 ? timeoutMs / 3 : 1, TimeUnit.NANOSECONDS.toMillis(deadlineNanos - now));
    return new RetryAction(RetryDecision.RETRY, Duration.ofMillis(retryDelayMs), "");
  }
}
