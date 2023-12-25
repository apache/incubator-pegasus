package org.apache.pegasus.client.retry;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.pegasus.client.ClientOptions;

/** A retry policy which generates retry interval with exponential backoff policy. */
public class ExponentialBackoffRetryPolicy implements RetryPolicy {

  private static final int[] RETRY_BACKOFF = {1, 2, 3, 5, 10, 20, 40, 100, 100, 100, 100, 200, 200};

  private final long retryBaseIntervalMs;

  private final int retryMaxTimes;

  public ExponentialBackoffRetryPolicy(ClientOptions opts) {
    this.retryBaseIntervalMs = opts.getRetryBaseInterval().toMillis();
    this.retryMaxTimes = opts.getRetryMaxTimes();
  }

  @Override
  public RetryAction shouldRetry(int retries, long deadlineNanos, Duration timeout) {
    if (retries >= retryMaxTimes) {
      return new RetryAction(
          RetryDecision.FAIL, Duration.ZERO, "max retry times " + retryMaxTimes + " reached");
    }
    long now = System.nanoTime();
    if (now >= deadlineNanos) {
      return new RetryAction(RetryDecision.FAIL, Duration.ZERO, "request deadline reached");
    }
    long normalIntervalMs =
        retryBaseIntervalMs * RETRY_BACKOFF[Math.min(retries, RETRY_BACKOFF.length - 1)];
    // 1% possible jitter
    long jitterMs = (long) (normalIntervalMs * ThreadLocalRandom.current().nextFloat() * 0.01f);
    long retryIntervalMs =
        Math.min(normalIntervalMs + jitterMs, TimeUnit.NANOSECONDS.toMillis(deadlineNanos - now));
    return new RetryAction(RetryDecision.RETRY, Duration.ofMillis(retryIntervalMs), "");
  }
}
