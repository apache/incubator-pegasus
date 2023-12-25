package org.apache.pegasus.client.retry;

import java.time.Duration;

public interface RetryPolicy {

  enum RetryDecision {
    FAIL,
    RETRY
  }

  final class RetryAction {
    private final RetryDecision decision;

    private final Duration delay;

    private final String reason;

    public RetryAction(RetryDecision decision, Duration delay, String reason) {
      this.decision = decision;
      this.delay = delay;
      this.reason = reason;
    }

    public RetryDecision getDecision() {
      return decision;
    }

    public Duration getDelay() {
      return delay;
    }

    public String getReason() {
      return reason;
    }
  }

  RetryAction shouldRetry(int retries, long deadlineNanos, Duration timeout);
}
