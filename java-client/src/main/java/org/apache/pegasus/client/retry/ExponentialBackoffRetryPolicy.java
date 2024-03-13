/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pegasus.client.retry;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.pegasus.client.ClientOptions;

/** A retry policy which generates retry interval with exponential backoff policy. */
public class ExponentialBackoffRetryPolicy implements RetryPolicy {

  private static final int[] RETRY_BACKOFF = {1, 2, 3, 5, 10, 20, 40, 100, 100, 100, 100, 200, 200};

  private final long retryBaseIntervalNanos;

  private final int retryMaxTimes;

  public ExponentialBackoffRetryPolicy(ClientOptions opts) {
    // The minimum unit of retry interval time for user is milliseconds.
    // When the user sets the interval time unit to nano, it will change to default interval ms.
    if (opts.getRetryBaseInterval().toMillis() < 1) {
      this.retryBaseIntervalNanos = ClientOptions.DEFAULT_RETRY_BASE_INTERVAL_MS * 1000000;
    } else {
      this.retryBaseIntervalNanos = opts.getRetryBaseInterval().toNanos();
    }
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
    long normalIntervalNanos =
        retryBaseIntervalNanos * RETRY_BACKOFF[Math.min(retries, RETRY_BACKOFF.length - 1)];
    // 1% possible jitter
    long jitterNanos =
        (long) (normalIntervalNanos * ThreadLocalRandom.current().nextFloat() * 0.01f);
    long retryIntervalNanos =
        Math.min(
            normalIntervalNanos + jitterNanos, TimeUnit.NANOSECONDS.toNanos(deadlineNanos - now));
    return new RetryAction(RetryDecision.RETRY, Duration.ofNanos(retryIntervalNanos), "");
  }
}
