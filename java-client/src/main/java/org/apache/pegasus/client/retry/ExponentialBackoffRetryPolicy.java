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
