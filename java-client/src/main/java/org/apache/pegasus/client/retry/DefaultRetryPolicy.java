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
import java.util.concurrent.TimeUnit;
import org.apache.pegasus.client.ClientOptions;

/**
 * The default retry policy, which is the only policy before we introduce the retry policy
 * mechanism. It only considers the timeout value to calculate the retry delay, that's why we need
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
    long timeoutNanos = timeout.toNanos();
    long retryDelayNanos =
        Math.min(
            timeoutNanos > 3000000 ? timeoutNanos / 3000000 : 1,
            TimeUnit.NANOSECONDS.toNanos(deadlineNanos - now));
    return new RetryAction(RetryDecision.RETRY, Duration.ofNanos(retryDelayNanos), "");
  }
}
