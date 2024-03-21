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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.apache.pegasus.client.ClientOptions;
import org.junit.jupiter.api.Test;

public class TestExponentialBackoffRetryPolicy {

  @Test
  public void test() {
    ClientOptions opts =
        ClientOptions.builder().retryBaseInterval(Duration.ofMillis(10)).retryMaxTimes(200).build();
    ExponentialBackoffRetryPolicy policy = new ExponentialBackoffRetryPolicy(opts);

    long now = System.nanoTime();
    RetryPolicy.RetryAction action = policy.shouldRetry(0, now + TimeUnit.MINUTES.toNanos(1), null);
    assertEquals(action.getDecision(), RetryPolicy.RetryDecision.RETRY);
    // exp = 1
    assertThat(action.getDelay().toMillis(), both(greaterThan(0L)).and(lessThan(20L)));

    now = System.nanoTime();
    action = policy.shouldRetry(1, now + TimeUnit.MINUTES.toNanos(1), null);
    assertEquals(action.getDecision(), RetryPolicy.RetryDecision.RETRY);
    // exp = 2
    assertThat(action.getDelay().toMillis(), both(greaterThan(10L)).and(lessThan(30L)));

    now = System.nanoTime();
    action = policy.shouldRetry(100, now + TimeUnit.MINUTES.toNanos(1), null);
    assertEquals(action.getDecision(), RetryPolicy.RetryDecision.RETRY);
    // exp = 200
    assertThat(action.getDelay().toMillis(), both(greaterThan(1500L)).and(lessThan(2500L)));

    now = System.nanoTime();
    action = policy.shouldRetry(1000, now + TimeUnit.MINUTES.toNanos(1), null);
    // reach max times
    assertEquals(action.getDecision(), RetryPolicy.RetryDecision.FAIL);

    now = System.nanoTime();
    action = policy.shouldRetry(1000, now - 100, null);
    // reach deadline
    assertEquals(action.getDecision(), RetryPolicy.RetryDecision.FAIL);
  }
}
