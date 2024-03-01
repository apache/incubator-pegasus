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
    assertEquals(100, action.getDelay().toNanos());

    now = System.nanoTime();
    action = policy.shouldRetry(1, now + TimeUnit.SECONDS.toNanos(1), Duration.ofSeconds(10));
    assertEquals(RetryPolicy.RetryDecision.RETRY, action.getDecision());
    // should not have a delay which makes the nanos greater than deadline
    assertThat(action.getDelay().toNanos(), lessThanOrEqualTo(TimeUnit.SECONDS.toNanos(1)));
  }
}
