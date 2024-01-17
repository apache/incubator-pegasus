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
