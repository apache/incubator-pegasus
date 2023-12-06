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
package org.apache.pegasus.client;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.fail;

import io.netty.util.concurrent.Promise;
import io.netty.util.concurrent.SingleThreadEventExecutor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

public class TestFutureGroup {

  private static final class TestEventExecutor extends SingleThreadEventExecutor {
    TestEventExecutor() {
      super(null, Executors.defaultThreadFactory(), false);
    }

    @Override
    protected void run() {
      while (!confirmShutdown()) {
        Runnable task = takeTask();
        if (task != null) {
          task.run();
        }
      }
    }
  }

  @Test
  public void testBlockingOperationException(TestInfo testInfo) throws Exception {
    // ensure pegasus client will throw PException when BlockingOperationException is thrown.

    TestEventExecutor executor = new TestEventExecutor();
    Promise<Void> promise = executor.newPromise();
    AtomicBoolean executed = new AtomicBoolean(false);
    AtomicBoolean success = new AtomicBoolean(true);

    executor.execute(
        () -> {
          // A background thread waiting for promise to complete.
          FutureGroup<Void> group = new FutureGroup<>(1);
          group.add(promise);
          try {
            group.waitAllCompleteOrOneFail(10000);
          } catch (PException e) {
            success.set(false);
            System.err.println(testInfo.getDisplayName() + ": " + e.toString());
          }
          executed.set(true);
        });

    while (executor.pendingTasks() != 0) {
      Thread.sleep(100);
    }

    promise.setSuccess(null);

    // block until the background thread finished.
    while (!executed.get()) {
      Thread.sleep(100);
    }

    assertFalse(success.get());
  }

  @Test
  public void testFutureWaitTimeout(TestInfo testInfo) throws Exception {
    TestEventExecutor executor = new TestEventExecutor();
    Promise<Void> promise = executor.newPromise();

    FutureGroup<Void> group = new FutureGroup<>(1);
    group.add(promise);
    try {
      // never wake up promise.
      group.waitAllCompleteOrOneFail(10);
    } catch (PException e) {
      // must throw exception
      System.err.println(testInfo.getDisplayName() + ": " + e.toString());
      return;
    }
    fail();
  }
}
