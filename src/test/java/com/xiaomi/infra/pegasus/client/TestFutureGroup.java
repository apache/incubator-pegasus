// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.
package com.xiaomi.infra.pegasus.client;

import io.netty.util.concurrent.Promise;
import io.netty.util.concurrent.SingleThreadEventExecutor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Assert;
import org.junit.Test;

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
  public void testBlockingOperationException() throws Exception {
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
            System.err.println("TestFutureGroup.testInterrupt: " + e.toString());
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

    Assert.assertFalse(success.get());
  }
}
