// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.
package com.xiaomi.infra.pegasus.client;

import io.netty.util.concurrent.Promise;
import io.netty.util.concurrent.SingleThreadEventExecutor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

public class TestFutureGroup {

  @Rule public TestName name = new TestName();

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
            System.err.println(name.getMethodName() + ": " + e.toString());
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

  @Test
  public void testFutureWaitTimeout() throws Exception {
    TestEventExecutor executor = new TestEventExecutor();
    Promise<Void> promise = executor.newPromise();

    FutureGroup<Void> group = new FutureGroup<>(1);
    group.add(promise);
    try {
      // never wake up promise.
      group.waitAllCompleteOrOneFail(10);
    } catch (PException e) {
      // must throw exception
      System.err.println(name.getMethodName() + ": " + e.toString());
      return;
    }
    Assert.fail();
  }
}
