// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.
package com.xiaomi.infra.pegasus.client;

import io.netty.util.concurrent.Future;
import java.util.ArrayList;
import java.util.List;

final class FutureGroup<Result> {

  FutureGroup(int initialCapacity) {
    asyncTasks = new ArrayList<>(initialCapacity);
  }

  public void add(Future<Result> task) {
    asyncTasks.add(task);
  }

  void waitAllCompleteOrOneFail(int timeoutMillis) throws PException {
    waitAllCompleteOrOneFail(null, timeoutMillis);
  }

  // Waits until all future tasks complete but terminate if one fails.
  // `results` is nullable
  void waitAllCompleteOrOneFail(List<Result> results, int timeoutMillis) throws PException {
    for (int i = 0; i < asyncTasks.size(); i++) {
      Future<Result> fu = asyncTasks.get(i);
      try {
        fu.await(timeoutMillis);
      } catch (Exception e) {
        throw new PException("async task #[" + i + "] await failed: " + e.toString());
      }
      if (fu.isSuccess()) {
        if (results != null) {
          results.set(i, fu.getNow());
        }
      } else {
        Throwable cause = fu.cause();
        if (cause == null) {
          throw new PException(
              String.format(
                  "async task #[" + i + "] failed: timeout expired (%dms)", timeoutMillis));
        }
        throw new PException("async task #[" + i + "] failed: " + cause.getMessage(), cause);
      }
    }
  }

  private List<Future<Result>> asyncTasks;
}
