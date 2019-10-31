// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.
package com.xiaomi.infra.pegasus.rpc.async;

import com.xiaomi.infra.pegasus.base.error_code;
import com.xiaomi.infra.pegasus.base.gpid;
import com.xiaomi.infra.pegasus.rpc.KeyHasher;
import com.xiaomi.infra.pegasus.rpc.ReplicationException;
import com.xiaomi.infra.pegasus.tools.Toollet;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;

/** Created by weijiesun on 16-11-25. */
public class TimeoutBenchmark {
  private static final Logger logger = org.slf4j.LoggerFactory.getLogger(TimeoutBenchmark.class);

  private void testTimeout(
      ClusterManager manager, TableHandler handle, int timeoutValue, int count) {
    manager.setTimeout(timeoutValue);

    long result[] = new long[count];

    com.xiaomi.infra.pegasus.apps.update_request req =
        new com.xiaomi.infra.pegasus.apps.update_request(
            new com.xiaomi.infra.pegasus.base.blob("hello".getBytes()),
            new com.xiaomi.infra.pegasus.base.blob("world".getBytes()),
            0);
    Toollet.test_operator op = new Toollet.test_operator(new gpid(1, 1), req);

    logger.warn("Start to test timeout {}ms for {} times", timeoutValue, count);
    long current = System.currentTimeMillis();
    final AtomicInteger call_count = new AtomicInteger(0);
    System.out.println("start");
    for (int i = 0; i < count; ++i) {
      try {
        handle.operate(op, 0);
        Assert.fail();
      } catch (ReplicationException e) {
        long t = System.currentTimeMillis();
        result[i] = t - current;
        current = t;
        Assert.assertEquals(e.getErrorType(), error_code.error_types.ERR_TIMEOUT);
      }
    }
    System.out.println("finished");
    long max_value = -1;
    long min_value = Long.MAX_VALUE;
    double average = 0;
    for (int i = 0; i < result.length; ++i) {
      max_value = Math.max(result[i], max_value);
      min_value = Math.min(result[i], min_value);
      average += result[i];
    }
    average = average / count;
    logger.warn(
        "Min timeout: {}, Max timeout: {}, average timeout: {}", min_value, max_value, average);
  }

  @Test
  public void timeoutChecker() {
    String[] metaList = {"127.0.0.1:34601", "127.0.0.1:34602", "127.0.0.1:34603"};
    ClusterManager manager = new ClusterManager(1000, 1, false, null, 60, metaList);

    TableHandler handle;
    try {
      handle = manager.openTable("temp", KeyHasher.DEFAULT);
    } catch (ReplicationException e) {
      e.printStackTrace();
      Assert.fail();
      return;
    }

    int[] timeoutValues = {1, 5, 10, 20, 50, 100, 1000};
    int[] timeoutCount = {10000, 2000, 1000, 500, 500, 100, 100};
    // int[] timeoutValues = {1};
    // int[] timeoutCount = {10000};
    for (int i = 0; i < timeoutValues.length; ++i) {
      testTimeout(manager, handle, timeoutValues[i], timeoutCount[i]);
    }
  }
}
