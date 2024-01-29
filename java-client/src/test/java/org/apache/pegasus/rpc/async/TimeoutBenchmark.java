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
package org.apache.pegasus.rpc.async;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pegasus.apps.update_request;
import org.apache.pegasus.base.blob;
import org.apache.pegasus.base.error_code;
import org.apache.pegasus.base.gpid;
import org.apache.pegasus.client.ClientOptions;
import org.apache.pegasus.rpc.InternalTableOptions;
import org.apache.pegasus.rpc.ReplicationException;
import org.apache.pegasus.tools.Toollet;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;

/** Created by weijiesun on 16-11-25. */
public class TimeoutBenchmark {
  private static final Logger logger = org.slf4j.LoggerFactory.getLogger(TimeoutBenchmark.class);

  private void testTimeout(
      ClusterManager manager, TableHandler handle, int timeoutValue, int count) {
    manager.setTimeout(timeoutValue);

    long result[] = new long[count];

    update_request req =
        new update_request(new blob("hello".getBytes()), new blob("world".getBytes()), 0);
    Toollet.test_operator op = new Toollet.test_operator(new gpid(1, 1), req);

    logger.warn("Start to test timeout {}ms for {} times", timeoutValue, count);
    long current = System.currentTimeMillis();
    final AtomicInteger call_count = new AtomicInteger(0);
    System.out.println("start");
    for (int i = 0; i < count; ++i) {
      try {
        handle.operate(op, 0);
        fail();
      } catch (ReplicationException e) {
        long t = System.currentTimeMillis();
        result[i] = t - current;
        current = t;
        assertEquals(e.getErrorType(), error_code.error_types.ERR_TIMEOUT);
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
    String metaList = "127.0.0.1:34601,127.0.0.1:34602,127.0.0.1:34603";
    ClusterManager manager =
        new ClusterManager(ClientOptions.builder().metaServers(metaList).build());

    TableHandler handle;
    try {
      handle = manager.openTable("temp", InternalTableOptions.forTest());
    } catch (ReplicationException e) {
      e.printStackTrace();
      fail();
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
