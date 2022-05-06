// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.xiaomi.infra.pegasus.client;

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

public class TestBatchGetByPartitions {
  volatile boolean isRunning = false;
  static final int totalTestKeyNum = 101;

  @Test
  public void testBatchGetByPartitions() throws PException {
    String tableName = "temp";
    PegasusTableInterface table = PegasusClientFactory.getSingletonClient().openTable(tableName);

    List<SetItem> batchSetItems = new ArrayList<>();
    List<Pair<byte[], byte[]>> pairs = new ArrayList<>();
    List<byte[]> values = new ArrayList<>();
    for (int i = 0; i < totalTestKeyNum; ++i) {
      SetItem oneItem = new SetItem();
      oneItem.hashKey = ("hashKey_" + i + "_" + System.currentTimeMillis()).getBytes();
      oneItem.sortKey = ("sortedKey_" + i + "_" + System.currentTimeMillis()).getBytes();
      oneItem.value = ("value_" + i + "_" + System.currentTimeMillis()).getBytes();
      pairs.add(Pair.of(oneItem.hashKey, oneItem.sortKey));
      values.add(oneItem.value);
      batchSetItems.add(oneItem);
    }

    List<PException> result = new ArrayList<>();
    table.batchSet2(batchSetItems, result, 0);
    for (int i = 0; i < totalTestKeyNum; ++i) {
      Assert.assertNull(result.get(i));
    }

    List<Pair<PException, byte[]>> getResult = new ArrayList<>();
    int resultCount = table.batchGetByPartitions(pairs, getResult, 0);
    Assert.assertEquals(totalTestKeyNum, resultCount);

    for (int i = 0; i < totalTestKeyNum; ++i) {
      Assert.assertNull(getResult.get(i).getLeft());
      Assert.assertArrayEquals(getResult.get(i).getRight(), values.get(i));
    }
  }

  @Test
  public void testStableQpsForPegasusShellShow() throws Exception {
    // only for auxiliary test
    Assume.assumeTrue(false);

    String tableName = "temp";
    PegasusTableInterface table = PegasusClientFactory.getSingletonClient().openTable(tableName);

    List<SetItem> batchSetItems = new ArrayList<>();
    List<Pair<byte[], byte[]>> pairs = new ArrayList<>();
    List<byte[]> values = new ArrayList<>();
    for (int i = 0; i < totalTestKeyNum; ++i) {
      SetItem oneItem = new SetItem();
      oneItem.hashKey = ("hashKey_" + i + "_" + System.currentTimeMillis()).getBytes();
      oneItem.sortKey = ("sortedKey_" + i + "_" + System.currentTimeMillis()).getBytes();
      oneItem.value = ("value_" + i + "_" + System.currentTimeMillis()).getBytes();
      pairs.add(Pair.of(oneItem.hashKey, oneItem.sortKey));
      values.add(oneItem.value);
      batchSetItems.add(oneItem);
    }

    List<PException> result = new ArrayList<>();
    table.batchSet2(batchSetItems, result, 0);
    for (int i = 0; i < totalTestKeyNum; ++i) {
      Assert.assertNull(result.get(i));
    }

    Runnable testRunnable =
        new Runnable() {
          @Override
          public void run() {
            while (isRunning) {
              List<Pair<PException, byte[]>> getResult = new ArrayList<>();
              try {
                table.batchGetByPartitions(pairs, getResult, 0);
              } catch (PException e) {
                e.printStackTrace();
              }

              try {
                Thread.sleep(50);
              } catch (InterruptedException e) {
                e.printStackTrace();
              }
            }
          }
        };

    isRunning = true;
    Thread oneThread = new Thread(testRunnable);
    oneThread.start();

    Thread.sleep(600000);

    isRunning = false;
    oneThread.join();
  }
}
