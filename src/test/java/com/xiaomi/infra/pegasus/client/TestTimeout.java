// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.
package com.xiaomi.infra.pegasus.client;

import org.junit.*;

public class TestTimeout {
  @Test
  public void testTimeout() throws PException {
    PegasusClientInterface client = PegasusClientFactory.getSingletonClient();
    String tableName = "temp";

    for (int i = 0; i < 1; ++i) {
      byte[] hashKey = "hello".getBytes();
      byte[] sortKey = "0".getBytes();
      byte[] value = ("world" + String.valueOf(i)).getBytes();

      long current = System.currentTimeMillis();
      try {
        client.set(tableName, hashKey, sortKey, value, 0);
        System.out.println(
            "set succeed, time takes: " + (System.currentTimeMillis() - current) + " ms");
      } catch (PException e) {
        System.out.println(
            "set encounter exception, time takes: "
                + (System.currentTimeMillis() - current)
                + " ms, "
                + e.getMessage());
      }

      current = System.currentTimeMillis();
      try {
        byte[] result = client.get(tableName, hashKey, sortKey);
        System.out.println(
            "get succeed, time takes: " + (System.currentTimeMillis() - current) + " ms");
        if (result != null) {
          System.out.println("get result: " + new String(result));
        }
      } catch (PException e) {
        System.out.println(
            "get encounter exception, time takes: "
                + (System.currentTimeMillis() - current)
                + " ms, "
                + e.getMessage());
        e.printStackTrace();
      }
    }
    PegasusClientFactory.closeSingletonClient();
  }
}
