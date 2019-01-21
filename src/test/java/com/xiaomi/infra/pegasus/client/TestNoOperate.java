// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.
package com.xiaomi.infra.pegasus.client;

import org.junit.Test;

/** Created by weijiesun on 16-11-24. */
public class TestNoOperate {
  @Test
  public void testNoOperate() throws PException {
    PegasusClientInterface client = PegasusClientFactory.getSingletonClient();
    String tableName = "temp";

    System.out.println("start to write some keys");
    for (int i = 0; i < 100; ++i) {
      String hashKey = "hello" + String.valueOf(i);
      String sortKey = "0";
      String value = "world";
      client.set(tableName, hashKey.getBytes(), sortKey.getBytes(), value.getBytes());
    }

    System.out.println("start to wait some time");

    try {
      Thread.sleep(5000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    PegasusClientFactory.closeSingletonClient();
  }
}
