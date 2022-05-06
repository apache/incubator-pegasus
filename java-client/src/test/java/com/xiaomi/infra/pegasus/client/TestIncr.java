// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.
package com.xiaomi.infra.pegasus.client;

/** @author qinzuoyan */
import org.junit.Assert;
import org.junit.Test;

/** Created by mi on 18-7-10. */
public class TestIncr {
  @Test
  public void testIncr() throws PException {
    PegasusClientInterface client = PegasusClientFactory.getSingletonClient();
    String tableName = "temp";

    byte[] hashKey = "incrkeyforjava".getBytes();
    byte[] sortKey = "0".getBytes();
    byte[] value = "".getBytes();
    int ttlSeconds = 0;

    try {
      System.out.println("set value ...");
      client.set(tableName, hashKey, sortKey, value, 0);
      System.out.println("set value ok");

      System.out.println("incr to empty value ...");
      long result = client.incr(tableName, hashKey, sortKey, 100);
      Assert.assertEquals(100, result);
      ttlSeconds = client.ttl(tableName, hashKey, sortKey);
      Assert.assertEquals(-1, ttlSeconds);
      System.out.println("incr to empty value ok");

      System.out.println("incr zero ...");
      result = client.incr(tableName, hashKey, sortKey, 0);
      Assert.assertEquals(100, result);
      ttlSeconds = client.ttl(tableName, hashKey, sortKey);
      Assert.assertEquals(-1, ttlSeconds);
      System.out.println("incr zero ok");

      System.out.println("incr negative ...");
      result = client.incr(tableName, hashKey, sortKey, -1);
      Assert.assertEquals(99, result);
      ttlSeconds = client.ttl(tableName, hashKey, sortKey);
      Assert.assertEquals(-1, ttlSeconds);
      System.out.println("incr negative ok");

      System.out.println("del value ...");
      client.del(tableName, hashKey, sortKey);
      System.out.println("del value ok");

      System.out.println("incr to un-exist value ...");
      result = client.incr(tableName, hashKey, sortKey, 200);
      Assert.assertEquals(200, result);
      ttlSeconds = client.ttl(tableName, hashKey, sortKey);
      Assert.assertEquals(-1, ttlSeconds);
      System.out.println("incr to un-exist value ok");

      System.out.println("incr with ttlSeconds > 0 ...");
      result = client.incr(tableName, hashKey, sortKey, 1, 10);
      Assert.assertEquals(201, result);
      ttlSeconds = client.ttl(tableName, hashKey, sortKey);
      Assert.assertTrue(ttlSeconds > 0);
      Assert.assertTrue(ttlSeconds <= 10);
      System.out.println("incr with ttlSeconds > 0 ok");

      System.out.println("incr with ttlSeconds == 0 ...");
      result = client.incr(tableName, hashKey, sortKey, 1);
      Assert.assertEquals(202, result);
      ttlSeconds = client.ttl(tableName, hashKey, sortKey);
      Assert.assertTrue(ttlSeconds > 0);
      Assert.assertTrue(ttlSeconds <= 10);
      System.out.println("incr with ttlSeconds == 0 ok");

      System.out.println("incr with ttlSeconds > 0 ...");
      result = client.incr(tableName, hashKey, sortKey, 1, 20);
      Assert.assertEquals(203, result);
      ttlSeconds = client.ttl(tableName, hashKey, sortKey);
      Assert.assertTrue(ttlSeconds > 10);
      Assert.assertTrue(ttlSeconds <= 20);
      System.out.println("incr with ttlSeconds > 0 ok");

      System.out.println("incr with ttlSeconds == -1 ...");
      result = client.incr(tableName, hashKey, sortKey, 1, -1);
      Assert.assertEquals(204, result);
      ttlSeconds = client.ttl(tableName, hashKey, sortKey);
      Assert.assertEquals(-1, ttlSeconds);
      System.out.println("incr with ttlSeconds == -1 ok");

      System.out.println("del value ...");
      client.del(tableName, hashKey, sortKey);
      System.out.println("del value ok");
    } catch (PException e) {
      e.printStackTrace();
      Assert.assertTrue(false);
    }

    PegasusClientFactory.closeSingletonClient();
  }
}
