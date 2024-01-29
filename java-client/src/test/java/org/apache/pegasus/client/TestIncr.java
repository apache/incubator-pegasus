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

/** @author qinzuoyan */
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

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
      assertEquals(100, result);
      ttlSeconds = client.ttl(tableName, hashKey, sortKey);
      assertEquals(-1, ttlSeconds);
      System.out.println("incr to empty value ok");

      System.out.println("incr zero ...");
      result = client.incr(tableName, hashKey, sortKey, 0);
      assertEquals(100, result);
      ttlSeconds = client.ttl(tableName, hashKey, sortKey);
      assertEquals(-1, ttlSeconds);
      System.out.println("incr zero ok");

      System.out.println("incr negative ...");
      result = client.incr(tableName, hashKey, sortKey, -1);
      assertEquals(99, result);
      ttlSeconds = client.ttl(tableName, hashKey, sortKey);
      assertEquals(-1, ttlSeconds);
      System.out.println("incr negative ok");

      System.out.println("del value ...");
      client.del(tableName, hashKey, sortKey);
      System.out.println("del value ok");

      System.out.println("incr to un-exist value ...");
      result = client.incr(tableName, hashKey, sortKey, 200);
      assertEquals(200, result);
      ttlSeconds = client.ttl(tableName, hashKey, sortKey);
      assertEquals(-1, ttlSeconds);
      System.out.println("incr to un-exist value ok");

      System.out.println("incr with ttlSeconds > 0 ...");
      result = client.incr(tableName, hashKey, sortKey, 1, 10);
      assertEquals(201, result);
      ttlSeconds = client.ttl(tableName, hashKey, sortKey);
      assertTrue(ttlSeconds > 0);
      assertTrue(ttlSeconds <= 10);
      System.out.println("incr with ttlSeconds > 0 ok");

      System.out.println("incr with ttlSeconds == 0 ...");
      result = client.incr(tableName, hashKey, sortKey, 1);
      assertEquals(202, result);
      ttlSeconds = client.ttl(tableName, hashKey, sortKey);
      assertTrue(ttlSeconds > 0);
      assertTrue(ttlSeconds <= 10);
      System.out.println("incr with ttlSeconds == 0 ok");

      System.out.println("incr with ttlSeconds > 0 ...");
      result = client.incr(tableName, hashKey, sortKey, 1, 20);
      assertEquals(203, result);
      ttlSeconds = client.ttl(tableName, hashKey, sortKey);
      assertTrue(ttlSeconds > 10);
      assertTrue(ttlSeconds <= 20);
      System.out.println("incr with ttlSeconds > 0 ok");

      System.out.println("incr with ttlSeconds == -1 ...");
      result = client.incr(tableName, hashKey, sortKey, 1, -1);
      assertEquals(204, result);
      ttlSeconds = client.ttl(tableName, hashKey, sortKey);
      assertEquals(-1, ttlSeconds);
      System.out.println("incr with ttlSeconds == -1 ok");

      System.out.println("del value ...");
      client.del(tableName, hashKey, sortKey);
      System.out.println("del value ok");
    } catch (PException e) {
      e.printStackTrace();
      assertTrue(false);
    }

    PegasusClientFactory.closeSingletonClient();
  }
}
