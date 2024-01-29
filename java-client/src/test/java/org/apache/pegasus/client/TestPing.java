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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;

/** Created by mi on 16-3-22. */
public class TestPing {
  @Test
  public void testPing() throws PException {
    PegasusClientInterface client =
        PegasusClientFactory.createClient("resource:///pegasus.properties");
    String tableName = "temp";

    byte[] hashKey = "hello".getBytes();
    byte[] sortKey = "0".getBytes();
    byte[] value = "world".getBytes();
    byte[] sortKey1 = "1".getBytes();
    byte[] value1 = "pegasus".getBytes();

    try {
      System.out.println("set value ...");
      client.set(tableName, hashKey, sortKey, value, 0);
      System.out.println("set value ok");

      System.out.println("set value1 ...");
      client.set(tableName, hashKey, sortKey1, value1, 0);
      System.out.println("set value1 ok");

      System.out.println("multi set ...");
      List<Pair<byte[], byte[]>> setValues = new ArrayList<Pair<byte[], byte[]>>();
      for (int i = 2; i < 9; ++i) {
        byte[] k = Integer.toString(i).getBytes();
        byte[] v = ("value" + i).getBytes();
        setValues.add(new ImmutablePair<byte[], byte[]>(k, v));
      }
      client.multiSet(tableName, hashKey, setValues);
      System.out.println("multi set ...");

      System.out.println("get value ...");
      byte[] result = client.get(tableName, hashKey, sortKey);
      assertTrue(Arrays.equals(value, result));
      System.out.println("get value ok");

      System.out.println("get ttl ...");
      int ttl = client.ttl(tableName, hashKey, sortKey);
      assertEquals(-1, ttl);
      System.out.println("get ttl ok");

      System.out.println("multi get ...");
      List<byte[]> sortKeys = new ArrayList<byte[]>();
      sortKeys.add("unexist-sort-key".getBytes());
      sortKeys.add(sortKey1);
      sortKeys.add(sortKey1);
      sortKeys.add(sortKey);
      List<Pair<byte[], byte[]>> values = new ArrayList<Pair<byte[], byte[]>>();
      boolean getAll = client.multiGet(tableName, hashKey, sortKeys, values);
      assertTrue(getAll);
      assertEquals(2, values.size());
      assertEquals(sortKey, values.get(0).getKey());
      assertArrayEquals(sortKey, values.get(0).getKey());
      assertArrayEquals(value, values.get(0).getValue());
      assertEquals(sortKey1, values.get(1).getKey());
      assertArrayEquals(sortKey1, values.get(1).getKey());
      assertArrayEquals(value1, values.get(1).getValue());
      System.out.println("multi get ok");

      System.out.println("multi get partial ...");
      sortKeys.clear();
      values.clear();
      sortKeys.add(sortKey);
      sortKeys.add(sortKey1);
      for (Pair<byte[], byte[]> p : setValues) {
        sortKeys.add(p.getKey());
      }
      getAll = client.multiGet(tableName, hashKey, sortKeys, 5, 1000000, values);
      assertFalse(getAll);
      assertEquals(5, values.size());
      assertEquals(sortKey, values.get(0).getKey());
      assertArrayEquals(sortKey, values.get(0).getKey());
      assertArrayEquals(value, values.get(0).getValue());
      assertEquals(sortKey1, values.get(1).getKey());
      assertArrayEquals(sortKey1, values.get(1).getKey());
      assertArrayEquals(value1, values.get(1).getValue());
      for (int i = 2; i < 5; ++i) {
        assertEquals(setValues.get(i - 2).getKey(), values.get(i).getKey());
        assertArrayEquals(setValues.get(i - 2).getKey(), values.get(i).getKey());
        assertArrayEquals(setValues.get(i - 2).getValue(), values.get(i).getValue());
      }
      System.out.println("multi get partial ok");

      System.out.println("del value ...");
      client.del(tableName, hashKey, sortKey);
      System.out.println("del value ok");

      System.out.println("get deleted value ...");
      result = client.get(tableName, hashKey, sortKey);
      assertEquals(result, null);
      System.out.println("get deleted value ok");
    } catch (PException e) {
      e.printStackTrace();
      assertTrue(false);
    }

    client.close();
    client.close();
  }
}
