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
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import io.netty.util.concurrent.Future;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;

/** Created by mi on 16-3-22. */
public class TestBasic {
  private static final Logger logger = org.slf4j.LoggerFactory.getLogger(TestBasic.class);

  private static final String basicSetGetDelHashKey = "TestBasic_testSetGetDel_hash_key_1";
  private static final String multiSetGetDelHashKey = "TestBasic_testMultiSetGetDel_hash_key_1";
  private static final String multiGetHashKey = "TestBasic_testMultiGet_hash_key_1";
  private static final String multiGetReverseHashKey = "TestBasic_testMultiGetReverse_hash_key_1";

  private static final char[] hexArray = "0123456789ABCDEF".toCharArray();

  public static String bytesToHex(byte[] bytes) {
    char[] hexChars = new char[bytes.length * 2];
    for (int j = 0; j < bytes.length; j++) {
      int v = bytes[j] & 0xFF;
      hexChars[j * 2] = hexArray[v >>> 4];
      hexChars[j * 2 + 1] = hexArray[v & 0x0F];
    }
    return new String(hexChars);
  }

  @Test
  public void testGenerateKey() throws PException {
    assertEquals(
        "00010A0BFEFF", bytesToHex(new byte[] {0x00, 0x01, 0x0A, 0x0B, (byte) 0xFE, (byte) 0xFF}));

    assertArrayEquals(
        new byte[] {0x00, 0x00}, PegasusClient.generateKey(new byte[] {}, new byte[] {}));
    assertArrayEquals(
        new byte[] {0x00, 0x01, 0x44}, PegasusClient.generateKey(new byte[] {0x44}, new byte[] {}));
    assertArrayEquals(
        new byte[] {0x00, 0x00, 0x55}, PegasusClient.generateKey(new byte[] {}, new byte[] {0x55}));
    assertArrayEquals(
        new byte[] {0x00, 0x01, 0x44, 0x55},
        PegasusClient.generateKey(new byte[] {0x44}, new byte[] {0x55}));
    try {
      byte[] k = PegasusClient.generateKey(new byte[64 * 1024], new byte[] {0x55});
      assertTrue(false);
    } catch (Exception e) {
    }

    assertArrayEquals(new byte[] {0x00, 0x01}, PegasusClient.generateNextBytes(new byte[] {}));
    assertArrayEquals(
        new byte[] {0x00, 0x01, 0x0B}, PegasusClient.generateNextBytes(new byte[] {0x0A}));
    assertArrayEquals(
        new byte[] {0x00, 0x01, (byte) 0xFF},
        PegasusClient.generateNextBytes(new byte[] {(byte) 0xFE}));
    assertArrayEquals(
        new byte[] {0x00, 0x02}, PegasusClient.generateNextBytes(new byte[] {(byte) 0xFF}));
    assertArrayEquals(
        new byte[] {0x00, 0x02, 0x0B},
        PegasusClient.generateNextBytes(new byte[] {0x0A, (byte) 0xFF}));
    assertArrayEquals(
        new byte[] {0x00, 0x03},
        PegasusClient.generateNextBytes(new byte[] {(byte) 0xFF, (byte) 0xFF}));
    try {
      byte[] k = PegasusClient.generateNextBytes(new byte[64 * 1024]);
      assertTrue(false);
    } catch (Exception e) {
    }

    assertArrayEquals(
        new byte[] {0x00, 0x01}, PegasusClient.generateNextBytes(new byte[] {}, new byte[] {}));
    assertArrayEquals(
        new byte[] {0x00, 0x01, 0x45},
        PegasusClient.generateNextBytes(new byte[] {0x44}, new byte[] {}));
    assertArrayEquals(
        new byte[] {0x00, 0x00, 0x56},
        PegasusClient.generateNextBytes(new byte[] {}, new byte[] {0x55}));
    assertArrayEquals(
        new byte[] {0x00, 0x01, 0x44, 0x56},
        PegasusClient.generateNextBytes(new byte[] {0x44}, new byte[] {0x55}));
    assertArrayEquals(
        new byte[] {0x00, 0x01, 0x45},
        PegasusClient.generateNextBytes(new byte[] {0x44}, new byte[] {(byte) 0xFF}));
    assertArrayEquals(
        new byte[] {0x00, 0x02},
        PegasusClient.generateNextBytes(new byte[] {(byte) 0xFF}, new byte[] {}));
    assertArrayEquals(
        new byte[] {0x00, 0x01, (byte) 0xFF, 0x56},
        PegasusClient.generateNextBytes(new byte[] {(byte) 0xFF}, new byte[] {0x55}));
    assertArrayEquals(
        new byte[] {0x00, 0x02},
        PegasusClient.generateNextBytes(new byte[] {(byte) 0xFF}, new byte[] {(byte) 0xFF}));
    assertArrayEquals(
        new byte[] {0x00, 0x02},
        PegasusClient.generateNextBytes(
            new byte[] {(byte) 0xFF}, new byte[] {(byte) 0xFF, (byte) 0xFF}));
    assertArrayEquals(
        new byte[] {0x00, 0x03},
        PegasusClient.generateNextBytes(
            new byte[] {(byte) 0xFF, (byte) 0xFF}, new byte[] {(byte) 0xFF}));
    try {
      byte[] k = PegasusClient.generateNextBytes(new byte[64 * 1024], new byte[0]);
      assertTrue(false);
    } catch (Exception e) {
    }
  }

  @Test
  public void testGetSingletonClient() throws PException {
    PegasusClientInterface client = PegasusClientFactory.getSingletonClient();
    PegasusClientInterface client1 = PegasusClientFactory.getSingletonClient();
    assertEquals(client, client1);
    try {
      PegasusClientInterface client2 =
          PegasusClientFactory.getSingletonClient("resource:///xxx.properties");
      assertTrue(false);
    } catch (PException e) {
    }
    PegasusClientFactory.closeSingletonClient();
  }

  @Test
  public void testSetGetDel() throws PException {
    PegasusClientInterface client = PegasusClientFactory.getSingletonClient();
    String tableName = "temp";
    byte[] hashKey = basicSetGetDelHashKey.getBytes();

    try {
      // set
      client.set(
          tableName, hashKey, "basic_test_sort_key_1".getBytes(), "basic_test_value_1".getBytes());

      // check exist
      boolean exist = client.exist(tableName, hashKey, "basic_test_sort_key_1".getBytes());
      assertTrue(exist);

      exist = client.exist(tableName, hashKey, "basic_test_sort_key_2".getBytes());
      assertFalse(exist);

      // check sortkey count
      long sortKeyCount = client.sortKeyCount(tableName, hashKey);
      assertEquals(1, sortKeyCount);

      // get
      byte[] value = client.get(tableName, hashKey, "basic_test_sort_key_1".getBytes());
      assertArrayEquals("basic_test_value_1".getBytes(), value);

      value = client.get(tableName, hashKey, "basic_test_sort_key_2".getBytes());
      assertEquals(null, value);

      // del
      client.del(tableName, hashKey, "basic_test_sort_key_1".getBytes());

      // check exist
      exist = client.exist(tableName, hashKey, "basic_test_sort_key_1".getBytes());
      assertFalse(exist);

      // check sortkey count
      sortKeyCount = client.sortKeyCount(tableName, hashKey);
      assertEquals(0, sortKeyCount);

      // check deleted
      value = client.get(tableName, hashKey, "basic_test_sort_key_1".getBytes());
      assertEquals(null, value);
    } catch (PException e) {
      e.printStackTrace();
      assertTrue(false);
    }

    PegasusClientFactory.closeSingletonClient();
  }

  @Test
  public void testMultiSetGetDel() throws PException {
    PegasusClientInterface client = PegasusClientFactory.getSingletonClient();
    String tableName = "temp";
    byte[] hashKey = multiSetGetDelHashKey.getBytes();

    try {
      // multi set
      List<Pair<byte[], byte[]>> values = new ArrayList<Pair<byte[], byte[]>>();
      values.add(Pair.of("basic_test_sort_key_1".getBytes(), "basic_test_value_1".getBytes()));
      values.add(Pair.of("basic_test_sort_key_2".getBytes(), "basic_test_value_2".getBytes()));
      values.add(Pair.of("basic_test_sort_key_3".getBytes(), "basic_test_value_3".getBytes()));
      values.add(Pair.of("basic_test_sort_key_4".getBytes(), "basic_test_value_4".getBytes()));
      client.multiSet(tableName, hashKey, values);

      // check exist
      boolean exist = client.exist(tableName, hashKey, "basic_test_sort_key_1".getBytes());
      assertTrue(exist);

      exist = client.exist(tableName, hashKey, "basic_test_sort_key_5".getBytes());
      assertFalse(exist);

      // check sortkey count
      long sortKeyCount = client.sortKeyCount(tableName, hashKey);
      assertEquals(4, sortKeyCount);

      // multi get
      List<byte[]> sortKeys = new ArrayList<byte[]>();
      sortKeys.add("basic_test_sort_key_0".getBytes());
      sortKeys.add("basic_test_sort_key_1".getBytes());
      sortKeys.add("basic_test_sort_key_2".getBytes());
      sortKeys.add("basic_test_sort_key_3".getBytes());
      sortKeys.add("basic_test_sort_key_4".getBytes());
      List<Pair<byte[], byte[]>> newValues = new ArrayList<Pair<byte[], byte[]>>();
      boolean ret = client.multiGet(tableName, hashKey, sortKeys, newValues);
      assertTrue(ret);
      assertEquals(4, newValues.size());
      assertArrayEquals("basic_test_sort_key_1".getBytes(), newValues.get(0).getKey());
      assertArrayEquals("basic_test_value_1".getBytes(), newValues.get(0).getValue());
      assertArrayEquals("basic_test_sort_key_2".getBytes(), newValues.get(1).getKey());
      assertArrayEquals("basic_test_value_2".getBytes(), newValues.get(1).getValue());
      assertArrayEquals("basic_test_sort_key_3".getBytes(), newValues.get(2).getKey());
      assertArrayEquals("basic_test_value_3".getBytes(), newValues.get(2).getValue());
      assertArrayEquals("basic_test_sort_key_4".getBytes(), newValues.get(3).getKey());
      assertArrayEquals("basic_test_value_4".getBytes(), newValues.get(3).getValue());

      // multi get with count limit
      newValues.clear();
      ret = client.multiGet(tableName, hashKey, sortKeys, 1, 0, newValues);
      assertFalse(ret);
      assertEquals(1, newValues.size());
      assertArrayEquals("basic_test_sort_key_1".getBytes(), newValues.get(0).getKey());
      assertArrayEquals("basic_test_value_1".getBytes(), newValues.get(0).getValue());

      // multi get with empty sortKeys
      sortKeys.clear();
      newValues.clear();
      ret = client.multiGet(tableName, hashKey, sortKeys, newValues);
      assertTrue(ret);
      assertEquals(4, newValues.size());
      assertArrayEquals("basic_test_sort_key_1".getBytes(), newValues.get(0).getKey());
      assertArrayEquals("basic_test_value_1".getBytes(), newValues.get(0).getValue());
      assertArrayEquals("basic_test_sort_key_2".getBytes(), newValues.get(1).getKey());
      assertArrayEquals("basic_test_value_2".getBytes(), newValues.get(1).getValue());
      assertArrayEquals("basic_test_sort_key_3".getBytes(), newValues.get(2).getKey());
      assertArrayEquals("basic_test_value_3".getBytes(), newValues.get(2).getValue());
      assertArrayEquals("basic_test_sort_key_4".getBytes(), newValues.get(3).getKey());
      assertArrayEquals("basic_test_value_4".getBytes(), newValues.get(3).getValue());

      // multi get with null sortKeys
      newValues.clear();
      ret = client.multiGet(tableName, hashKey, null, newValues);
      assertTrue(ret);
      assertEquals(4, newValues.size());
      assertArrayEquals("basic_test_sort_key_1".getBytes(), newValues.get(0).getKey());
      assertArrayEquals("basic_test_value_1".getBytes(), newValues.get(0).getValue());
      assertArrayEquals("basic_test_sort_key_2".getBytes(), newValues.get(1).getKey());
      assertArrayEquals("basic_test_value_2".getBytes(), newValues.get(1).getValue());
      assertArrayEquals("basic_test_sort_key_3".getBytes(), newValues.get(2).getKey());
      assertArrayEquals("basic_test_value_3".getBytes(), newValues.get(2).getValue());
      assertArrayEquals("basic_test_sort_key_4".getBytes(), newValues.get(3).getKey());
      assertArrayEquals("basic_test_value_4".getBytes(), newValues.get(3).getValue());

      // multi get sort keys
      sortKeys.clear();
      ret = client.multiGetSortKeys(tableName, hashKey, sortKeys);
      assertTrue(ret);
      assertEquals(4, sortKeys.size());
      assertArrayEquals("basic_test_sort_key_1".getBytes(), sortKeys.get(0));
      assertArrayEquals("basic_test_sort_key_2".getBytes(), sortKeys.get(1));
      assertArrayEquals("basic_test_sort_key_3".getBytes(), sortKeys.get(2));
      assertArrayEquals("basic_test_sort_key_4".getBytes(), sortKeys.get(3));

      // multi del
      sortKeys.clear();
      sortKeys.add("basic_test_sort_key_0".getBytes());
      sortKeys.add("basic_test_sort_key_1".getBytes());
      sortKeys.add("basic_test_sort_key_2".getBytes());
      client.multiDel(tableName, hashKey, sortKeys);

      // check sortkey count
      sortKeyCount = client.sortKeyCount(tableName, hashKey);
      assertEquals(2, sortKeyCount);

      // check deleted
      newValues.clear();
      ret = client.multiGet(tableName, hashKey, null, newValues);
      assertTrue(ret);
      assertEquals(2, newValues.size());
      assertArrayEquals("basic_test_sort_key_3".getBytes(), newValues.get(0).getKey());
      assertArrayEquals("basic_test_value_3".getBytes(), newValues.get(0).getValue());
      assertArrayEquals("basic_test_sort_key_4".getBytes(), newValues.get(1).getKey());
      assertArrayEquals("basic_test_value_4".getBytes(), newValues.get(1).getValue());

      // multi del all
      sortKeys.clear();
      sortKeys.add("basic_test_sort_key_3".getBytes());
      sortKeys.add("basic_test_sort_key_4".getBytes());
      sortKeys.add("basic_test_sort_key_5".getBytes());
      client.multiDel(tableName, hashKey, sortKeys);

      // check sortkey count
      sortKeyCount = client.sortKeyCount(tableName, hashKey);
      assertEquals(0, sortKeyCount);

      // check deleted by multiGet
      newValues.clear();
      ret = client.multiGet(tableName, hashKey, null, newValues);
      assertTrue(ret);
      assertEquals(0, newValues.size());

      // check deleted by multiGetSortKeys
      sortKeys.clear();
      ret = client.multiGetSortKeys(tableName, hashKey, sortKeys);
      assertTrue(ret);
      assertEquals(0, sortKeys.size());

      // multi set many kvs
      values.clear();
      for (int i = 1; i <= 200; i++) {
        String sortKey = "basic_test_sort_key_" + String.format("%03d", i);
        String value = "basic_test_value_" + String.valueOf(i);
        values.add(Pair.of(sortKey.getBytes(), value.getBytes()));
      }
      client.multiSet(tableName, hashKey, values);

      // multi get with no limit
      newValues.clear();
      ret = client.multiGet(tableName, hashKey, null, 0, 0, newValues);
      assertTrue(ret);
      assertEquals(200, newValues.size());
      for (int i = 1; i <= 200; i++) {
        String sortKey = "basic_test_sort_key_" + String.format("%03d", i);
        String value = "basic_test_value_" + String.valueOf(i);
        assertArrayEquals(sortKey.getBytes(), newValues.get(i - 1).getKey());
        assertArrayEquals(value.getBytes(), newValues.get(i - 1).getValue());
      }

      // multi del all
      sortKeys.clear();
      for (int i = 1; i <= 200; i++) {
        String sortKey = "basic_test_sort_key_" + String.format("%03d", i);
        assertArrayEquals(sortKey.getBytes(), newValues.get(i - 1).getKey());
        sortKeys.add(sortKey.getBytes());
      }
      client.multiDel(tableName, hashKey, sortKeys);

      // check sortkey count
      sortKeyCount = client.sortKeyCount(tableName, hashKey);
      assertEquals(0, sortKeyCount);
    } catch (PException e) {
      e.printStackTrace();
      assertTrue(false);
    }

    PegasusClientFactory.closeSingletonClient();
  }

  @Test
  public void testMultiGet() throws PException {
    PegasusClientInterface client = PegasusClientFactory.getSingletonClient();
    String tableName = "temp";
    byte[] hashKey = multiGetHashKey.getBytes();

    try {
      // multi set
      List<Pair<byte[], byte[]>> values = new ArrayList<Pair<byte[], byte[]>>();
      values.add(Pair.of("".getBytes(), "0".getBytes()));
      values.add(Pair.of("1".getBytes(), "1".getBytes()));
      values.add(Pair.of("1-abcdefg".getBytes(), "1-abcdefg".getBytes()));
      values.add(Pair.of("2".getBytes(), "2".getBytes()));
      values.add(Pair.of("2-abcdefg".getBytes(), "2-abcdefg".getBytes()));
      values.add(Pair.of("3".getBytes(), "3".getBytes()));
      values.add(Pair.of("3-efghijk".getBytes(), "3-efghijk".getBytes()));
      values.add(Pair.of("4".getBytes(), "4".getBytes()));
      values.add(Pair.of("4-hijklmn".getBytes(), "4-hijklmn".getBytes()));
      values.add(Pair.of("5".getBytes(), "5".getBytes()));
      values.add(Pair.of("5-hijklmn".getBytes(), "5-hijklmn".getBytes()));
      values.add(Pair.of("6".getBytes(), "6".getBytes()));
      values.add(Pair.of("7".getBytes(), "7".getBytes()));
      client.multiSet(tableName, hashKey, values);

      // check sortkey count
      long sortKeyCount = client.sortKeyCount(tableName, hashKey);
      assertEquals(13, sortKeyCount);

      // [null, null)
      MultiGetOptions options = new MultiGetOptions();
      assertTrue(options.startInclusive);
      assertFalse(options.stopInclusive);
      List<Pair<byte[], byte[]>> newValues = new ArrayList<Pair<byte[], byte[]>>();
      boolean ret = client.multiGet(tableName, hashKey, null, null, options, newValues);
      assertTrue(ret);
      assertEquals(13, newValues.size());
      assertArrayEquals("".getBytes(), newValues.get(0).getKey());
      assertArrayEquals("1".getBytes(), newValues.get(1).getKey());
      assertArrayEquals("1-abcdefg".getBytes(), newValues.get(2).getKey());
      assertArrayEquals("2".getBytes(), newValues.get(3).getKey());
      assertArrayEquals("2-abcdefg".getBytes(), newValues.get(4).getKey());
      assertArrayEquals("3".getBytes(), newValues.get(5).getKey());
      assertArrayEquals("3-efghijk".getBytes(), newValues.get(6).getKey());
      assertArrayEquals("4".getBytes(), newValues.get(7).getKey());
      assertArrayEquals("4-hijklmn".getBytes(), newValues.get(8).getKey());
      assertArrayEquals("5".getBytes(), newValues.get(9).getKey());
      assertArrayEquals("5-hijklmn".getBytes(), newValues.get(10).getKey());
      assertArrayEquals("6".getBytes(), newValues.get(11).getKey());
      assertArrayEquals("7".getBytes(), newValues.get(12).getKey());

      // [null, null]
      options = new MultiGetOptions();
      options.startInclusive = true;
      options.stopInclusive = true;
      newValues.clear();
      ret = client.multiGet(tableName, hashKey, null, null, options, newValues);
      assertTrue(ret);
      assertEquals(13, newValues.size());
      assertArrayEquals("".getBytes(), newValues.get(0).getKey());
      assertArrayEquals("1".getBytes(), newValues.get(1).getKey());
      assertArrayEquals("1-abcdefg".getBytes(), newValues.get(2).getKey());
      assertArrayEquals("2".getBytes(), newValues.get(3).getKey());
      assertArrayEquals("2-abcdefg".getBytes(), newValues.get(4).getKey());
      assertArrayEquals("3".getBytes(), newValues.get(5).getKey());
      assertArrayEquals("3-efghijk".getBytes(), newValues.get(6).getKey());
      assertArrayEquals("4".getBytes(), newValues.get(7).getKey());
      assertArrayEquals("4-hijklmn".getBytes(), newValues.get(8).getKey());
      assertArrayEquals("5".getBytes(), newValues.get(9).getKey());
      assertArrayEquals("5-hijklmn".getBytes(), newValues.get(10).getKey());
      assertArrayEquals("6".getBytes(), newValues.get(11).getKey());
      assertArrayEquals("7".getBytes(), newValues.get(12).getKey());

      // (null, null)
      options = new MultiGetOptions();
      options.startInclusive = false;
      options.stopInclusive = false;
      newValues.clear();
      ret = client.multiGet(tableName, hashKey, null, null, options, newValues);
      assertTrue(ret);
      assertEquals(12, newValues.size());
      assertArrayEquals("1".getBytes(), newValues.get(0).getKey());
      assertArrayEquals("1-abcdefg".getBytes(), newValues.get(1).getKey());
      assertArrayEquals("2".getBytes(), newValues.get(2).getKey());
      assertArrayEquals("2-abcdefg".getBytes(), newValues.get(3).getKey());
      assertArrayEquals("3".getBytes(), newValues.get(4).getKey());
      assertArrayEquals("3-efghijk".getBytes(), newValues.get(5).getKey());
      assertArrayEquals("4".getBytes(), newValues.get(6).getKey());
      assertArrayEquals("4-hijklmn".getBytes(), newValues.get(7).getKey());
      assertArrayEquals("5".getBytes(), newValues.get(8).getKey());
      assertArrayEquals("5-hijklmn".getBytes(), newValues.get(9).getKey());
      assertArrayEquals("6".getBytes(), newValues.get(10).getKey());
      assertArrayEquals("7".getBytes(), newValues.get(11).getKey());

      // (null, null]
      options = new MultiGetOptions();
      options.startInclusive = false;
      options.stopInclusive = true;
      newValues.clear();
      ret = client.multiGet(tableName, hashKey, null, null, options, newValues);
      assertTrue(ret);
      assertEquals(12, newValues.size());
      assertArrayEquals("1".getBytes(), newValues.get(0).getKey());
      assertArrayEquals("1-abcdefg".getBytes(), newValues.get(1).getKey());
      assertArrayEquals("2".getBytes(), newValues.get(2).getKey());
      assertArrayEquals("2-abcdefg".getBytes(), newValues.get(3).getKey());
      assertArrayEquals("3".getBytes(), newValues.get(4).getKey());
      assertArrayEquals("3-efghijk".getBytes(), newValues.get(5).getKey());
      assertArrayEquals("4".getBytes(), newValues.get(6).getKey());
      assertArrayEquals("4-hijklmn".getBytes(), newValues.get(7).getKey());
      assertArrayEquals("5".getBytes(), newValues.get(8).getKey());
      assertArrayEquals("5-hijklmn".getBytes(), newValues.get(9).getKey());
      assertArrayEquals("6".getBytes(), newValues.get(10).getKey());
      assertArrayEquals("7".getBytes(), newValues.get(11).getKey());

      // [null, 1]
      options = new MultiGetOptions();
      options.startInclusive = true;
      options.stopInclusive = true;
      newValues.clear();
      ret = client.multiGet(tableName, hashKey, null, "1".getBytes(), options, newValues);
      assertTrue(ret);
      assertEquals(2, newValues.size());
      assertArrayEquals("".getBytes(), newValues.get(0).getKey());
      assertArrayEquals("1".getBytes(), newValues.get(1).getKey());

      // [null, 1)
      options = new MultiGetOptions();
      options.startInclusive = true;
      options.stopInclusive = false;
      newValues.clear();
      ret = client.multiGet(tableName, hashKey, null, "1".getBytes(), options, newValues);
      assertTrue(ret);
      assertEquals(1, newValues.size());
      assertArrayEquals("".getBytes(), newValues.get(0).getKey());

      // (null, 1]
      options = new MultiGetOptions();
      options.startInclusive = false;
      options.stopInclusive = true;
      newValues.clear();
      ret = client.multiGet(tableName, hashKey, null, "1".getBytes(), options, newValues);
      assertTrue(ret);
      assertEquals(1, newValues.size());
      assertArrayEquals("1".getBytes(), newValues.get(0).getKey());

      // (null, 1)
      options = new MultiGetOptions();
      options.startInclusive = false;
      options.stopInclusive = false;
      newValues.clear();
      ret = client.multiGet(tableName, hashKey, null, "1".getBytes(), options, newValues);
      assertTrue(ret);
      assertEquals(0, newValues.size());

      // [1, 1]
      options = new MultiGetOptions();
      options.startInclusive = true;
      options.stopInclusive = true;
      newValues.clear();
      ret = client.multiGet(tableName, hashKey, "1".getBytes(), "1".getBytes(), options, newValues);
      assertTrue(ret);
      assertEquals(1, newValues.size());
      assertArrayEquals("1".getBytes(), newValues.get(0).getKey());

      // [1, 1)
      options = new MultiGetOptions();
      options.startInclusive = true;
      options.stopInclusive = false;
      newValues.clear();
      ret = client.multiGet(tableName, hashKey, "1".getBytes(), "1".getBytes(), options, newValues);
      assertTrue(ret);
      assertEquals(0, newValues.size());

      // (1, 1]
      options = new MultiGetOptions();
      options.startInclusive = false;
      options.stopInclusive = true;
      newValues.clear();
      ret = client.multiGet(tableName, hashKey, "1".getBytes(), "1".getBytes(), options, newValues);
      assertTrue(ret);
      assertEquals(0, newValues.size());

      // (1, 1)
      options = new MultiGetOptions();
      options.startInclusive = false;
      options.stopInclusive = false;
      newValues.clear();
      ret = client.multiGet(tableName, hashKey, "1".getBytes(), "1".getBytes(), options, newValues);
      assertTrue(ret);
      assertEquals(0, newValues.size());

      // [2, 1]
      options = new MultiGetOptions();
      options.startInclusive = false;
      options.stopInclusive = false;
      newValues.clear();
      ret = client.multiGet(tableName, hashKey, "2".getBytes(), "1".getBytes(), options, newValues);
      assertTrue(ret);
      assertEquals(0, newValues.size());

      // match-anywhere("-")
      options = new MultiGetOptions();
      options.sortKeyFilterType = FilterType.FT_MATCH_ANYWHERE;
      options.sortKeyFilterPattern = "-".getBytes();
      newValues.clear();
      ret = client.multiGet(tableName, hashKey, null, null, options, newValues);
      assertTrue(ret);
      assertEquals(5, newValues.size());
      assertArrayEquals("1-abcdefg".getBytes(), newValues.get(0).getKey());
      assertArrayEquals("2-abcdefg".getBytes(), newValues.get(1).getKey());
      assertArrayEquals("3-efghijk".getBytes(), newValues.get(2).getKey());
      assertArrayEquals("4-hijklmn".getBytes(), newValues.get(3).getKey());
      assertArrayEquals("5-hijklmn".getBytes(), newValues.get(4).getKey());

      // match-anywhere("1")
      options = new MultiGetOptions();
      options.sortKeyFilterType = FilterType.FT_MATCH_ANYWHERE;
      options.sortKeyFilterPattern = "1".getBytes();
      newValues.clear();
      ret = client.multiGet(tableName, hashKey, null, null, options, newValues);
      assertTrue(ret);
      assertEquals(2, newValues.size());
      assertArrayEquals("1".getBytes(), newValues.get(0).getKey());
      assertArrayEquals("1-abcdefg".getBytes(), newValues.get(1).getKey());

      // match-anywhere("1-")
      options = new MultiGetOptions();
      options.sortKeyFilterType = FilterType.FT_MATCH_ANYWHERE;
      options.sortKeyFilterPattern = "1-".getBytes();
      newValues.clear();
      ret = client.multiGet(tableName, hashKey, null, null, options, newValues);
      assertTrue(ret);
      assertEquals(1, newValues.size());
      assertArrayEquals("1-abcdefg".getBytes(), newValues.get(0).getKey());

      // match-anywhere("abc")
      options = new MultiGetOptions();
      options.sortKeyFilterType = FilterType.FT_MATCH_ANYWHERE;
      options.sortKeyFilterPattern = "abc".getBytes();
      newValues.clear();
      ret = client.multiGet(tableName, hashKey, null, null, options, newValues);
      assertTrue(ret);
      assertEquals(2, newValues.size());
      assertArrayEquals("1-abcdefg".getBytes(), newValues.get(0).getKey());
      assertArrayEquals("2-abcdefg".getBytes(), newValues.get(1).getKey());

      // match-prefix("1")
      options = new MultiGetOptions();
      options.sortKeyFilterType = FilterType.FT_MATCH_PREFIX;
      options.sortKeyFilterPattern = "1".getBytes();
      newValues.clear();
      ret = client.multiGet(tableName, hashKey, null, null, options, newValues);
      assertTrue(ret);
      assertEquals(2, newValues.size());
      assertArrayEquals("1".getBytes(), newValues.get(0).getKey());
      assertArrayEquals("1-abcdefg".getBytes(), newValues.get(1).getKey());

      // match-prefix("1") in [0, 1)
      options = new MultiGetOptions();
      options.sortKeyFilterType = FilterType.FT_MATCH_PREFIX;
      options.sortKeyFilterPattern = "1".getBytes();
      options.startInclusive = true;
      options.stopInclusive = false;
      newValues.clear();
      ret = client.multiGet(tableName, hashKey, "0".getBytes(), "1".getBytes(), options, newValues);
      assertTrue(ret);
      assertEquals(0, newValues.size());

      // match-prefix("1") in [0, 1]
      options = new MultiGetOptions();
      options.sortKeyFilterType = FilterType.FT_MATCH_PREFIX;
      options.sortKeyFilterPattern = "1".getBytes();
      options.startInclusive = true;
      options.stopInclusive = true;
      newValues.clear();
      ret = client.multiGet(tableName, hashKey, "0".getBytes(), "1".getBytes(), options, newValues);
      assertTrue(ret);
      assertEquals(1, newValues.size());
      assertArrayEquals("1".getBytes(), newValues.get(0).getKey());

      // match-prefix("1") in [1, 2]
      options = new MultiGetOptions();
      options.sortKeyFilterType = FilterType.FT_MATCH_PREFIX;
      options.sortKeyFilterPattern = "1".getBytes();
      options.startInclusive = true;
      options.stopInclusive = true;
      newValues.clear();
      ret = client.multiGet(tableName, hashKey, "1".getBytes(), "2".getBytes(), options, newValues);
      assertTrue(ret);
      assertEquals(2, newValues.size());
      assertArrayEquals("1".getBytes(), newValues.get(0).getKey());
      assertArrayEquals("1-abcdefg".getBytes(), newValues.get(1).getKey());

      // match-prefix("1") in (1, 2]
      options = new MultiGetOptions();
      options.sortKeyFilterType = FilterType.FT_MATCH_PREFIX;
      options.sortKeyFilterPattern = "1".getBytes();
      options.startInclusive = false;
      options.stopInclusive = true;
      newValues.clear();
      ret = client.multiGet(tableName, hashKey, "1".getBytes(), "2".getBytes(), options, newValues);
      assertTrue(ret);
      assertEquals(1, newValues.size());
      assertArrayEquals("1-abcdefg".getBytes(), newValues.get(0).getKey());

      // match-prefix("1") in (1-abcdefg, 2]
      options = new MultiGetOptions();
      options.sortKeyFilterType = FilterType.FT_MATCH_PREFIX;
      options.sortKeyFilterPattern = "1".getBytes();
      options.startInclusive = false;
      options.stopInclusive = true;
      newValues.clear();
      ret =
          client.multiGet(
              tableName, hashKey, "1-abcdefg".getBytes(), "2".getBytes(), options, newValues);
      assertTrue(ret);
      assertEquals(0, newValues.size());

      // match-prefix("1-")
      options = new MultiGetOptions();
      options.sortKeyFilterType = FilterType.FT_MATCH_PREFIX;
      options.sortKeyFilterPattern = "1-".getBytes();
      newValues.clear();
      ret = client.multiGet(tableName, hashKey, null, null, options, newValues);
      assertTrue(ret);
      assertEquals(1, newValues.size());
      assertArrayEquals("1-abcdefg".getBytes(), newValues.get(0).getKey());

      // match-prefix("1-x")
      options = new MultiGetOptions();
      options.sortKeyFilterType = FilterType.FT_MATCH_PREFIX;
      options.sortKeyFilterPattern = "1-x".getBytes();
      newValues.clear();
      ret = client.multiGet(tableName, hashKey, null, null, options, newValues);
      assertTrue(ret);
      assertEquals(0, newValues.size());

      // match-prefix("abc")
      options = new MultiGetOptions();
      options.sortKeyFilterType = FilterType.FT_MATCH_PREFIX;
      options.sortKeyFilterPattern = "abc".getBytes();
      newValues.clear();
      ret = client.multiGet(tableName, hashKey, null, null, options, newValues);
      assertTrue(ret);
      assertEquals(0, newValues.size());

      // match-prefix("efg")
      options = new MultiGetOptions();
      options.sortKeyFilterType = FilterType.FT_MATCH_PREFIX;
      options.sortKeyFilterPattern = "efg".getBytes();
      newValues.clear();
      ret = client.multiGet(tableName, hashKey, null, null, options, newValues);
      assertTrue(ret);
      assertEquals(0, newValues.size());

      // match-prefix("ijk")
      options = new MultiGetOptions();
      options.sortKeyFilterType = FilterType.FT_MATCH_PREFIX;
      options.sortKeyFilterPattern = "ijk".getBytes();
      newValues.clear();
      ret = client.multiGet(tableName, hashKey, null, null, options, newValues);
      assertTrue(ret);
      assertEquals(0, newValues.size());

      // match-prefix("lnm")
      options = new MultiGetOptions();
      options.sortKeyFilterType = FilterType.FT_MATCH_PREFIX;
      options.sortKeyFilterPattern = "lmn".getBytes();
      newValues.clear();
      ret = client.multiGet(tableName, hashKey, null, null, options, newValues);
      assertTrue(ret);
      assertEquals(0, newValues.size());

      // match-postfix("5-hijklmn")
      options = new MultiGetOptions();
      options.sortKeyFilterType = FilterType.FT_MATCH_PREFIX;
      options.sortKeyFilterPattern = "5-hijklmn".getBytes();
      newValues.clear();
      ret = client.multiGet(tableName, hashKey, null, null, options, newValues);
      assertTrue(ret);
      assertEquals(1, newValues.size());
      assertArrayEquals("5-hijklmn".getBytes(), newValues.get(0).getKey());

      // match-postfix("1")
      options = new MultiGetOptions();
      options.sortKeyFilterType = FilterType.FT_MATCH_POSTFIX;
      options.sortKeyFilterPattern = "1".getBytes();
      newValues.clear();
      ret = client.multiGet(tableName, hashKey, null, null, options, newValues);
      assertTrue(ret);
      assertEquals(1, newValues.size());
      assertArrayEquals("1".getBytes(), newValues.get(0).getKey());

      // match-postfix("1-")
      options = new MultiGetOptions();
      options.sortKeyFilterType = FilterType.FT_MATCH_POSTFIX;
      options.sortKeyFilterPattern = "1-".getBytes();
      newValues.clear();
      ret = client.multiGet(tableName, hashKey, null, null, options, newValues);
      assertTrue(ret);
      assertEquals(0, newValues.size());

      // match-postfix("1-x")
      options = new MultiGetOptions();
      options.sortKeyFilterType = FilterType.FT_MATCH_POSTFIX;
      options.sortKeyFilterPattern = "1-x".getBytes();
      newValues.clear();
      ret = client.multiGet(tableName, hashKey, null, null, options, newValues);
      assertTrue(ret);
      assertEquals(0, newValues.size());

      // match-postfix("abc")
      options = new MultiGetOptions();
      options.sortKeyFilterType = FilterType.FT_MATCH_POSTFIX;
      options.sortKeyFilterPattern = "abc".getBytes();
      newValues.clear();
      ret = client.multiGet(tableName, hashKey, null, null, options, newValues);
      assertTrue(ret);
      assertEquals(0, newValues.size());

      // match-postfix("efg")
      options = new MultiGetOptions();
      options.sortKeyFilterType = FilterType.FT_MATCH_POSTFIX;
      options.sortKeyFilterPattern = "efg".getBytes();
      newValues.clear();
      ret = client.multiGet(tableName, hashKey, null, null, options, newValues);
      assertTrue(ret);
      assertEquals(2, newValues.size());
      assertArrayEquals("1-abcdefg".getBytes(), newValues.get(0).getKey());
      assertArrayEquals("2-abcdefg".getBytes(), newValues.get(1).getKey());

      // match-postfix("ijk")
      options = new MultiGetOptions();
      options.sortKeyFilterType = FilterType.FT_MATCH_POSTFIX;
      options.sortKeyFilterPattern = "ijk".getBytes();
      newValues.clear();
      ret = client.multiGet(tableName, hashKey, null, null, options, newValues);
      assertTrue(ret);
      assertEquals(1, newValues.size());
      assertArrayEquals("3-efghijk".getBytes(), newValues.get(0).getKey());

      // match-postfix("lmn")
      options = new MultiGetOptions();
      options.sortKeyFilterType = FilterType.FT_MATCH_POSTFIX;
      options.sortKeyFilterPattern = "lmn".getBytes();
      newValues.clear();
      ret = client.multiGet(tableName, hashKey, null, null, options, newValues);
      assertTrue(ret);
      assertEquals(2, newValues.size());
      assertArrayEquals("4-hijklmn".getBytes(), newValues.get(0).getKey());
      assertArrayEquals("5-hijklmn".getBytes(), newValues.get(1).getKey());

      // match-postfix("5-hijklmn")
      options = new MultiGetOptions();
      options.sortKeyFilterType = FilterType.FT_MATCH_POSTFIX;
      options.sortKeyFilterPattern = "5-hijklmn".getBytes();
      newValues.clear();
      ret = client.multiGet(tableName, hashKey, null, null, options, newValues);
      assertTrue(ret);
      assertEquals(1, newValues.size());
      assertArrayEquals("5-hijklmn".getBytes(), newValues.get(0).getKey());

      // maxCount = 4
      options = new MultiGetOptions();
      newValues.clear();
      ret = client.multiGet(tableName, hashKey, null, null, options, 4, -1, newValues);
      assertFalse(ret);
      assertEquals(4, newValues.size());
      assertArrayEquals("".getBytes(), newValues.get(0).getKey());
      assertArrayEquals("1".getBytes(), newValues.get(1).getKey());
      assertArrayEquals("1-abcdefg".getBytes(), newValues.get(2).getKey());
      assertArrayEquals("2".getBytes(), newValues.get(3).getKey());

      // maxCount = 1
      options = new MultiGetOptions();
      newValues.clear();
      ret =
          client.multiGet(
              tableName, hashKey, "5".getBytes(), "6".getBytes(), options, 1, -1, newValues);
      assertFalse(ret);
      assertEquals(1, newValues.size());
      assertArrayEquals("5".getBytes(), newValues.get(0).getKey());

      // multi del all
      List<byte[]> sortKeys = new ArrayList<byte[]>();
      sortKeys.add("".getBytes());
      sortKeys.add("1".getBytes());
      sortKeys.add("1-abcdefg".getBytes());
      sortKeys.add("2".getBytes());
      sortKeys.add("2-abcdefg".getBytes());
      sortKeys.add("3".getBytes());
      sortKeys.add("3-efghijk".getBytes());
      sortKeys.add("4".getBytes());
      sortKeys.add("4-hijklmn".getBytes());
      sortKeys.add("5".getBytes());
      sortKeys.add("5-hijklmn".getBytes());
      sortKeys.add("6".getBytes());
      sortKeys.add("7".getBytes());
      client.multiDel(tableName, hashKey, sortKeys);

      // check sortkey count
      sortKeyCount = client.sortKeyCount(tableName, hashKey);
      assertEquals(0, sortKeyCount);
    } catch (PException e) {
      e.printStackTrace();
      assertTrue(false);
    }

    PegasusClientFactory.closeSingletonClient();
  }

  @Test
  public void testMultiGetReverse() throws PException {
    PegasusClientInterface client = PegasusClientFactory.getSingletonClient();
    String tableName = "temp";
    byte[] hashKey = multiGetReverseHashKey.getBytes();

    try {
      // multi set
      List<Pair<byte[], byte[]>> values = new ArrayList<Pair<byte[], byte[]>>();
      values.add(Pair.of("".getBytes(), "0".getBytes()));
      values.add(Pair.of("1".getBytes(), "1".getBytes()));
      values.add(Pair.of("1-abcdefg".getBytes(), "1-abcdefg".getBytes()));
      values.add(Pair.of("2".getBytes(), "2".getBytes()));
      values.add(Pair.of("2-abcdefg".getBytes(), "2-abcdefg".getBytes()));
      values.add(Pair.of("3".getBytes(), "3".getBytes()));
      values.add(Pair.of("3-efghijk".getBytes(), "3-efghijk".getBytes()));
      values.add(Pair.of("4".getBytes(), "4".getBytes()));
      values.add(Pair.of("4-hijklmn".getBytes(), "4-hijklmn".getBytes()));
      values.add(Pair.of("5".getBytes(), "5".getBytes()));
      values.add(Pair.of("5-hijklmn".getBytes(), "5-hijklmn".getBytes()));
      values.add(Pair.of("6".getBytes(), "6".getBytes()));
      values.add(Pair.of("7".getBytes(), "7".getBytes()));
      client.multiSet(tableName, hashKey, values);

      // check sortkey count
      long sortKeyCount = client.sortKeyCount(tableName, hashKey);
      assertEquals(13, sortKeyCount);

      // [null, null)
      MultiGetOptions options = new MultiGetOptions();
      assertTrue(options.startInclusive);
      assertFalse(options.stopInclusive);
      options.reverse = true;
      List<Pair<byte[], byte[]>> newValues = new ArrayList<Pair<byte[], byte[]>>();
      boolean ret = client.multiGet(tableName, hashKey, null, null, options, newValues);
      assertTrue(ret);
      assertEquals(13, newValues.size());
      assertArrayEquals("".getBytes(), newValues.get(0).getKey());
      assertArrayEquals("1".getBytes(), newValues.get(1).getKey());
      assertArrayEquals("1-abcdefg".getBytes(), newValues.get(2).getKey());
      assertArrayEquals("2".getBytes(), newValues.get(3).getKey());
      assertArrayEquals("2-abcdefg".getBytes(), newValues.get(4).getKey());
      assertArrayEquals("3".getBytes(), newValues.get(5).getKey());
      assertArrayEquals("3-efghijk".getBytes(), newValues.get(6).getKey());
      assertArrayEquals("4".getBytes(), newValues.get(7).getKey());
      assertArrayEquals("4-hijklmn".getBytes(), newValues.get(8).getKey());
      assertArrayEquals("5".getBytes(), newValues.get(9).getKey());
      assertArrayEquals("5-hijklmn".getBytes(), newValues.get(10).getKey());
      assertArrayEquals("6".getBytes(), newValues.get(11).getKey());
      assertArrayEquals("7".getBytes(), newValues.get(12).getKey());

      // [null, null]
      options = new MultiGetOptions();
      options.startInclusive = true;
      options.stopInclusive = true;
      options.reverse = true;
      newValues.clear();
      ret = client.multiGet(tableName, hashKey, null, null, options, newValues);
      assertTrue(ret);
      assertEquals(13, newValues.size());
      assertArrayEquals("".getBytes(), newValues.get(0).getKey());
      assertArrayEquals("1".getBytes(), newValues.get(1).getKey());
      assertArrayEquals("1-abcdefg".getBytes(), newValues.get(2).getKey());
      assertArrayEquals("2".getBytes(), newValues.get(3).getKey());
      assertArrayEquals("2-abcdefg".getBytes(), newValues.get(4).getKey());
      assertArrayEquals("3".getBytes(), newValues.get(5).getKey());
      assertArrayEquals("3-efghijk".getBytes(), newValues.get(6).getKey());
      assertArrayEquals("4".getBytes(), newValues.get(7).getKey());
      assertArrayEquals("4-hijklmn".getBytes(), newValues.get(8).getKey());
      assertArrayEquals("5".getBytes(), newValues.get(9).getKey());
      assertArrayEquals("5-hijklmn".getBytes(), newValues.get(10).getKey());
      assertArrayEquals("6".getBytes(), newValues.get(11).getKey());
      assertArrayEquals("7".getBytes(), newValues.get(12).getKey());

      // (null, null)
      options = new MultiGetOptions();
      options.startInclusive = false;
      options.stopInclusive = false;
      options.reverse = true;
      newValues.clear();
      ret = client.multiGet(tableName, hashKey, null, null, options, newValues);
      assertTrue(ret);
      assertEquals(12, newValues.size());
      assertArrayEquals("1".getBytes(), newValues.get(0).getKey());
      assertArrayEquals("1-abcdefg".getBytes(), newValues.get(1).getKey());
      assertArrayEquals("2".getBytes(), newValues.get(2).getKey());
      assertArrayEquals("2-abcdefg".getBytes(), newValues.get(3).getKey());
      assertArrayEquals("3".getBytes(), newValues.get(4).getKey());
      assertArrayEquals("3-efghijk".getBytes(), newValues.get(5).getKey());
      assertArrayEquals("4".getBytes(), newValues.get(6).getKey());
      assertArrayEquals("4-hijklmn".getBytes(), newValues.get(7).getKey());
      assertArrayEquals("5".getBytes(), newValues.get(8).getKey());
      assertArrayEquals("5-hijklmn".getBytes(), newValues.get(9).getKey());
      assertArrayEquals("6".getBytes(), newValues.get(10).getKey());
      assertArrayEquals("7".getBytes(), newValues.get(11).getKey());

      // (null, null]
      options = new MultiGetOptions();
      options.startInclusive = false;
      options.stopInclusive = true;
      options.reverse = true;
      newValues.clear();
      ret = client.multiGet(tableName, hashKey, null, null, options, newValues);
      assertTrue(ret);
      assertEquals(12, newValues.size());
      assertArrayEquals("1".getBytes(), newValues.get(0).getKey());
      assertArrayEquals("1-abcdefg".getBytes(), newValues.get(1).getKey());
      assertArrayEquals("2".getBytes(), newValues.get(2).getKey());
      assertArrayEquals("2-abcdefg".getBytes(), newValues.get(3).getKey());
      assertArrayEquals("3".getBytes(), newValues.get(4).getKey());
      assertArrayEquals("3-efghijk".getBytes(), newValues.get(5).getKey());
      assertArrayEquals("4".getBytes(), newValues.get(6).getKey());
      assertArrayEquals("4-hijklmn".getBytes(), newValues.get(7).getKey());
      assertArrayEquals("5".getBytes(), newValues.get(8).getKey());
      assertArrayEquals("5-hijklmn".getBytes(), newValues.get(9).getKey());
      assertArrayEquals("6".getBytes(), newValues.get(10).getKey());
      assertArrayEquals("7".getBytes(), newValues.get(11).getKey());

      // [null, 1]
      options = new MultiGetOptions();
      options.startInclusive = true;
      options.stopInclusive = true;
      options.reverse = true;
      newValues.clear();
      ret = client.multiGet(tableName, hashKey, null, "1".getBytes(), options, newValues);
      assertTrue(ret);
      assertEquals(2, newValues.size());
      assertArrayEquals("".getBytes(), newValues.get(0).getKey());
      assertArrayEquals("1".getBytes(), newValues.get(1).getKey());

      // [null, 1)
      options = new MultiGetOptions();
      options.startInclusive = true;
      options.stopInclusive = false;
      options.reverse = true;
      newValues.clear();
      ret = client.multiGet(tableName, hashKey, null, "1".getBytes(), options, newValues);
      assertTrue(ret);
      assertEquals(1, newValues.size());
      assertArrayEquals("".getBytes(), newValues.get(0).getKey());

      // (null, 1]
      options = new MultiGetOptions();
      options.startInclusive = false;
      options.stopInclusive = true;
      options.reverse = true;
      newValues.clear();
      ret = client.multiGet(tableName, hashKey, null, "1".getBytes(), options, newValues);
      assertTrue(ret);
      assertEquals(1, newValues.size());
      assertArrayEquals("1".getBytes(), newValues.get(0).getKey());

      // (null, 1)
      options = new MultiGetOptions();
      options.startInclusive = false;
      options.stopInclusive = false;
      options.reverse = true;
      newValues.clear();
      ret = client.multiGet(tableName, hashKey, null, "1".getBytes(), options, newValues);
      assertTrue(ret);
      assertEquals(0, newValues.size());

      // [1, 1]
      options = new MultiGetOptions();
      options.startInclusive = true;
      options.stopInclusive = true;
      options.reverse = true;
      newValues.clear();
      ret = client.multiGet(tableName, hashKey, "1".getBytes(), "1".getBytes(), options, newValues);
      assertTrue(ret);
      assertEquals(1, newValues.size());
      assertArrayEquals("1".getBytes(), newValues.get(0).getKey());

      // [1, 1)
      options = new MultiGetOptions();
      options.startInclusive = true;
      options.stopInclusive = false;
      options.reverse = true;
      newValues.clear();
      ret = client.multiGet(tableName, hashKey, "1".getBytes(), "1".getBytes(), options, newValues);
      assertTrue(ret);
      assertEquals(0, newValues.size());

      // (1, 1]
      options = new MultiGetOptions();
      options.startInclusive = false;
      options.stopInclusive = true;
      options.reverse = true;
      newValues.clear();
      ret = client.multiGet(tableName, hashKey, "1".getBytes(), "1".getBytes(), options, newValues);
      assertTrue(ret);
      assertEquals(0, newValues.size());

      // (1, 1)
      options = new MultiGetOptions();
      options.startInclusive = false;
      options.stopInclusive = false;
      options.reverse = true;
      newValues.clear();
      ret = client.multiGet(tableName, hashKey, "1".getBytes(), "1".getBytes(), options, newValues);
      assertTrue(ret);
      assertEquals(0, newValues.size());

      // [2, 1]
      options = new MultiGetOptions();
      options.startInclusive = false;
      options.stopInclusive = false;
      options.reverse = true;
      newValues.clear();
      ret = client.multiGet(tableName, hashKey, "2".getBytes(), "1".getBytes(), options, newValues);
      assertTrue(ret);
      assertEquals(0, newValues.size());

      // match-anywhere("-")
      options = new MultiGetOptions();
      options.sortKeyFilterType = FilterType.FT_MATCH_ANYWHERE;
      options.sortKeyFilterPattern = "-".getBytes();
      options.reverse = true;
      newValues.clear();
      ret = client.multiGet(tableName, hashKey, null, null, options, newValues);
      assertTrue(ret);
      assertEquals(5, newValues.size());
      assertArrayEquals("1-abcdefg".getBytes(), newValues.get(0).getKey());
      assertArrayEquals("2-abcdefg".getBytes(), newValues.get(1).getKey());
      assertArrayEquals("3-efghijk".getBytes(), newValues.get(2).getKey());
      assertArrayEquals("4-hijklmn".getBytes(), newValues.get(3).getKey());
      assertArrayEquals("5-hijklmn".getBytes(), newValues.get(4).getKey());

      // match-anywhere("1")
      options = new MultiGetOptions();
      options.sortKeyFilterType = FilterType.FT_MATCH_ANYWHERE;
      options.sortKeyFilterPattern = "1".getBytes();
      options.reverse = true;
      newValues.clear();
      ret = client.multiGet(tableName, hashKey, null, null, options, newValues);
      assertTrue(ret);
      assertEquals(2, newValues.size());
      assertArrayEquals("1".getBytes(), newValues.get(0).getKey());
      assertArrayEquals("1-abcdefg".getBytes(), newValues.get(1).getKey());

      // match-anywhere("1-")
      options = new MultiGetOptions();
      options.sortKeyFilterType = FilterType.FT_MATCH_ANYWHERE;
      options.sortKeyFilterPattern = "1-".getBytes();
      options.reverse = true;
      newValues.clear();
      ret = client.multiGet(tableName, hashKey, null, null, options, newValues);
      assertTrue(ret);
      assertEquals(1, newValues.size());
      assertArrayEquals("1-abcdefg".getBytes(), newValues.get(0).getKey());

      // match-anywhere("abc")
      options = new MultiGetOptions();
      options.sortKeyFilterType = FilterType.FT_MATCH_ANYWHERE;
      options.sortKeyFilterPattern = "abc".getBytes();
      options.reverse = true;
      newValues.clear();
      ret = client.multiGet(tableName, hashKey, null, null, options, newValues);
      assertTrue(ret);
      assertEquals(2, newValues.size());
      assertArrayEquals("1-abcdefg".getBytes(), newValues.get(0).getKey());
      assertArrayEquals("2-abcdefg".getBytes(), newValues.get(1).getKey());

      // match-prefix("1")
      options = new MultiGetOptions();
      options.sortKeyFilterType = FilterType.FT_MATCH_PREFIX;
      options.sortKeyFilterPattern = "1".getBytes();
      options.reverse = true;
      newValues.clear();
      ret = client.multiGet(tableName, hashKey, null, null, options, newValues);
      assertTrue(ret);
      assertEquals(2, newValues.size());
      assertArrayEquals("1".getBytes(), newValues.get(0).getKey());
      assertArrayEquals("1-abcdefg".getBytes(), newValues.get(1).getKey());

      // match-prefix("1") in [0, 1)
      options = new MultiGetOptions();
      options.sortKeyFilterType = FilterType.FT_MATCH_PREFIX;
      options.sortKeyFilterPattern = "1".getBytes();
      options.startInclusive = true;
      options.stopInclusive = false;
      options.reverse = true;
      newValues.clear();
      ret = client.multiGet(tableName, hashKey, "0".getBytes(), "1".getBytes(), options, newValues);
      assertTrue(ret);
      assertEquals(0, newValues.size());

      // match-prefix("1") in [0, 1]
      options = new MultiGetOptions();
      options.sortKeyFilterType = FilterType.FT_MATCH_PREFIX;
      options.sortKeyFilterPattern = "1".getBytes();
      options.startInclusive = true;
      options.stopInclusive = true;
      options.reverse = true;
      newValues.clear();
      ret = client.multiGet(tableName, hashKey, "0".getBytes(), "1".getBytes(), options, newValues);
      assertTrue(ret);
      assertEquals(1, newValues.size());
      assertArrayEquals("1".getBytes(), newValues.get(0).getKey());

      // match-prefix("1") in [1, 2]
      options = new MultiGetOptions();
      options.sortKeyFilterType = FilterType.FT_MATCH_PREFIX;
      options.sortKeyFilterPattern = "1".getBytes();
      options.startInclusive = true;
      options.stopInclusive = true;
      options.reverse = true;
      newValues.clear();
      ret = client.multiGet(tableName, hashKey, "1".getBytes(), "2".getBytes(), options, newValues);
      assertTrue(ret);
      assertEquals(2, newValues.size());
      assertArrayEquals("1".getBytes(), newValues.get(0).getKey());
      assertArrayEquals("1-abcdefg".getBytes(), newValues.get(1).getKey());

      // match-prefix("1") in (1, 2]
      options = new MultiGetOptions();
      options.sortKeyFilterType = FilterType.FT_MATCH_PREFIX;
      options.sortKeyFilterPattern = "1".getBytes();
      options.startInclusive = false;
      options.stopInclusive = true;
      options.reverse = true;
      newValues.clear();
      ret = client.multiGet(tableName, hashKey, "1".getBytes(), "2".getBytes(), options, newValues);
      assertTrue(ret);
      assertEquals(1, newValues.size());
      assertArrayEquals("1-abcdefg".getBytes(), newValues.get(0).getKey());

      // match-prefix("1") in (1-abcdefg, 2]
      options = new MultiGetOptions();
      options.sortKeyFilterType = FilterType.FT_MATCH_PREFIX;
      options.sortKeyFilterPattern = "1".getBytes();
      options.startInclusive = false;
      options.stopInclusive = true;
      options.reverse = true;
      newValues.clear();
      ret =
          client.multiGet(
              tableName, hashKey, "1-abcdefg".getBytes(), "2".getBytes(), options, newValues);
      assertTrue(ret);
      assertEquals(0, newValues.size());

      // match-prefix("1-")
      options = new MultiGetOptions();
      options.sortKeyFilterType = FilterType.FT_MATCH_PREFIX;
      options.sortKeyFilterPattern = "1-".getBytes();
      options.reverse = true;
      newValues.clear();
      ret = client.multiGet(tableName, hashKey, null, null, options, newValues);
      assertTrue(ret);
      assertEquals(1, newValues.size());
      assertArrayEquals("1-abcdefg".getBytes(), newValues.get(0).getKey());

      // match-prefix("1-x")
      options = new MultiGetOptions();
      options.sortKeyFilterType = FilterType.FT_MATCH_PREFIX;
      options.sortKeyFilterPattern = "1-x".getBytes();
      options.reverse = true;
      newValues.clear();
      ret = client.multiGet(tableName, hashKey, null, null, options, newValues);
      assertTrue(ret);
      assertEquals(0, newValues.size());

      // match-prefix("abc")
      options = new MultiGetOptions();
      options.sortKeyFilterType = FilterType.FT_MATCH_PREFIX;
      options.sortKeyFilterPattern = "abc".getBytes();
      options.reverse = true;
      newValues.clear();
      ret = client.multiGet(tableName, hashKey, null, null, options, newValues);
      assertTrue(ret);
      assertEquals(0, newValues.size());

      // match-prefix("efg")
      options = new MultiGetOptions();
      options.sortKeyFilterType = FilterType.FT_MATCH_PREFIX;
      options.sortKeyFilterPattern = "efg".getBytes();
      options.reverse = true;
      newValues.clear();
      ret = client.multiGet(tableName, hashKey, null, null, options, newValues);
      assertTrue(ret);
      assertEquals(0, newValues.size());

      // match-prefix("ijk")
      options = new MultiGetOptions();
      options.sortKeyFilterType = FilterType.FT_MATCH_PREFIX;
      options.sortKeyFilterPattern = "ijk".getBytes();
      options.reverse = true;
      newValues.clear();
      ret = client.multiGet(tableName, hashKey, null, null, options, newValues);
      assertTrue(ret);
      assertEquals(0, newValues.size());

      // match-prefix("lnm")
      options = new MultiGetOptions();
      options.sortKeyFilterType = FilterType.FT_MATCH_PREFIX;
      options.sortKeyFilterPattern = "lmn".getBytes();
      options.reverse = true;
      newValues.clear();
      ret = client.multiGet(tableName, hashKey, null, null, options, newValues);
      assertTrue(ret);
      assertEquals(0, newValues.size());

      // match-postfix("5-hijklmn")
      options = new MultiGetOptions();
      options.sortKeyFilterType = FilterType.FT_MATCH_PREFIX;
      options.sortKeyFilterPattern = "5-hijklmn".getBytes();
      options.reverse = true;
      newValues.clear();
      ret = client.multiGet(tableName, hashKey, null, null, options, newValues);
      assertTrue(ret);
      assertEquals(1, newValues.size());
      assertArrayEquals("5-hijklmn".getBytes(), newValues.get(0).getKey());

      // match-postfix("1")
      options = new MultiGetOptions();
      options.sortKeyFilterType = FilterType.FT_MATCH_POSTFIX;
      options.sortKeyFilterPattern = "1".getBytes();
      options.reverse = true;
      newValues.clear();
      ret = client.multiGet(tableName, hashKey, null, null, options, newValues);
      assertTrue(ret);
      assertEquals(1, newValues.size());
      assertArrayEquals("1".getBytes(), newValues.get(0).getKey());

      // match-postfix("1-")
      options = new MultiGetOptions();
      options.sortKeyFilterType = FilterType.FT_MATCH_POSTFIX;
      options.sortKeyFilterPattern = "1-".getBytes();
      options.reverse = true;
      newValues.clear();
      ret = client.multiGet(tableName, hashKey, null, null, options, newValues);
      assertTrue(ret);
      assertEquals(0, newValues.size());

      // match-postfix("1-x")
      options = new MultiGetOptions();
      options.sortKeyFilterType = FilterType.FT_MATCH_POSTFIX;
      options.sortKeyFilterPattern = "1-x".getBytes();
      options.reverse = true;
      newValues.clear();
      ret = client.multiGet(tableName, hashKey, null, null, options, newValues);
      assertTrue(ret);
      assertEquals(0, newValues.size());

      // match-postfix("abc")
      options = new MultiGetOptions();
      options.sortKeyFilterType = FilterType.FT_MATCH_POSTFIX;
      options.sortKeyFilterPattern = "abc".getBytes();
      options.reverse = true;
      newValues.clear();
      ret = client.multiGet(tableName, hashKey, null, null, options, newValues);
      assertTrue(ret);
      assertEquals(0, newValues.size());

      // match-postfix("efg")
      options = new MultiGetOptions();
      options.sortKeyFilterType = FilterType.FT_MATCH_POSTFIX;
      options.sortKeyFilterPattern = "efg".getBytes();
      options.reverse = true;
      newValues.clear();
      ret = client.multiGet(tableName, hashKey, null, null, options, newValues);
      assertTrue(ret);
      assertEquals(2, newValues.size());
      assertArrayEquals("1-abcdefg".getBytes(), newValues.get(0).getKey());
      assertArrayEquals("2-abcdefg".getBytes(), newValues.get(1).getKey());

      // match-postfix("ijk")
      options = new MultiGetOptions();
      options.sortKeyFilterType = FilterType.FT_MATCH_POSTFIX;
      options.sortKeyFilterPattern = "ijk".getBytes();
      options.reverse = true;
      newValues.clear();
      ret = client.multiGet(tableName, hashKey, null, null, options, newValues);
      assertTrue(ret);
      assertEquals(1, newValues.size());
      assertArrayEquals("3-efghijk".getBytes(), newValues.get(0).getKey());

      // match-postfix("lmn")
      options = new MultiGetOptions();
      options.sortKeyFilterType = FilterType.FT_MATCH_POSTFIX;
      options.sortKeyFilterPattern = "lmn".getBytes();
      options.reverse = true;
      newValues.clear();
      ret = client.multiGet(tableName, hashKey, null, null, options, newValues);
      assertTrue(ret);
      assertEquals(2, newValues.size());
      assertArrayEquals("4-hijklmn".getBytes(), newValues.get(0).getKey());
      assertArrayEquals("5-hijklmn".getBytes(), newValues.get(1).getKey());

      // match-postfix("5-hijklmn")
      options = new MultiGetOptions();
      options.sortKeyFilterType = FilterType.FT_MATCH_POSTFIX;
      options.sortKeyFilterPattern = "5-hijklmn".getBytes();
      options.reverse = true;
      newValues.clear();
      ret = client.multiGet(tableName, hashKey, null, null, options, newValues);
      assertTrue(ret);
      assertEquals(1, newValues.size());
      assertArrayEquals("5-hijklmn".getBytes(), newValues.get(0).getKey());

      // maxCount = 4
      options = new MultiGetOptions();
      options.reverse = true;
      newValues.clear();
      ret = client.multiGet(tableName, hashKey, null, null, options, 4, -1, newValues);
      assertFalse(ret);
      assertEquals(4, newValues.size());
      assertArrayEquals("5".getBytes(), newValues.get(0).getKey());
      assertArrayEquals("5-hijklmn".getBytes(), newValues.get(1).getKey());
      assertArrayEquals("6".getBytes(), newValues.get(2).getKey());
      assertArrayEquals("7".getBytes(), newValues.get(3).getKey());

      // maxCount = 1
      options = new MultiGetOptions();
      options.startInclusive = true;
      options.stopInclusive = true;
      options.reverse = true;
      newValues.clear();
      ret =
          client.multiGet(
              tableName, hashKey, "5".getBytes(), "6".getBytes(), options, 1, -1, newValues);
      assertFalse(ret);
      assertEquals(1, newValues.size());
      assertArrayEquals("6".getBytes(), newValues.get(0).getKey());

      // multi del all
      List<byte[]> sortKeys = new ArrayList<byte[]>();
      sortKeys.add("".getBytes());
      sortKeys.add("1".getBytes());
      sortKeys.add("1-abcdefg".getBytes());
      sortKeys.add("2".getBytes());
      sortKeys.add("2-abcdefg".getBytes());
      sortKeys.add("3".getBytes());
      sortKeys.add("3-efghijk".getBytes());
      sortKeys.add("4".getBytes());
      sortKeys.add("4-hijklmn".getBytes());
      sortKeys.add("5".getBytes());
      sortKeys.add("5-hijklmn".getBytes());
      sortKeys.add("6".getBytes());
      sortKeys.add("7".getBytes());
      client.multiDel(tableName, hashKey, sortKeys);

      // check sortkey count
      sortKeyCount = client.sortKeyCount(tableName, hashKey);
      assertEquals(0, sortKeyCount);
    } catch (PException e) {
      e.printStackTrace();
      assertTrue(false);
    }

    PegasusClientFactory.closeSingletonClient();
  }

  @Test
  public void testBatchSetGetDel() throws PException {
    PegasusClientInterface client = PegasusClientFactory.getSingletonClient();
    String tableName = "temp";

    try {
      // batch set
      List<SetItem> items = new ArrayList<SetItem>();
      items.add(
          new SetItem(
              "TestBasic_testBatchSetGetDel_hash_key_1".getBytes(),
              "basic_test_sort_key".getBytes(),
              "basic_test_value_1".getBytes()));
      items.add(
          new SetItem(
              "TestBasic_testBatchSetGetDel_hash_key_2".getBytes(),
              "basic_test_sort_key".getBytes(),
              "basic_test_value_2".getBytes()));
      items.add(
          new SetItem(
              "TestBasic_testBatchSetGetDel_hash_key_3".getBytes(),
              "basic_test_sort_key".getBytes(),
              "basic_test_value_3".getBytes()));
      client.batchSet(tableName, items);

      // check exist
      boolean exist =
          client.exist(
              tableName,
              "TestBasic_testBatchSetGetDel_hash_key_1".getBytes(),
              "basic_test_sort_key".getBytes());
      assertTrue(exist);
      exist =
          client.exist(
              tableName,
              "TestBasic_testBatchSetGetDel_hash_key_2".getBytes(),
              "basic_test_sort_key".getBytes());
      assertTrue(exist);
      exist =
          client.exist(
              tableName,
              "TestBasic_testBatchSetGetDel_hash_key_3".getBytes(),
              "basic_test_sort_key".getBytes());
      assertTrue(exist);

      // batch get
      List<Pair<byte[], byte[]>> keys = new ArrayList<Pair<byte[], byte[]>>();
      keys.add(
          Pair.of(
              "TestBasic_testBatchSetGetDel_hash_key_1".getBytes(),
              "basic_test_sort_key".getBytes()));
      keys.add(
          Pair.of(
              "TestBasic_testBatchSetGetDel_hash_key_2".getBytes(),
              "basic_test_sort_key".getBytes()));
      keys.add(
          Pair.of(
              "TestBasic_testBatchSetGetDel_hash_key_3".getBytes(),
              "basic_test_sort_key".getBytes()));
      List<byte[]> values = new ArrayList<byte[]>();
      client.batchGet(tableName, keys, values);
      assertEquals(3, values.size());
      assertArrayEquals("basic_test_value_1".getBytes(), values.get(0));
      assertArrayEquals("basic_test_value_2".getBytes(), values.get(1));
      assertArrayEquals("basic_test_value_3".getBytes(), values.get(2));

      // batch del
      client.batchDel(tableName, keys);

      // check deleted
      client.batchGet(tableName, keys, values);
      assertEquals(3, values.size());
      assertNull(values.get(0));
      assertNull(values.get(1));
      assertNull(values.get(2));
    } catch (PException e) {
      e.printStackTrace();
      assertTrue(false);
    }

    PegasusClientFactory.closeSingletonClient();
  }

  @Test
  public void testBatchSetGetDel2() throws PException {
    PegasusClientInterface client = PegasusClientFactory.getSingletonClient();
    String tableName = "temp";

    try {
      // batch set
      List<SetItem> items = new ArrayList<SetItem>();
      items.add(
          new SetItem(
              "TestBasic_testBatchSetGetDel2_hash_key_1".getBytes(),
              "basic_test_sort_key".getBytes(),
              "basic_test_value_1".getBytes()));
      items.add(
          new SetItem(
              "TestBasic_testBatchSetGetDel2_hash_key_2".getBytes(),
              "basic_test_sort_key".getBytes(),
              "basic_test_value_2".getBytes()));
      items.add(
          new SetItem(
              "TestBasic_testBatchSetGetDel2_hash_key_3".getBytes(),
              "basic_test_sort_key".getBytes(),
              "basic_test_value_3".getBytes()));
      List<PException> resultsBatchSet = new ArrayList<PException>();
      int count = client.batchSet2(tableName, items, resultsBatchSet);
      assertEquals(3, count);
      assertEquals(3, resultsBatchSet.size());
      assertNull(resultsBatchSet.get(0));
      assertNull(resultsBatchSet.get(1));
      assertNull(resultsBatchSet.get(2));

      // check exist
      boolean exist =
          client.exist(
              tableName,
              "TestBasic_testBatchSetGetDel2_hash_key_1".getBytes(),
              "basic_test_sort_key".getBytes());
      assertTrue(exist);
      exist =
          client.exist(
              tableName,
              "TestBasic_testBatchSetGetDel2_hash_key_2".getBytes(),
              "basic_test_sort_key".getBytes());
      assertTrue(exist);
      exist =
          client.exist(
              tableName,
              "TestBasic_testBatchSetGetDel2_hash_key_3".getBytes(),
              "basic_test_sort_key".getBytes());
      assertTrue(exist);

      // batch get
      List<Pair<byte[], byte[]>> keys = new ArrayList<Pair<byte[], byte[]>>();
      keys.add(
          Pair.of(
              "TestBasic_testBatchSetGetDel2_hash_key_1".getBytes(),
              "basic_test_sort_key".getBytes()));
      keys.add(
          Pair.of(
              "TestBasic_testBatchSetGetDel2_hash_key_2".getBytes(),
              "basic_test_sort_key".getBytes()));
      keys.add(
          Pair.of(
              "TestBasic_testBatchSetGetDel2_hash_key_3".getBytes(),
              "basic_test_sort_key".getBytes()));
      List<Pair<PException, byte[]>> resultsBatchGet = new ArrayList<Pair<PException, byte[]>>();
      count = client.batchGet2(tableName, keys, resultsBatchGet);
      assertEquals(3, count);
      assertEquals(3, resultsBatchGet.size());
      assertNull(resultsBatchGet.get(0).getLeft());
      assertArrayEquals("basic_test_value_1".getBytes(), resultsBatchGet.get(0).getRight());
      assertNull(resultsBatchGet.get(1).getLeft());
      assertArrayEquals("basic_test_value_2".getBytes(), resultsBatchGet.get(1).getRight());
      assertNull(resultsBatchGet.get(2).getLeft());
      assertArrayEquals("basic_test_value_3".getBytes(), resultsBatchGet.get(2).getRight());

      // batch del
      List<PException> resultsBatchDel = new ArrayList<PException>();
      count = client.batchDel2(tableName, keys, resultsBatchDel);
      assertEquals(3, count);
      assertEquals(3, resultsBatchSet.size());
      assertNull(resultsBatchSet.get(0));
      assertNull(resultsBatchSet.get(1));
      assertNull(resultsBatchSet.get(2));

      // check deleted
      List<byte[]> values = new ArrayList<byte[]>();
      client.batchGet(tableName, keys, values);
      assertEquals(3, values.size());
      assertNull(values.get(0));
      assertNull(values.get(1));
      assertNull(values.get(2));
    } catch (PException e) {
      e.printStackTrace();
      assertTrue(false);
    }

    PegasusClientFactory.closeSingletonClient();
  }

  @Test
  public void testBatchMultiSetGetDel() throws PException {
    PegasusClientInterface client = PegasusClientFactory.getSingletonClient();
    String tableName = "temp";

    try {
      // batch multi set
      List<HashKeyData> items = new ArrayList<HashKeyData>();
      items.add(new HashKeyData("TestBasic_testBatchMultiSetGetDel_hash_key_1".getBytes()));
      items
          .get(items.size() - 1)
          .addData("basic_test_sort_key_1".getBytes(), "basic_test_value_1".getBytes());
      items
          .get(items.size() - 1)
          .addData("basic_test_sort_key_2".getBytes(), "basic_test_value_2".getBytes());
      items
          .get(items.size() - 1)
          .addData("basic_test_sort_key_3".getBytes(), "basic_test_value_3".getBytes());
      items.add(new HashKeyData("TestBasic_testBatchMultiSetGetDel_hash_key_2".getBytes()));
      items
          .get(items.size() - 1)
          .addData("basic_test_sort_key_1".getBytes(), "basic_test_value_1".getBytes());
      items
          .get(items.size() - 1)
          .addData("basic_test_sort_key_2".getBytes(), "basic_test_value_2".getBytes());
      items.add(new HashKeyData("TestBasic_testBatchMultiSetGetDel_hash_key_3".getBytes()));
      items
          .get(items.size() - 1)
          .addData("basic_test_sort_key_1".getBytes(), "basic_test_value_1".getBytes());
      client.batchMultiSet(tableName, items);

      // batch multi get
      List<Pair<byte[], List<byte[]>>> keys = new ArrayList<Pair<byte[], List<byte[]>>>();
      List<byte[]> nullSortKeys = null;
      keys.add(Pair.of("TestBasic_testBatchMultiSetGetDel_hash_key_1".getBytes(), nullSortKeys));
      keys.add(Pair.of("TestBasic_testBatchMultiSetGetDel_hash_key_2".getBytes(), nullSortKeys));
      keys.add(Pair.of("TestBasic_testBatchMultiSetGetDel_hash_key_3".getBytes(), nullSortKeys));
      keys.add(Pair.of("TestBasic_testBatchMultiSetGetDel_hash_key_4".getBytes(), nullSortKeys));
      List<HashKeyData> values = new ArrayList<HashKeyData>();
      client.batchMultiGet(tableName, keys, values);
      assertEquals(4, values.size());

      assertTrue(values.get(0).isAllFetched());
      assertArrayEquals(keys.get(0).getLeft(), values.get(0).hashKey);
      assertEquals(3, values.get(0).values.size());
      assertArrayEquals("basic_test_sort_key_1".getBytes(), values.get(0).values.get(0).getLeft());
      assertArrayEquals("basic_test_value_1".getBytes(), values.get(0).values.get(0).getRight());
      assertArrayEquals("basic_test_sort_key_2".getBytes(), values.get(0).values.get(1).getLeft());
      assertArrayEquals("basic_test_value_2".getBytes(), values.get(0).values.get(1).getRight());
      assertArrayEquals("basic_test_sort_key_3".getBytes(), values.get(0).values.get(2).getLeft());
      assertArrayEquals("basic_test_value_3".getBytes(), values.get(0).values.get(2).getRight());

      assertTrue(values.get(1).isAllFetched());
      assertArrayEquals(keys.get(1).getLeft(), values.get(1).hashKey);
      assertEquals(2, values.get(1).values.size());
      assertArrayEquals("basic_test_sort_key_1".getBytes(), values.get(1).values.get(0).getLeft());
      assertArrayEquals("basic_test_value_1".getBytes(), values.get(1).values.get(0).getRight());
      assertArrayEquals("basic_test_sort_key_2".getBytes(), values.get(1).values.get(1).getLeft());
      assertArrayEquals("basic_test_value_2".getBytes(), values.get(1).values.get(1).getRight());

      assertTrue(values.get(2).isAllFetched());
      assertArrayEquals(keys.get(2).getLeft(), values.get(2).hashKey);
      assertEquals(1, values.get(2).values.size());
      assertArrayEquals("basic_test_sort_key_1".getBytes(), values.get(2).values.get(0).getLeft());
      assertArrayEquals("basic_test_value_1".getBytes(), values.get(2).values.get(0).getRight());

      assertTrue(values.get(3).isAllFetched());
      assertArrayEquals(keys.get(3).getLeft(), values.get(3).hashKey);
      assertEquals(0, values.get(3).values.size());

      // batch multi del
      List<Pair<byte[], List<byte[]>>> delKeys = new ArrayList<Pair<byte[], List<byte[]>>>();
      List<byte[]> delSortKeys = new ArrayList<byte[]>();
      delSortKeys.add("basic_test_sort_key_1".getBytes());
      delSortKeys.add("basic_test_sort_key_2".getBytes());
      delSortKeys.add("basic_test_sort_key_3".getBytes());
      delSortKeys.add("basic_test_sort_key_4".getBytes());
      delKeys.add(Pair.of("TestBasic_testBatchMultiSetGetDel_hash_key_1".getBytes(), delSortKeys));
      delKeys.add(Pair.of("TestBasic_testBatchMultiSetGetDel_hash_key_2".getBytes(), delSortKeys));
      delKeys.add(Pair.of("TestBasic_testBatchMultiSetGetDel_hash_key_3".getBytes(), delSortKeys));
      client.batchMultiDel(tableName, delKeys);

      // check deleted
      client.batchMultiGet(tableName, keys, values);
      assertEquals(4, values.size());
      assertEquals(0, values.get(0).values.size());
      assertEquals(0, values.get(1).values.size());
      assertEquals(0, values.get(2).values.size());
      assertEquals(0, values.get(3).values.size());
    } catch (PException e) {
      e.printStackTrace();
      assertTrue(false);
    }

    PegasusClientFactory.closeSingletonClient();
  }

  @Test
  public void testBatchMultiSetGetDel2() throws PException {
    PegasusClientInterface client = PegasusClientFactory.getSingletonClient();
    String tableName = "temp";

    try {
      // batch multi set
      List<HashKeyData> items = new ArrayList<HashKeyData>();
      items.add(new HashKeyData("TestBasic_testBatchMultiSetGetDel2_hash_key_1".getBytes()));
      items
          .get(items.size() - 1)
          .addData("basic_test_sort_key_1".getBytes(), "basic_test_value_1".getBytes());
      items
          .get(items.size() - 1)
          .addData("basic_test_sort_key_2".getBytes(), "basic_test_value_2".getBytes());
      items
          .get(items.size() - 1)
          .addData("basic_test_sort_key_3".getBytes(), "basic_test_value_3".getBytes());
      items.add(new HashKeyData("TestBasic_testBatchMultiSetGetDel2_hash_key_2".getBytes()));
      items
          .get(items.size() - 1)
          .addData("basic_test_sort_key_1".getBytes(), "basic_test_value_1".getBytes());
      items
          .get(items.size() - 1)
          .addData("basic_test_sort_key_2".getBytes(), "basic_test_value_2".getBytes());
      items.add(new HashKeyData("TestBasic_testBatchMultiSetGetDel2_hash_key_3".getBytes()));
      items
          .get(items.size() - 1)
          .addData("basic_test_sort_key_1".getBytes(), "basic_test_value_1".getBytes());
      List<PException> resultsBatchMultiSet = new ArrayList<PException>();
      int count = client.batchMultiSet2(tableName, items, resultsBatchMultiSet);
      assertEquals(3, count);
      assertEquals(3, resultsBatchMultiSet.size());
      assertNull(resultsBatchMultiSet.get(0));
      assertNull(resultsBatchMultiSet.get(1));
      assertNull(resultsBatchMultiSet.get(2));

      // batch multi get
      List<Pair<byte[], List<byte[]>>> keys = new ArrayList<Pair<byte[], List<byte[]>>>();
      List<byte[]> nullSortKeys = null;
      keys.add(Pair.of("TestBasic_testBatchMultiSetGetDel2_hash_key_1".getBytes(), nullSortKeys));
      keys.add(Pair.of("TestBasic_testBatchMultiSetGetDel2_hash_key_2".getBytes(), nullSortKeys));
      keys.add(Pair.of("TestBasic_testBatchMultiSetGetDel2_hash_key_3".getBytes(), nullSortKeys));
      keys.add(Pair.of("TestBasic_testBatchMultiSetGetDel2_hash_key_4".getBytes(), nullSortKeys));
      List<Pair<PException, HashKeyData>> resultsBatchMultiGet =
          new ArrayList<Pair<PException, HashKeyData>>();
      count = client.batchMultiGet2(tableName, keys, resultsBatchMultiGet);
      assertEquals(4, count);
      assertEquals(4, resultsBatchMultiGet.size());

      assertNull(resultsBatchMultiGet.get(0).getLeft());
      assertArrayEquals(keys.get(0).getLeft(), resultsBatchMultiGet.get(0).getRight().hashKey);
      assertEquals(3, resultsBatchMultiGet.get(0).getRight().values.size());
      assertArrayEquals(
          "basic_test_sort_key_1".getBytes(),
          resultsBatchMultiGet.get(0).getRight().values.get(0).getLeft());
      assertArrayEquals(
          "basic_test_value_1".getBytes(),
          resultsBatchMultiGet.get(0).getRight().values.get(0).getRight());
      assertArrayEquals(
          "basic_test_sort_key_2".getBytes(),
          resultsBatchMultiGet.get(0).getRight().values.get(1).getLeft());
      assertArrayEquals(
          "basic_test_value_2".getBytes(),
          resultsBatchMultiGet.get(0).getRight().values.get(1).getRight());
      assertArrayEquals(
          "basic_test_sort_key_3".getBytes(),
          resultsBatchMultiGet.get(0).getRight().values.get(2).getLeft());
      assertArrayEquals(
          "basic_test_value_3".getBytes(),
          resultsBatchMultiGet.get(0).getRight().values.get(2).getRight());

      assertNull(resultsBatchMultiGet.get(1).getLeft());
      assertArrayEquals(keys.get(1).getLeft(), resultsBatchMultiGet.get(1).getRight().hashKey);
      assertEquals(2, resultsBatchMultiGet.get(1).getRight().values.size());
      assertArrayEquals(
          "basic_test_sort_key_1".getBytes(),
          resultsBatchMultiGet.get(1).getRight().values.get(0).getLeft());
      assertArrayEquals(
          "basic_test_value_1".getBytes(),
          resultsBatchMultiGet.get(1).getRight().values.get(0).getRight());
      assertArrayEquals(
          "basic_test_sort_key_2".getBytes(),
          resultsBatchMultiGet.get(1).getRight().values.get(1).getLeft());
      assertArrayEquals(
          "basic_test_value_2".getBytes(),
          resultsBatchMultiGet.get(1).getRight().values.get(1).getRight());

      assertNull(resultsBatchMultiGet.get(2).getLeft());
      assertArrayEquals(keys.get(2).getLeft(), resultsBatchMultiGet.get(2).getRight().hashKey);
      assertEquals(1, resultsBatchMultiGet.get(2).getRight().values.size());
      assertArrayEquals(
          "basic_test_sort_key_1".getBytes(),
          resultsBatchMultiGet.get(2).getRight().values.get(0).getLeft());
      assertArrayEquals(
          "basic_test_value_1".getBytes(),
          resultsBatchMultiGet.get(2).getRight().values.get(0).getRight());

      assertNull(resultsBatchMultiGet.get(3).getLeft());
      assertArrayEquals(keys.get(3).getLeft(), resultsBatchMultiGet.get(3).getRight().hashKey);
      assertEquals(0, resultsBatchMultiGet.get(3).getRight().values.size());

      // batch multi del
      List<Pair<byte[], List<byte[]>>> delKeys = new ArrayList<Pair<byte[], List<byte[]>>>();
      List<byte[]> delSortKeys = new ArrayList<byte[]>();
      delSortKeys.add("basic_test_sort_key_1".getBytes());
      delSortKeys.add("basic_test_sort_key_2".getBytes());
      delSortKeys.add("basic_test_sort_key_3".getBytes());
      delSortKeys.add("basic_test_sort_key_4".getBytes());
      delKeys.add(Pair.of("TestBasic_testBatchMultiSetGetDel2_hash_key_1".getBytes(), delSortKeys));
      delKeys.add(Pair.of("TestBasic_testBatchMultiSetGetDel2_hash_key_2".getBytes(), delSortKeys));
      delKeys.add(Pair.of("TestBasic_testBatchMultiSetGetDel2_hash_key_3".getBytes(), delSortKeys));
      List<PException> resultsBatchMultiDel = new ArrayList<PException>();
      count = client.batchMultiDel2(tableName, delKeys, resultsBatchMultiDel);
      assertEquals(3, count);
      assertEquals(3, resultsBatchMultiSet.size());
      assertNull(resultsBatchMultiSet.get(0));
      assertNull(resultsBatchMultiSet.get(1));
      assertNull(resultsBatchMultiSet.get(2));

      // check deleted
      List<HashKeyData> values = new ArrayList<HashKeyData>();
      client.batchMultiGet(tableName, keys, values);
      assertEquals(4, values.size());
      assertEquals(0, values.get(0).values.size());
      assertEquals(0, values.get(1).values.size());
      assertEquals(0, values.get(2).values.size());
      assertEquals(0, values.get(3).values.size());
    } catch (PException e) {
      e.printStackTrace();
      assertTrue(false);
    }

    PegasusClientFactory.closeSingletonClient();
  }

  @Test
  public void asyncApiTest() throws PException {
    PegasusClientInterface client = PegasusClientFactory.getSingletonClient();
    PegasusTableInterface tb = client.openTable("temp");

    String asyncHashPrefix = "AsyncApiTestHash";
    String asyncSortPrefix = "AsyncApiTestSort";
    String asyncValuePrefix = "AsyncApiTestValue";
    String key = asyncHashPrefix + "_0";

    // Exist
    System.out.println("Test exist");
    try {
      assertFalse(tb.asyncExist(key.getBytes(), key.getBytes(), 0).await().getNow());
      assertFalse(tb.asyncExist(null, null, 0).await().getNow());
      assertFalse(tb.asyncExist(null, key.getBytes(), 0).await().getNow());
      assertFalse(tb.asyncExist(key.getBytes(), null, 0).await().getNow());
    } catch (Throwable e) {
      e.printStackTrace();
      fail();
    }

    try {
      assertNull(tb.asyncSet(key.getBytes(), key.getBytes(), key.getBytes(), 0).await().getNow());
      assertTrue(tb.asyncExist(key.getBytes(), key.getBytes(), 0).await().getNow());
    } catch (Throwable e) {
      e.printStackTrace();
      fail();
    }

    // SortKeyCount
    System.out.println("Test sortkeycount");
    try {
      Long ans = tb.asyncSortKeyCount(key.getBytes(), 0).await().getNow();
      assertEquals(1, (long) ans);

      assertNull(tb.asyncDel(key.getBytes(), key.getBytes(), 0).await().getNow());
      ans = tb.asyncSortKeyCount(key.getBytes(), 0).await().getNow();
      assertEquals(0, (long) ans);

      Future<Long> future = tb.asyncSortKeyCount(null, 0).await();
      assertFalse(future.isSuccess());
      assertTrue(future.cause() instanceof PException);
    } catch (Throwable e) {
      e.printStackTrace();
      fail();
    }

    // Get
    System.out.println("Test get");
    try {
      assertNull(tb.asyncGet(null, null, 0).await().getNow());
      assertNull(tb.asyncGet(null, key.getBytes(), 0).await().getNow());
      assertNull(tb.asyncGet(key.getBytes(), null, 0).await().getNow());
      assertNull(tb.asyncGet(key.getBytes(), key.getBytes(), 0).await().getNow());

      assertNull(tb.asyncSet(key.getBytes(), key.getBytes(), key.getBytes(), 0).await().getNow());
      assertArrayEquals(
          key.getBytes(), tb.asyncGet(key.getBytes(), key.getBytes(), 0).await().getNow());

      assertNull(tb.asyncDel(key.getBytes(), key.getBytes(), 0).await().getNow());
    } catch (Throwable e) {
      e.printStackTrace();
      fail();
    }

    // Set & ttl
    System.out.println("Test set & ttl");
    try {
      assertNull(
          tb.asyncSet(key.getBytes(), key.getBytes(), key.getBytes(), 5, 1).await().getNow());
      assertArrayEquals(
          key.getBytes(), tb.asyncGet(key.getBytes(), key.getBytes(), 0).await().getNow());

      Integer ttlSeconds = tb.asyncTTL(key.getBytes(), key.getBytes(), 0).await().getNow();
      assertEquals(5, (int) ttlSeconds);

      Thread.sleep(6000);
      assertFalse(tb.asyncExist(key.getBytes(), key.getBytes(), 0).await().getNow());
    } catch (Throwable e) {
      e.printStackTrace();
      fail();
    }

    // multiGet exeception handle
    System.out.println("Test multiget exception handle");
    try {
      Future<PegasusTableInterface.MultiGetResult> f = tb.asyncMultiGet(null, null, 0).await();
      assertFalse(f.isSuccess());
      assertTrue(f.cause() instanceof PException);

      f = tb.asyncMultiGet("".getBytes(), null, 0).await();
      assertFalse(f.isSuccess());
      assertTrue(f.cause() instanceof PException);
    } catch (Throwable e) {
      e.printStackTrace();
      fail();
    }

    // multiset exception handle
    System.out.println("Test multiset exception handle");
    try {
      Future<Void> f = tb.asyncMultiSet(null, null, 0).await();
      assertFalse(f.isSuccess());
      assertTrue(f.cause() instanceof PException);

      f = tb.asyncMultiSet("hehe".getBytes(), null, 0).await();
      assertFalse(f.isSuccess());
      assertTrue(f.cause() instanceof PException);

      f = tb.asyncMultiSet("hehe".getBytes(), new ArrayList<Pair<byte[], byte[]>>(), 0).await();
      assertFalse(f.isSuccess());
      assertTrue(f.cause() instanceof PException);
    } catch (Throwable e) {
      e.printStackTrace();
      fail();
    }

    // multiDel exception handle
    System.out.println("Test multidel exception handle");
    try {
      Future<Void> f = tb.asyncMultiDel(null, null, 0).await();
      assertFalse(f.isSuccess());
      assertTrue(f.cause() instanceof PException);

      f = tb.asyncMultiDel("hehe".getBytes(), null, 0).await();
      assertFalse(f.isSuccess());
      assertTrue(f.cause() instanceof PException);

      f = tb.asyncMultiDel("hehe".getBytes(), new ArrayList<byte[]>(), 0).await();
      assertFalse(f.isSuccess());
      assertTrue(f.cause() instanceof PException);
    } catch (Throwable e) {
      e.printStackTrace();
      fail();
    }

    // Set/get pipeline
    System.out.println("Test set/get pipeline");
    try {
      ArrayList<Future<Void>> fl = new ArrayList<Future<Void>>();

      int totalNumber = 100;
      for (int i = 0; i < totalNumber; ++i) {
        String formatted = String.format("%03d", i);
        String sortKey = asyncSortPrefix + "_" + formatted;
        String value = asyncValuePrefix + "_" + formatted;
        Future<Void> f =
            tb.asyncSet(asyncHashPrefix.getBytes(), sortKey.getBytes(), value.getBytes(), 0, 0);
        fl.add(f);
      }

      for (int i = 0; i < totalNumber; ++i) {
        fl.get(i).await();
        assertTrue(fl.get(i).isSuccess());
      }

      Long sortCount = tb.asyncSortKeyCount(asyncHashPrefix.getBytes(), 0).await().getNow();
      assertEquals(totalNumber, (long) sortCount);

      PegasusTableInterface.MultiGetSortKeysResult result =
          tb.asyncMultiGetSortKeys(asyncHashPrefix.getBytes(), 150, 1000000, 0).await().getNow();
      assertEquals(totalNumber, result.keys.size());

      ArrayList<Future<byte[]>> fl2 = new ArrayList<Future<byte[]>>();
      for (byte[] sortKey : result.keys) {
        Future<byte[]> f = tb.asyncGet(asyncHashPrefix.getBytes(), sortKey, 0);
        fl2.add(f);
      }

      for (int i = 0; i < totalNumber; ++i) {
        byte[] value = fl2.get(i).await().getNow();
        String formatted = String.format("%03d", i);
        String expectValue = asyncValuePrefix + "_" + formatted;
        assertArrayEquals(expectValue.getBytes(), value);
      }

      fl.clear();
      for (byte[] sortKey : result.keys) {
        Future<Void> f = tb.asyncDel(asyncHashPrefix.getBytes(), sortKey, 0);
        fl.add(f);
      }

      for (Future<Void> f : fl) {
        f.await();
        assertTrue(f.isSuccess());
      }

      Long sortKeyCount = tb.sortKeyCount(asyncHashPrefix.getBytes(), 0);
      assertEquals(0, (long) sortKeyCount);

    } catch (Throwable e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void asyncApiSample() throws PException {
    PegasusClientInterface client = PegasusClientFactory.getSingletonClient();
    PegasusTableInterface table = client.openTable("temp");

    String key = "hello";
    String value = "world";

    Future<Void> f = table.asyncSet(null, null, null, 0);
    f.addListener(
        new PegasusTableInterface.SetListener() {
          @Override
          public void operationComplete(Future<Void> future) throws Exception {
            if (future.isSuccess()) {
              System.out.println("set succeed");
              fail();
            } else {
              assert future.cause() instanceof PException;
            }
          }
        });
    f.awaitUninterruptibly();
  }

  @Test
  public void scanWithFilter() throws PException {
    PegasusClientInterface client = PegasusClientFactory.getSingletonClient();
    PegasusTableInterface table = client.openTable("temp");
    byte[] hashKey = "x".getBytes();
    List<Pair<byte[], byte[]>> values = new ArrayList<Pair<byte[], byte[]>>();
    values.add(Pair.of("m_1".getBytes(), "a".getBytes()));
    values.add(Pair.of("m_2".getBytes(), "a".getBytes()));
    values.add(Pair.of("m_3".getBytes(), "a".getBytes()));
    values.add(Pair.of("m_4".getBytes(), "a".getBytes()));
    values.add(Pair.of("m_5".getBytes(), "a".getBytes()));
    values.add(Pair.of("n_1".getBytes(), "b".getBytes()));
    values.add(Pair.of("n_2".getBytes(), "b".getBytes()));
    values.add(Pair.of("n_3".getBytes(), "b".getBytes()));

    try {
      // multi set
      table.multiSet(hashKey, values, 0);
    } catch (PException e) {
      e.printStackTrace();
      assertTrue(false);
    }

    try {
      // scan with batch_size = 10
      ScanOptions options = new ScanOptions();
      options.sortKeyFilterType = FilterType.FT_MATCH_PREFIX;
      options.sortKeyFilterPattern = "m".getBytes();
      options.batchSize = 10;
      Map<String, String> data = new HashMap<String, String>();
      PegasusScannerInterface scanner = table.getScanner(hashKey, null, null, options);
      assertNotNull(scanner);
      Pair<Pair<byte[], byte[]>, byte[]> item;
      while ((item = scanner.next()) != null) {
        assertArrayEquals(hashKey, item.getLeft().getLeft());
        assertArrayEquals("a".getBytes(), item.getRight());
        data.put(new String(item.getLeft().getRight()), new String(item.getRight()));
      }
      assertEquals(5, data.size());
      assertTrue(data.containsKey("m_1"));
      assertTrue(data.containsKey("m_2"));
      assertTrue(data.containsKey("m_3"));
      assertTrue(data.containsKey("m_4"));
      assertTrue(data.containsKey("m_5"));
    } catch (PException e) {
      e.printStackTrace();
      assertTrue(false);
    }

    try {
      // scan with batch_size = 3
      ScanOptions options = new ScanOptions();
      options.sortKeyFilterType = FilterType.FT_MATCH_PREFIX;
      options.sortKeyFilterPattern = "m".getBytes();
      options.batchSize = 3;
      Map<String, String> data = new HashMap<String, String>();
      PegasusScannerInterface scanner = table.getScanner(hashKey, null, null, options);
      assertNotNull(scanner);
      Pair<Pair<byte[], byte[]>, byte[]> item;
      while ((item = scanner.next()) != null) {
        assertArrayEquals(hashKey, item.getLeft().getLeft());
        assertArrayEquals("a".getBytes(), item.getRight());
        data.put(new String(item.getLeft().getRight()), new String(item.getRight()));
      }
      assertEquals(5, data.size());
      assertTrue(data.containsKey("m_1"));
      assertTrue(data.containsKey("m_2"));
      assertTrue(data.containsKey("m_3"));
      assertTrue(data.containsKey("m_4"));
      assertTrue(data.containsKey("m_5"));
    } catch (PException e) {
      e.printStackTrace();
      assertTrue(false);
    }

    try {
      // multi del
      List<byte[]> sortKeys = new ArrayList<byte[]>();
      for (int i = 0; i < values.size(); i++) {
        sortKeys.add(values.get(i).getKey());
      }
      table.multiDel(hashKey, sortKeys, 0);
    } catch (PException e) {
      e.printStackTrace();
      assertTrue(false);
    }

    PegasusClientFactory.closeSingletonClient();
  }

  @Test
  public void fullScanWithFilter() throws PException {
    PegasusClientInterface client = PegasusClientFactory.getSingletonClient();
    PegasusTableInterface table = client.openTable("temp");
    byte[] hashKey = "x".getBytes();
    List<Pair<byte[], byte[]>> values = new ArrayList<Pair<byte[], byte[]>>();
    values.add(Pair.of("m_1".getBytes(), "a".getBytes()));
    values.add(Pair.of("m_2".getBytes(), "a".getBytes()));
    values.add(Pair.of("m_3".getBytes(), "a".getBytes()));
    values.add(Pair.of("m_4".getBytes(), "a".getBytes()));
    values.add(Pair.of("m_5".getBytes(), "a".getBytes()));
    values.add(Pair.of("n_1".getBytes(), "b".getBytes()));
    values.add(Pair.of("n_2".getBytes(), "b".getBytes()));
    values.add(Pair.of("n_3".getBytes(), "b".getBytes()));

    try {
      // multi set
      table.multiSet(hashKey, values, 0);
    } catch (PException e) {
      e.printStackTrace();
      assertTrue(false);
    }

    try {
      // scan with batch_size = 10
      ScanOptions options = new ScanOptions();
      options.sortKeyFilterType = FilterType.FT_MATCH_PREFIX;
      options.sortKeyFilterPattern = "m".getBytes();
      options.batchSize = 10;
      Map<String, String> data = new HashMap<String, String>();
      List<PegasusScannerInterface> scanners = table.getUnorderedScanners(1, options);
      assertEquals(1, scanners.size());
      PegasusScannerInterface scanner = scanners.get(0);
      Pair<Pair<byte[], byte[]>, byte[]> item;
      while ((item = scanner.next()) != null) {
        assertArrayEquals(hashKey, item.getLeft().getLeft());
        assertArrayEquals("a".getBytes(), item.getRight());
        data.put(new String(item.getLeft().getRight()), new String(item.getRight()));
      }
      assertEquals(5, data.size());
      assertTrue(data.containsKey("m_1"));
      assertTrue(data.containsKey("m_2"));
      assertTrue(data.containsKey("m_3"));
      assertTrue(data.containsKey("m_4"));
      assertTrue(data.containsKey("m_5"));
    } catch (PException e) {
      e.printStackTrace();
      assertTrue(false);
    }

    try {
      // scan with batch_size = 3
      ScanOptions options = new ScanOptions();
      options.sortKeyFilterType = FilterType.FT_MATCH_PREFIX;
      options.sortKeyFilterPattern = "m".getBytes();
      options.batchSize = 3;
      Map<String, String> data = new HashMap<String, String>();
      List<PegasusScannerInterface> scanners = table.getUnorderedScanners(1, options);
      assertEquals(1, scanners.size());
      PegasusScannerInterface scanner = scanners.get(0);
      Pair<Pair<byte[], byte[]>, byte[]> item;
      while ((item = scanner.next()) != null) {
        assertArrayEquals(hashKey, item.getLeft().getLeft());
        assertArrayEquals("a".getBytes(), item.getRight());
        data.put(new String(item.getLeft().getRight()), new String(item.getRight()));
      }
      assertEquals(5, data.size());
      assertTrue(data.containsKey("m_1"));
      assertTrue(data.containsKey("m_2"));
      assertTrue(data.containsKey("m_3"));
      assertTrue(data.containsKey("m_4"));
      assertTrue(data.containsKey("m_5"));
    } catch (PException e) {
      e.printStackTrace();
      assertTrue(false);
    }

    try {
      // multi del
      List<byte[]> sortKeys = new ArrayList<byte[]>();
      for (int i = 0; i < values.size(); i++) {
        sortKeys.add(values.get(i).getKey());
      }
      table.multiDel(hashKey, sortKeys, 0);
    } catch (PException e) {
      e.printStackTrace();
      assertTrue(false);
    }

    PegasusClientFactory.closeSingletonClient();
  }

  @Test
  public void createClient() throws PException {
    System.out.println("test createClient with clientOptions");
    ClientOptions clientOptions = ClientOptions.create();
    byte[] value = null;

    // test createClient(clientOptions)
    PegasusClientInterface client = null;
    try {
      client = PegasusClientFactory.createClient(clientOptions);
      client.set(
          "temp",
          "createClient".getBytes(),
          "createClient_0".getBytes(),
          "createClient_0".getBytes());
      value = client.get("temp", "createClient".getBytes(), "createClient_0".getBytes());
    } catch (Exception e) {
      e.printStackTrace();
      assertTrue(false);
    }

    assertTrue(new String(value).equals("createClient_0"));

    // test getSingletonClient(ClientOptions options)
    PegasusClientInterface singletonClient = null;
    try {
      singletonClient = PegasusClientFactory.getSingletonClient(clientOptions);
      singletonClient.set(
          "temp",
          "getSingletonClient".getBytes(),
          "createClient_1".getBytes(),
          "createClient_1".getBytes());
      value =
          singletonClient.get("temp", "getSingletonClient".getBytes(), "createClient_1".getBytes());
    } catch (Exception e) {
      e.printStackTrace();
      assertTrue(false);
    }

    assertTrue(new String(value).equals("createClient_1"));

    // test getSingletonClient(ClientOptions options) --> same clientOptions
    PegasusClientInterface singletonClient1 = null;
    try {
      singletonClient1 = PegasusClientFactory.getSingletonClient(clientOptions);
      singletonClient1.set(
          "temp",
          "getSingletonClient".getBytes(),
          "createClient_2".getBytes(),
          "createClient_2".getBytes());
      value =
          singletonClient1.get(
              "temp", "getSingletonClient".getBytes(), "createClient_2".getBytes());
    } catch (Exception e) {
      e.printStackTrace();
      assertTrue(false);
    }

    assertTrue(new String(value).equals("createClient_2"));
    assertTrue(singletonClient1 == singletonClient);

    // test getSingletonClient(ClientOptions options) --> different clientOptions,but values of
    // clientOptions is same
    ClientOptions clientOptions1 = ClientOptions.create();
    try {
      singletonClient1 = PegasusClientFactory.getSingletonClient(clientOptions1);
      singletonClient1.set(
          "temp",
          "getSingletonClient".getBytes(),
          "createClient_3".getBytes(),
          "createClient_3".getBytes());
      value =
          singletonClient1.get(
              "temp", "getSingletonClient".getBytes(), "createClient_3".getBytes());
    } catch (Exception e) {
      e.printStackTrace();
      assertTrue(false);
    }

    assertTrue(new String(value).equals("createClient_3"));
    assertTrue(singletonClient1 == singletonClient);

    // test getSingletonClient(ClientOptions options) --> different clientOptions,and values of
    // clientOptions is different
    ClientOptions clientOptions2 =
        ClientOptions.builder()
            .metaServers("127.0.0.1:34601,127.0.0.1:34602,127.0.0.1:34603")
            .asyncWorkers(5) // default value is 4,this set different value
            .build();
    try {
      singletonClient1 = PegasusClientFactory.getSingletonClient(clientOptions2);
      singletonClient1.set(
          "temp",
          "getSingletonClient".getBytes(),
          "createClient_4".getBytes(),
          "createClient_4".getBytes());
      value =
          singletonClient1.get(
              "temp", "getSingletonClient".getBytes(), "createClient_4".getBytes());
    } catch (Exception e) {
      // if values of clientOptions is different,the code's right logic is "throw exception"
      assertTrue(true);
    }

    PegasusClientFactory.closeSingletonClient();
  }

  @Test
  public void delRange() throws PException {
    PegasusClientInterface client = PegasusClientFactory.getSingletonClient();
    DelRangeOptions delRangeOptions = new DelRangeOptions();

    String tableName = "temp";

    // multi set values
    List<Pair<byte[], byte[]>> values = new ArrayList<Pair<byte[], byte[]>>();
    int count = 0;

    while (count < 150) {
      values.add(Pair.of(("k_" + count).getBytes(), ("v_" + count).getBytes()));
      count++;
    }
    List<byte[]> remainingSortKey = new ArrayList<byte[]>();
    List<Pair<byte[], byte[]>> remainingValue = new ArrayList<Pair<byte[], byte[]>>();

    assertNull(
        assertDoesNotThrow(
            () -> {
              client.multiSet(tableName, "delRange".getBytes(), values);
              client.delRange(
                  tableName,
                  "delRange".getBytes(),
                  "k_0".getBytes(),
                  "k_90".getBytes(),
                  delRangeOptions);

              remainingSortKey.add("k_90".getBytes());
              remainingSortKey.add("k_91".getBytes());
              remainingSortKey.add("k_92".getBytes());
              remainingSortKey.add("k_93".getBytes());
              remainingSortKey.add("k_94".getBytes());
              remainingSortKey.add("k_95".getBytes());
              remainingSortKey.add("k_96".getBytes());
              remainingSortKey.add("k_97".getBytes());
              remainingSortKey.add("k_98".getBytes());
              remainingSortKey.add("k_99".getBytes());
              client.multiGet(tableName, "delRange".getBytes(), remainingSortKey, remainingValue);

              return delRangeOptions.nextSortKey;
            }));

    List<String> valueStr = new ArrayList<String>();
    for (Pair<byte[], byte[]> pair : remainingValue) {
      valueStr.add(new String(pair.getValue()));
    }
    assertEquals(10, valueStr.size());
    assertTrue(valueStr.contains("v_90"));
    assertTrue(valueStr.contains("v_91"));
    assertTrue(valueStr.contains("v_92"));
    assertTrue(valueStr.contains("v_93"));
    assertTrue(valueStr.contains("v_94"));
    assertTrue(valueStr.contains("v_95"));
    assertTrue(valueStr.contains("v_96"));
    assertTrue(valueStr.contains("v_97"));
    assertTrue(valueStr.contains("v_98"));
    assertTrue(valueStr.contains("v_99"));
    remainingValue.clear();
    valueStr.clear();

    // delRange with FT_MATCH_POSTFIX option
    delRangeOptions.sortKeyFilterType = FilterType.FT_MATCH_POSTFIX;
    delRangeOptions.sortKeyFilterPattern = "k_93".getBytes();

    assertDoesNotThrow(
        () -> {
          client.delRange(
              tableName,
              "delRange".getBytes(),
              "k_90".getBytes(),
              "k_95".getBytes(),
              delRangeOptions);
          client.multiGet(tableName, "delRange".getBytes(), remainingSortKey, remainingValue);
        });
    for (Pair<byte[], byte[]> pair : remainingValue) {
      valueStr.add(new String(pair.getValue()));
    }
    assertEquals(9, valueStr.size());
    assertTrue(!valueStr.contains("v_93"));
    remainingValue.clear();
    valueStr.clear();

    // delRange with "*Inclusive" option
    delRangeOptions.startInclusive = false;
    delRangeOptions.stopInclusive = true;
    delRangeOptions.sortKeyFilterType = FilterType.FT_NO_FILTER;
    delRangeOptions.sortKeyFilterPattern = null;
    assertDoesNotThrow(
        () -> {
          client.delRange(
              tableName,
              "delRange".getBytes(),
              "k_90".getBytes(),
              "k_95".getBytes(),
              delRangeOptions);
          client.multiGet(tableName, "delRange".getBytes(), remainingSortKey, remainingValue);
        });

    for (Pair<byte[], byte[]> pair : remainingValue) {
      valueStr.add(new String(pair.getValue()));
    }

    assertEquals(5, valueStr.size());
    assertTrue(valueStr.contains("v_90"));
    assertTrue(valueStr.contains("v_96"));
    assertTrue(valueStr.contains("v_97"));
    assertTrue(valueStr.contains("v_98"));
    assertTrue(valueStr.contains("v_99"));
    remainingValue.clear();
    valueStr.clear();

    DelRangeOptions delRangeOptions2 = new DelRangeOptions();
    // test hashKey can't be null or ""
    assertEquals(
        "{version}: Invalid parameter: hash key can't be empty",
        assertThrows(
                PException.class,
                () -> {
                  client.delRange(
                      tableName, null, "k1".getBytes(), "k2".getBytes(), delRangeOptions2);
                })
            .getMessage());

    assertEquals(
        "{version}: Invalid parameter: hash key can't be empty",
        assertThrows(
                PException.class,
                () -> {
                  client.delRange(
                      tableName, "".getBytes(), "k1".getBytes(), "k2".getBytes(), delRangeOptions2);
                })
            .getMessage());

    // test sortKey can be null, means delete from first to last
    assertNull(
        assertDoesNotThrow(
            () -> {
              client.multiSet(tableName, "delRange".getBytes(), values);
              client.delRange(tableName, "delRange".getBytes(), null, null, delRangeOptions2);
              client.multiGet(tableName, "delRange".getBytes(), remainingSortKey, remainingValue);
              return delRangeOptions2.nextSortKey;
            }));

    assertEquals(remainingValue.size(), 0);
  }

  @Test
  public void testWriteSizeLimit() throws PException {
    // Test config from pegasus.properties
    PegasusClientInterface client1 = PegasusClientFactory.getSingletonClient();
    testWriteSizeLimit(client1);
    // Test config from ClientOptions
    ClientOptions clientOptions = ClientOptions.create();
    PegasusClientInterface client2 = PegasusClientFactory.createClient(clientOptions);
    testWriteSizeLimit(client2);
  }

  private void testWriteSizeLimit(PegasusClientInterface client) {
    assertNotNull(client);
    String tableName = "temp";
    // test hashKey size > 1024
    String hashKeyExceed = RandomStringUtils.random(1025, true, true);
    String sortKey = "limitSortKey";
    String value = "limitValueSize";
    try {
      client.set(tableName, hashKeyExceed.getBytes(), sortKey.getBytes(), value.getBytes());
    } catch (PException e) {
      assertTrue(
          e.getMessage()
              .contains("Exceed the hashKey length threshold = 1024,hashKeyLength = 1025"));
    }

    // test sortKey size > 1024
    String hashKey = "limitHashKey";
    String sortKeyExceed = RandomStringUtils.random(1025, true, true);
    try {
      client.set(tableName, hashKey.getBytes(), sortKeyExceed.getBytes(), value.getBytes());
    } catch (PException e) {
      assertTrue(
          e.getMessage()
              .contains("Exceed the sort key length threshold = 1024,sortKeyLength = 1025"));
    }

    // test singleValue size > 400 * 1024
    String valueExceed = RandomStringUtils.random(400 * 1024 + 1, true, true);
    try {
      client.set(tableName, hashKey.getBytes(), sortKey.getBytes(), valueExceed.getBytes());
    } catch (PException e) {
      assertTrue(
          e.getMessage()
              .contains("Exceed the value length threshold = 409600,valueLength = 409601"));
    }

    // test multi value count > 1000
    int count = 2000;
    List<Pair<byte[], byte[]>> multiValues = new ArrayList<Pair<byte[], byte[]>>();
    while (count-- > 0) {
      multiValues.add(Pair.of(sortKey.getBytes(), value.getBytes()));
    }
    try {
      client.multiSet(tableName, hashKey.getBytes(), multiValues);
    } catch (PException e) {
      assertTrue(
          e.getMessage().contains("Exceed the value count threshold = 1000,valueCount = 2000"));
    }

    // test multi value size > 1024 * 1024
    String multiValue2 = RandomStringUtils.random(5 * 1024, true, true);
    List<Pair<byte[], byte[]>> multiValues2 = new ArrayList<Pair<byte[], byte[]>>();
    int count2 = 500;
    while (count2-- > 0) {
      multiValues2.add(Pair.of(sortKey.getBytes(), multiValue2.getBytes()));
    }
    try {
      client.multiSet(tableName, hashKey.getBytes(), multiValues2);
    } catch (PException e) {
      assertTrue(e.getMessage().contains("Exceed the multi value length threshold = 1048576"));
    }

    // test mutations value count > 1000
    CheckAndMutateOptions options = new CheckAndMutateOptions();
    Mutations mutations = new Mutations();

    int count3 = 1500;
    while (count3-- > 0) {
      mutations.set(sortKey.getBytes(), value.getBytes(), 0);
    }

    try {
      PegasusTableInterface.CheckAndMutateResult result =
          client.checkAndMutate(
              tableName,
              hashKey.getBytes(),
              sortKey.getBytes(),
              CheckType.CT_VALUE_NOT_EXIST,
              null,
              mutations,
              options);
    } catch (PException e) {
      assertTrue(
          e.getMessage().contains("Exceed the value count threshold = 1000,valueCount = 1500"));
    }

    // test mutations value size > 1024 * 1024
    int count4 = 100;
    Mutations mutations2 = new Mutations();
    String mutationValue2 = RandomStringUtils.random(20 * 1024, true, true);
    while (count4-- > 0) {
      mutations2.set(sortKey.getBytes(), mutationValue2.getBytes(), 0);
    }

    try {
      PegasusTableInterface.CheckAndMutateResult result =
          client.checkAndMutate(
              tableName,
              hashKey.getBytes(),
              sortKey.getBytes(),
              CheckType.CT_VALUE_NOT_EXIST,
              null,
              mutations2,
              options);
    } catch (PException e) {
      assertTrue(e.getMessage().contains("Exceed the multi value length threshold = 1048576"));
    }
  }

  @Test // test for making sure return "maxFetchCount" if has "maxFetchCount" valid record
  public void testScanRangeWithValueExpired() throws PException, InterruptedException {
    String tableName = "temp";
    String hashKey = "hashKey_testScanRangeWithValueExpired";
    // generate records: sortKeys=[expired_0....expired_999,persistent_0...persistent_9]
    generateRecordsWithExpired(tableName, hashKey, 1000, 10);

    PegasusTable table =
        (PegasusTable) PegasusClientFactory.getSingletonClient().openTable(tableName);
    // case A: scan all record
    // case A1: scan all record: if persistent record count >= maxFetchCount, it must return
    // maxFetchCount records
    int maxFetchCount = 5;
    PegasusTable.ScanRangeResult caseA1 =
        table.scanRange(hashKey.getBytes(), null, null, new ScanOptions(), maxFetchCount, 0);
    assertScanResult(hashKey, 0, 4, false, caseA1);
    // case A2: scan all record: if persistent record count < maxFetchCount, it only return
    // persistent count records
    maxFetchCount = 100;
    PegasusTable.ScanRangeResult caseA2 =
        table.scanRange(hashKey.getBytes(), null, null, new ScanOptions(), maxFetchCount, 0);
    assertScanResult(hashKey, 0, 9, true, caseA2);

    // case B: scan limit record by "startSortKey" and "":
    // case B1: scan limit record by "expired_0" and "", if persistent record count >=
    // maxFetchCount, it must return maxFetchCount records
    maxFetchCount = 5;
    PegasusTable.ScanRangeResult caseB1 =
        table.scanRange(
            hashKey.getBytes(),
            "expired_0".getBytes(),
            "".getBytes(),
            new ScanOptions(),
            maxFetchCount,
            0);
    assertScanResult(hashKey, 0, 4, false, caseB1);
    // case B2: scan limit record by "expired_0" and "", if persistent record count < maxFetchCount,
    // it only return valid records
    maxFetchCount = 50;
    PegasusTable.ScanRangeResult caseB2 =
        table.scanRange(
            hashKey.getBytes(),
            "expired_0".getBytes(),
            "".getBytes(),
            new ScanOptions(),
            maxFetchCount,
            0);
    assertScanResult(hashKey, 0, 9, true, caseB2);
    // case B3: scan limit record by "persistent_5" and "", if following persistent record count <
    // maxFetchCount, it only return valid records
    maxFetchCount = 50;
    PegasusTable.ScanRangeResult caseB3 =
        table.scanRange(
            hashKey.getBytes(),
            "persistent_5".getBytes(),
            "".getBytes(),
            new ScanOptions(),
            maxFetchCount,
            0);
    assertScanResult(hashKey, 5, 9, true, caseB3);
    // case B4: scan limit record by "persistent_5" and "", if following persistent record count >
    // maxFetchCount, it only return valid records
    maxFetchCount = 3;
    PegasusTable.ScanRangeResult caseB4 =
        table.scanRange(
            hashKey.getBytes(),
            "persistent_5".getBytes(),
            "".getBytes(),
            new ScanOptions(),
            maxFetchCount,
            0);
    assertScanResult(hashKey, 5, 7, false, caseB4);

    // case C: scan limit record by "" and "stopSortKey":
    // case C1: scan limit record by "" and "expired_7", if will return 0 record
    maxFetchCount = 3;
    PegasusTable.ScanRangeResult caseC1 =
        table.scanRange(
            hashKey.getBytes(),
            "".getBytes(),
            "expired_7".getBytes(),
            new ScanOptions(),
            maxFetchCount,
            0);
    assertTrue(caseC1.allFetched);
    assertEquals(0, caseC1.results.size()); // among "" and "expired_7" has 0 valid record
    // case C2: scan limit record by "" and "persistent_7", if valid record count < maxFetchCount,
    // it only return valid record
    maxFetchCount = 10;
    PegasusTable.ScanRangeResult caseC2 =
        table.scanRange(
            hashKey.getBytes(),
            "".getBytes(),
            "persistent_7".getBytes(),
            new ScanOptions(),
            maxFetchCount,
            0);
    assertScanResult(hashKey, 0, 6, true, caseC2);
    // case C3: scan limit record by "" and "persistent_7", if valid record count > maxFetchCount,
    // it only return valid record
    maxFetchCount = 2;
    PegasusTable.ScanRangeResult caseC3 =
        table.scanRange(
            hashKey.getBytes(),
            "".getBytes(),
            "persistent_7".getBytes(),
            new ScanOptions(),
            maxFetchCount,
            0);
    assertScanResult(hashKey, 0, 1, false, caseC3);

    // case D: use multiGetSortKeys, which actually equal with case A but no value
    // case D1: maxFetchCount > 0, return maxFetchCount valid record
    maxFetchCount = -1;
    PegasusTableInterface.MultiGetSortKeysResult caseD1 =
        table.multiGetSortKeys(hashKey.getBytes(), 5, maxFetchCount, 0);
    assertFalse(caseD1.allFetched);
    assertEquals(5, caseD1.keys.size());
    for (int i = 0; i <= 4; i++) {
      assertEquals("persistent_" + i, new String(caseD1.keys.get(i)));
    }
    // case D1: maxFetchCount < 0, return all valid record
    maxFetchCount = -1;
    PegasusTableInterface.MultiGetSortKeysResult caseD2 =
        table.multiGetSortKeys(hashKey.getBytes(), 10, maxFetchCount, 0);
    assertTrue(caseD2.allFetched);
    assertEquals(10, caseD2.keys.size());
    for (int i = 0; i <= 9; i++) {
      assertEquals("persistent_" + i, new String(caseD2.keys.get(i)));
    }
  }

  private void generateRecordsWithExpired(
      String tableName, String hashKey, int expiredCount, int persistentCount)
      throws PException, InterruptedException {
    PegasusClientInterface client = PegasusClientFactory.getSingletonClient();
    // Specify the prefixes to make sure the expired records are stored in front of the persistent
    // ones.
    String expiredSortKeyPrefix = "expired_";
    String persistentSortKeyPrefix = "persistent_";
    while (expiredCount-- > 0) {
      client.set(
          tableName,
          hashKey.getBytes(),
          (expiredSortKeyPrefix + expiredCount).getBytes(),
          (expiredSortKeyPrefix + expiredCount + "_value").getBytes(),
          1);
    }
    // Sleep a while to make sure the records are expired.
    Thread.sleep(1000);
    while (persistentCount-- > 0) {
      client.set(
          tableName,
          hashKey.getBytes(),
          (persistentSortKeyPrefix + persistentCount).getBytes(),
          (persistentSortKeyPrefix + persistentCount + "_value").getBytes());
    }
    PegasusClientFactory.closeSingletonClient();
  }

  private void assertScanResult(
      String hashKey,
      int startIndex,
      int stopIndex,
      boolean expectAllFetched,
      PegasusTable.ScanRangeResult actuallyRes) {
    assertEquals(expectAllFetched, actuallyRes.allFetched);
    assertEquals(stopIndex - startIndex + 1, actuallyRes.results.size());
    for (int i = startIndex; i <= stopIndex; i++) {
      assertEquals(hashKey, new String(actuallyRes.results.get(i - startIndex).getLeft().getKey()));
      assertEquals(
          "persistent_" + i,
          new String(actuallyRes.results.get(i - startIndex).getLeft().getValue()));
      assertEquals(
          "persistent_" + i + "_value",
          new String(actuallyRes.results.get(i - startIndex).getRight()));
    }
  }

  @Test
  public void testOperationTimeout() throws PException {
    String tableName = "temp";

    // Create a PegasusClientInterface object 'client' which causes exceptions throw out.
    Duration caseTimeout = Duration.ofMillis(1);
    ClientOptions client_opt = ClientOptions.builder().operationTimeout(caseTimeout).build();
    PegasusClientInterface client = PegasusClientFactory.createClient(client_opt);

    // Create a PegasusTableInterface object 'tb' which doesn't cause exceptions throw out.
    PegasusClientInterface client2 = PegasusClientFactory.createClient(ClientOptions.create());
    PegasusTableInterface tb = client2.openTable(tableName);

    String hashKey = "TestHash_0";
    String sortKey = "TestSort_0";

    try {
      logger.info("Test multiSet PException request");
      {
        String value = RandomStringUtils.random(5, true, true);
        List<Pair<byte[], byte[]>> values = new ArrayList<Pair<byte[], byte[]>>();
        int count = 500;
        while (count-- > 0) {
          values.add(Pair.of(sortKey.getBytes(), value.getBytes()));
        }

        Throwable exception =
            assertThrows(
                PException.class,
                () -> {
                  client.multiSet(tableName, hashKey.getBytes(), values);
                });
        assertTrue(
            exception
                .getMessage()
                .contains(
                    "request=[hashKey[:32]=\"TestHash_0\",sortKey[:32]=\"\",sortKeyCount=500,valueLength=2500]"));
      }

      logger.info("Test checkAndMutate PException request");
      {
        Mutations mutations = new Mutations();
        mutations.set(sortKey.getBytes(), "2".getBytes());

        CheckAndMutateOptions options = new CheckAndMutateOptions();
        options.returnCheckValue = true;
        Throwable exception =
            assertThrows(
                PException.class,
                () -> {
                  client.checkAndMutate(
                      tableName,
                      hashKey.getBytes(),
                      "k5".getBytes(),
                      CheckType.CT_VALUE_INT_LESS,
                      "2".getBytes(),
                      mutations,
                      options);
                });
        assertTrue(
            exception
                .getMessage()
                .contains(
                    "request=[hashKey[:32]=\"TestHash_0\",sortKey[:32]=\"k5\",sortKeyCount=1,valueLength=1]"));
      }

      logger.info("Test multiDel PException request");
      {
        List<byte[]> sortKeys = new ArrayList<byte[]>();
        List<Pair<byte[], byte[]>> values = new ArrayList<Pair<byte[], byte[]>>();
        int count = 500;
        while (count-- > 0) {
          String tempSortKey = "basic_test_sort_key_0";
          sortKeys.add(tempSortKey.getBytes());
          values.add(Pair.of(tempSortKey.getBytes(), "basic_test_value_0".getBytes()));
        }

        // Expect multiSet with a higher timeout will not throw exception.
        assertDoesNotThrow(
            () -> {
              tb.multiSet(hashKey.getBytes(), values, 5000);
            });

        // Expect multiSet with a lower timeout will throw exception.
        Throwable exception =
            assertThrows(
                PException.class,
                () -> {
                  client.multiDel(tableName, hashKey.getBytes(), sortKeys);
                });
        assertTrue(
            exception
                .getMessage()
                .contains(
                    "request=[hashKey[:32]=\"TestHash_0\",sortKey[:32]=\"\",sortKeyCount=500,valueLength=-1]"));
      }
    } catch (Throwable e) {
      e.printStackTrace();
      fail();
    }
  }
}
