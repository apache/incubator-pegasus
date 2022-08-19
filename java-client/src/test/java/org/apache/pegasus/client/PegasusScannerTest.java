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

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.Future;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

public class PegasusScannerTest {
  static char[] CCH =
      "_0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ".toCharArray();
  static char[] buffer = new char[256];
  static Random random;

  private static PegasusClientInterface client;
  private static final String tableName = "temp";

  private static TreeMap<String, TreeMap<String, String>> base;
  private static String expectedHashKey;

  @BeforeClass
  public static void setup() throws PException {
    client = PegasusClientFactory.getSingletonClient();
    random = new Random();
    base = new TreeMap<>();
    expectedHashKey = randomString();

    for (int i = 0; i < buffer.length; i++) {
      buffer[i] = CCH[random.nextInt(CCH.length)];
    }

    clearDatabase();

    TreeMap<String, String> hashMap = new TreeMap<>();
    for (int i = 0; i < 1000 || hashMap.size() < 1000; i++) {
      String sortKey = randomString();
      String value = randomString();

      client.set(tableName, expectedHashKey.getBytes(), sortKey.getBytes(), value.getBytes(), 0);
      hashMap.put(sortKey, value);
    }
    base.put(expectedHashKey, hashMap);

    for (int i = 0; i < 1000 || base.size() < 1000; i++) {
      String hashKey = randomString();
      TreeMap<String, String> sortMap = base.computeIfAbsent(hashKey, k -> new TreeMap<>());
      for (int j = 0; j < 10 || sortMap.size() < 10; j++) {
        String sortKey = randomString();
        String value = randomString();
        client.set(tableName, hashKey.getBytes(), sortKey.getBytes(), value.getBytes(), 0);
        sortMap.put(sortKey, value);
      }
    }
  }

  @AfterClass
  public static void tearDownTestCase() throws PException {
    clearDatabase();
  }

  @Test
  public void testAllSortKey() throws PException {
    ScanOptions options = new ScanOptions();
    TreeMap<String, String> data = new TreeMap<>();
    PegasusScannerInterface scanner =
        client.getScanner(
            tableName, expectedHashKey.getBytes(), new byte[] {}, new byte[] {}, options);
    Assert.assertNotNull(scanner);
    Pair<Pair<byte[], byte[]>, byte[]> item;
    while ((item = scanner.next()) != null) {
      Assert.assertEquals(expectedHashKey, new String(item.getLeft().getLeft()));
      checkAndPutSortMap(
          data,
          expectedHashKey,
          new String(item.getLeft().getRight()),
          new String(item.getRight()));
    }
    scanner.close();
    compareSortMap(data, base.get(expectedHashKey), expectedHashKey);
  }

  @Test
  public void testHasNext() throws PException {
    ScanOptions options = new ScanOptions();
    TreeMap<String, String> data = new TreeMap<>();
    PegasusScannerInterface scanner =
        client.getScanner(
            tableName, expectedHashKey.getBytes(), new byte[] {}, new byte[] {}, options);
    Assert.assertNotNull(scanner);
    Pair<Pair<byte[], byte[]>, byte[]> item;
    int count = 0;
    while (scanner.hasNext()) {
      item = scanner.next();
      count++;
      Assert.assertEquals(expectedHashKey, new String(item.getLeft().getLeft()));
      checkAndPutSortMap(
          data,
          expectedHashKey,
          new String(item.getLeft().getRight()),
          new String(item.getRight()));
      if ((item = scanner.next()) != null) {
        count++;
      }
      Assert.assertEquals(expectedHashKey, new String(item.getLeft().getLeft()));
      checkAndPutSortMap(
          data,
          expectedHashKey,
          new String(item.getLeft().getRight()),
          new String(item.getRight()));
    }
    Assert.assertEquals(1000, count);
    scanner.close();
    compareSortMap(data, base.get(expectedHashKey), expectedHashKey);
  }

  @Test
  public void testInclusive() throws PException {
    Iterator<String> iterator = base.get(expectedHashKey).keySet().iterator();
    for (int i = random.nextInt(500); i >= 0; i--) iterator.next();
    String start = iterator.next();
    for (int i = random.nextInt(400) + 50; i >= 0; i--) iterator.next();
    String stop = iterator.next();

    ScanOptions options = new ScanOptions();
    options.startInclusive = true;
    options.stopInclusive = true;
    TreeMap<String, String> data = new TreeMap<String, String>();
    PegasusScannerInterface scanner =
        client.getScanner(
            tableName, expectedHashKey.getBytes(), start.getBytes(), stop.getBytes(), options);
    Assert.assertNotNull(scanner);
    Pair<Pair<byte[], byte[]>, byte[]> item;
    while ((item = scanner.next()) != null) {
      Assert.assertEquals(expectedHashKey, new String(item.getLeft().getLeft()));
      checkAndPutSortMap(
          data,
          expectedHashKey,
          new String(item.getLeft().getRight()),
          new String(item.getRight()));
    }
    scanner.close();
    compareSortMap(
        data, base.get(expectedHashKey).subMap(start, true, stop, true), expectedHashKey);
  }

  @Test
  public void testExclusive() throws PException {
    Iterator<String> iterator = base.get(expectedHashKey).keySet().iterator();
    for (int i = random.nextInt(500); i >= 0; i--) iterator.next();
    String start = iterator.next();
    for (int i = random.nextInt(400) + 50; i >= 0; i--) iterator.next();
    String stop = iterator.next();

    ScanOptions options = new ScanOptions();
    options.startInclusive = false;
    options.stopInclusive = false;
    TreeMap<String, String> data = new TreeMap<String, String>();
    PegasusScannerInterface scanner =
        client.getScanner(
            tableName, expectedHashKey.getBytes(), start.getBytes(), stop.getBytes(), options);
    Assert.assertNotNull(scanner);
    Pair<Pair<byte[], byte[]>, byte[]> item;
    while ((item = scanner.next()) != null) {
      Assert.assertEquals(expectedHashKey, new String(item.getLeft().getLeft()));
      checkAndPutSortMap(
          data,
          expectedHashKey,
          new String(item.getLeft().getRight()),
          new String(item.getRight()));
    }
    scanner.close();
    compareSortMap(
        data, base.get(expectedHashKey).subMap(start, false, stop, false), expectedHashKey);
  }

  @Test
  public void testOnePoint() throws PException {
    Iterator<String> iterator = base.get(expectedHashKey).keySet().iterator();
    for (int i = random.nextInt(800); i >= 0; i--) iterator.next();
    String start = iterator.next();

    ScanOptions options = new ScanOptions();
    options.startInclusive = true;
    options.stopInclusive = true;
    PegasusScannerInterface scanner =
        client.getScanner(
            tableName, expectedHashKey.getBytes(), start.getBytes(), start.getBytes(), options);
    Assert.assertNotNull(scanner);
    Pair<Pair<byte[], byte[]>, byte[]> item = scanner.next();
    Assert.assertEquals(start, new String(item.getLeft().getRight()));
    item = scanner.next();
    Assert.assertNull(item);
    scanner.close();
  }

  @Test
  public void testHalfInclusive() throws PException {
    Iterator<String> iterator = base.get(expectedHashKey).keySet().iterator();
    for (int i = random.nextInt(800); i >= 0; i--) iterator.next();
    String start = iterator.next();

    ScanOptions options = new ScanOptions();
    options.startInclusive = true;
    options.stopInclusive = false;
    PegasusScannerInterface scanner =
        client.getScanner(
            tableName, expectedHashKey.getBytes(), start.getBytes(), start.getBytes(), options);
    Assert.assertNotNull(scanner);
    Pair<Pair<byte[], byte[]>, byte[]> item = scanner.next();
    Assert.assertNull(item);
    scanner.close();
  }

  @Test
  public void testVoidSpan() throws PException {
    Iterator<String> iterator = base.get(expectedHashKey).keySet().iterator();
    for (int i = random.nextInt(500); i >= 0; i--) iterator.next();
    String start = iterator.next();
    for (int i = random.nextInt(400) + 50; i >= 0; i--) iterator.next();
    String stop = iterator.next();

    ScanOptions options = new ScanOptions();
    options.startInclusive = true;
    options.stopInclusive = true;
    PegasusScannerInterface scanner =
        client.getScanner(
            tableName, expectedHashKey.getBytes(), stop.getBytes(), start.getBytes(), options);
    Assert.assertNotNull(scanner);
    Pair<Pair<byte[], byte[]>, byte[]> item = scanner.next();
    Assert.assertNull(item);
    scanner.close();
  }

  @Test
  public void testOverallScan() throws PException {
    ScanOptions options = new ScanOptions();
    TreeMap<String, TreeMap<String, String>> data = new TreeMap<String, TreeMap<String, String>>();
    List<PegasusScannerInterface> scanners = client.getUnorderedScanners(tableName, 3, options);
    Assert.assertTrue(scanners.size() <= 3);

    for (int i = scanners.size() - 1; i >= 0; i--) {
      PegasusScannerInterface scanner = scanners.get(i);
      Assert.assertNotNull(scanner);
      Pair<Pair<byte[], byte[]>, byte[]> item;
      while ((item = scanner.next()) != null) {
        checkAndPut(
            data,
            new String(item.getLeft().getLeft()),
            new String(item.getLeft().getRight()),
            new String(item.getRight()));
      }
      scanner.close();
    }
    compare(data, base);
  }

  @Test
  public void testAsyncScan() throws PException {
    ScanOptions options = new ScanOptions();
    TreeMap<String, TreeMap<String, String>> data = new TreeMap<String, TreeMap<String, String>>();
    List<PegasusScannerInterface> scanners = client.getUnorderedScanners(tableName, 3, options);
    Assert.assertTrue(scanners.size() <= 3);

    for (int i = scanners.size() - 1; i >= 0; i--) {
      PegasusScannerInterface scanner = scanners.get(i);
      Assert.assertNotNull(scanner);
      Future<Pair<Pair<byte[], byte[]>, byte[]>> item;
      while (true) {
        item = scanner.asyncNext();
        try {
          Pair<Pair<byte[], byte[]>, byte[]> pair = item.get();
          if (pair == null) break;
          checkAndPut(
              data,
              new String(pair.getLeft().getLeft()),
              new String(pair.getLeft().getRight()),
              new String(pair.getRight()));
        } catch (Exception e) {
          e.printStackTrace();
          Assert.assertTrue(false);
        }
      }
    }
    compare(data, base);
  }

  @Test
  public void testConcurrentCallAsyncScan() throws PException {
    int[] batchSizes = {10, 100, 500, 1000};
    int totalKvSize = 10000; // we just use scan to acquire 10000 kv-pairs
    long start_time_ms = 0, end_time_ms = 0;
    Deque<Future<Pair<Pair<byte[], byte[]>, byte[]>>> futures =
        new LinkedList<Future<Pair<Pair<byte[], byte[]>, byte[]>>>();
    for (int idx = 0; idx < batchSizes.length; idx++) {
      int batchSize = batchSizes[idx];
      ScanOptions options = new ScanOptions();
      options.batchSize = batchSize;
      TreeMap<String, TreeMap<String, String>> data =
          new TreeMap<String, TreeMap<String, String>>();
      List<PegasusScannerInterface> scanners = client.getUnorderedScanners(tableName, 1, options);
      Assert.assertTrue(scanners.size() <= 1);
      System.out.println(
          "start to scan " + totalKvSize + " kv-pairs with scan batch size " + batchSize);
      start_time_ms = System.currentTimeMillis();
      futures.clear();
      for (int cnt = 0; cnt < totalKvSize; cnt++) {
        futures.add(scanners.get(0).asyncNext());
      }
      try {
        for (Future<Pair<Pair<byte[], byte[]>, byte[]>> fu : futures) {
          fu.get();
        }
      } catch (Exception e) {
        e.printStackTrace();
        Assert.assertTrue(false);
      }
      end_time_ms = System.currentTimeMillis();
      System.out.println("consuing time: " + (end_time_ms - start_time_ms) + "ms");
    }
  }

  @Test
  public void testHashKeyFilteringScan() throws PException {
    ScanOptions options = new ScanOptions();
    options.hashKeyFilterType = FilterType.FT_MATCH_PREFIX;
    options.hashKeyFilterPattern = expectedHashKey.getBytes();
    TreeMap<String, String> data = new TreeMap<String, String>();
    List<PegasusScannerInterface> scanners = client.getUnorderedScanners(tableName, 1, options);
    PegasusScannerInterface scanner = scanners.get(0);
    Assert.assertNotNull(scanner);
    Pair<Pair<byte[], byte[]>, byte[]> item;
    while ((item = scanner.next()) != null) {
      Assert.assertArrayEquals(expectedHashKey.getBytes(), item.getLeft().getLeft());
      checkAndPutSortMap(
          data,
          expectedHashKey,
          new String(item.getLeft().getRight()),
          new String(item.getRight()));
    }
    scanner.close();
    compareSortMap(data, base.get(expectedHashKey), expectedHashKey);
  }

  @Test
  public void testScanFailRecover() throws PException {
    PegasusClientInterface client = PegasusClientFactory.getSingletonClient();
    String tableName = "temp";
    String hashKey = "scanHashKey";
    int count = 0;
    while (count++ < 100) {
      client.set(tableName, hashKey.getBytes(), String.valueOf(count).getBytes(), "".getBytes());
    }

    ScanOptions options = new ScanOptions();
    options.batchSize = 1;
    PegasusScannerInterface scanner =
        client.getScanner(tableName, hashKey.getBytes(), "".getBytes(), "".getBytes(), options);
    Pair<Pair<byte[], byte[]>, byte[]> item;
    List<Pair<Pair<byte[], byte[]>, byte[]>> items = new ArrayList<>();

    // test encounter error
    int loop = 0;
    boolean encounterErrorMocked = false;
    while (loop++ < 100) {
      try {
        if ((item = scanner.next()) != null) {
          items.add(item);
          if (!encounterErrorMocked) {
            // only mock _encounterError = true, all the follow request will be failed
            mockEncounterErrorForTest(scanner);
            encounterErrorMocked = true;
          }
        }
      } catch (PException e) {
        Assertions.assertTrue(e.getMessage().contains("encounter unknown error"));
      }
    }
    Assertions.assertEquals(1, items.size());
    items.clear();

    // test encounter rpc error
    boolean rpcErrorMocked = false;
    scanner =
        client.getScanner(tableName, hashKey.getBytes(), "".getBytes(), "".getBytes(), options);
    while (true) {
      try {
        if ((item = scanner.next()) != null) {
          items.add(item);
          if (!rpcErrorMocked) {
            // mock _encounterError = true and _rpcFailed = true, follow request will be recovered
            // automatically
            mockRpcErrorForTest(scanner);
            rpcErrorMocked = true;
          }
        } else {
          break;
        }
      } catch (PException e) {
        Assertions.assertTrue(e.getMessage().contains("scan failed with error rpc"));
      }
    }
    Assertions.assertEquals(100, items.size());
  }

  private void mockEncounterErrorForTest(PegasusScannerInterface scanner) {
    try {
      Field encounterErrorField = scanner.getClass().getDeclaredField("encounterError");
      encounterErrorField.setAccessible(true);
      encounterErrorField.set(scanner, true);
      Field causeField = scanner.getClass().getDeclaredField("cause");
      causeField.setAccessible(true);
      causeField.set(scanner, new PException("encounter unknown error"));
    } catch (Exception ignored) {
    }
  }

  protected void mockRpcErrorForTest(PegasusScannerInterface scanner) {
    try {
      Field encounterErrorField = scanner.getClass().getDeclaredField("encounterError");
      encounterErrorField.setAccessible(true);
      encounterErrorField.set(scanner, true);
      Field rpcFailedField = scanner.getClass().getDeclaredField("rpcFailed");
      rpcFailedField.setAccessible(true);
      rpcFailedField.set(scanner, true);
      Field causeField = scanner.getClass().getDeclaredField("cause");
      causeField.setAccessible(true);
      causeField.set(scanner, new PException("scan failed with error rpc"));
    } catch (Exception ignored) {
    }
  }

  private static void clearDatabase() throws PException {
    ScanOptions options = new ScanOptions();
    List<PegasusScannerInterface> scanners = client.getUnorderedScanners(tableName, 1, options);
    Assert.assertEquals(1, scanners.size());
    Assert.assertNotNull(scanners.get(0));

    Pair<Pair<byte[], byte[]>, byte[]> item;
    while ((item = scanners.get(0).next()) != null) {
      client.del(tableName, item.getLeft().getLeft(), item.getLeft().getRight());
    }
    scanners.get(0).close();

    scanners = client.getUnorderedScanners(tableName, 1, options);
    Assert.assertEquals(1, scanners.size());
    Assert.assertNotNull(scanners.get(0));
    item = scanners.get(0).next();
    scanners.get(0).close();
    Assert.assertNull(
        item == null
            ? null
            : String.format(
                "Database is cleared but not empty, hashKey=%s, sortKey=%s",
                new String(item.getLeft().getLeft()), new String(item.getLeft().getRight())),
        item);
  }

  private static String randomString() {
    int pos = random.nextInt(buffer.length);
    buffer[pos] = CCH[random.nextInt(CCH.length)];
    int length = random.nextInt(buffer.length) + 1;
    if (pos + length < buffer.length) {
      return new String(buffer, pos, length);
    } else {
      return new String(buffer, pos, buffer.length - pos)
          + new String(buffer, 0, length + pos - buffer.length);
    }
  }

  private static void checkAndPut(
      TreeMap<String, TreeMap<String, String>> data, String hashKey, String sortKey, String value) {
    TreeMap<String, String> sortMap = data.get(hashKey);
    if (sortMap == null) {
      sortMap = new TreeMap<>();
      data.put(hashKey, sortMap);
    } else {
      Assert.assertNull(
          String.format(
              "Duplicate: hashKey=%s, sortKye=%s, oldValue=%s, newValue=%s",
              hashKey, sortKey, sortMap.get(sortKey), value),
          sortMap.get(sortKey));
    }
    sortMap.put(sortKey, value);
  }

  private static void checkAndPutSortMap(
      TreeMap<String, String> data, String hashKey, String sortKey, String value) {
    Assert.assertNull(
        String.format(
            "Duplicate: hashKey=%s, sortKye=%s, oldValue=%s, newValue=%s",
            hashKey, sortKey, data.get(sortKey), value),
        data.get(sortKey));
    data.put(sortKey, value);
  }

  private static void compare(
      TreeMap<String, TreeMap<String, String>> data,
      TreeMap<String, TreeMap<String, String>> base) {
    Iterator<Map.Entry<String, TreeMap<String, String>>> iterator1 = data.entrySet().iterator();
    Iterator<Map.Entry<String, TreeMap<String, String>>> iterator2 = base.entrySet().iterator();
    while (true) {
      Map.Entry<String, TreeMap<String, String>> kv1 =
          iterator1.hasNext() ? iterator1.next() : null;
      Map.Entry<String, TreeMap<String, String>> kv2 =
          iterator2.hasNext() ? iterator2.next() : null;
      if (kv1 == null) {
        Assert.assertNull(
            kv2 == null ? null : String.format("Only in base: hashKey=%s", kv2.getKey()), kv2);
        break;
      }
      Assert.assertNotNull(String.format("Only in data: hashKey=%s", kv1.getKey()), kv2);
      Assert.assertEquals(
          String.format("Diff: dataHashKey=%s, baseHashKey=%s", kv1.getKey(), kv2.getKey()),
          kv1.getKey(),
          kv2.getKey());
      Iterator<Map.Entry<String, String>> iterator3 = kv1.getValue().entrySet().iterator();
      Iterator<Map.Entry<String, String>> iterator4 = kv2.getValue().entrySet().iterator();
      while (true) {
        Map.Entry<String, String> kv3 = iterator3.hasNext() ? iterator3.next() : null;
        Map.Entry<String, String> kv4 = iterator4.hasNext() ? iterator4.next() : null;
        if (kv3 == null) {
          Assert.assertNull(
              kv4 == null
                  ? null
                  : String.format(
                      "Only in base: hashKey=%s, sortKey=%s, value=%s",
                      kv1.getKey(), kv4.getKey(), kv4.getValue()),
              kv4);
          break;
        }
        Assert.assertNotNull(
            String.format(
                "Only in data: hashKey=%s, sortKey=%s, value=%s",
                kv1.getKey(), kv3.getKey(), kv3.getValue()),
            kv4);
        Assert.assertEquals(
            String.format(
                "Diff: hashKey=%s, dataSortKey=%s, dataValue=%s, baseSortKey=%s, baseValue=%s",
                kv1.getKey(), kv3.getKey(), kv3.getValue(), kv4.getKey(), kv4.getValue()),
            kv3,
            kv4);
      }
    }
  }

  private static void compareSortMap(
      NavigableMap<String, String> data, NavigableMap<String, String> base, String hashKey) {
    Iterator<Map.Entry<String, String>> iterator1 = data.entrySet().iterator();
    Iterator<Map.Entry<String, String>> iterator2 = base.entrySet().iterator();
    while (true) {
      Map.Entry<String, String> kv1 = iterator1.hasNext() ? iterator1.next() : null;
      Map.Entry<String, String> kv2 = iterator2.hasNext() ? iterator2.next() : null;
      if (kv1 == null) {
        Assert.assertNull(
            kv2 == null
                ? null
                : String.format(
                    "Only in base: hashKey=%s, sortKey=%s, value=%s",
                    hashKey, kv2.getKey(), kv2.getValue()),
            kv2);
        break;
      }
      Assert.assertNotNull(
          String.format(
              "Only in data: hashKey=%s, sortKey=%s, value=%s",
              hashKey, kv1.getKey(), kv1.getValue()),
          kv2);
      Assert.assertEquals(
          String.format(
              "Diff: hashKey=%s, dataSortKey=%s, dataValue=%s, baseSortKey=%s, baseValue=%s",
              hashKey, kv1.getKey(), kv1.getValue(), kv2.getKey(), kv2.getValue()),
          kv1,
          kv2);
    }
  }
}
