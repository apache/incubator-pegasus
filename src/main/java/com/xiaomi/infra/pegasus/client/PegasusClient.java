// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.
package com.xiaomi.infra.pegasus.client;

import com.xiaomi.infra.pegasus.client.PegasusTableInterface.MultiGetSortKeysResult;
import com.xiaomi.infra.pegasus.rpc.*;
import com.xiaomi.infra.pegasus.tools.Tools;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author qinzuoyan
 *     <p>Implementation of {@link PegasusClientInterface}.
 */
public class PegasusClient implements PegasusClientInterface {
  private static final Logger LOGGER = LoggerFactory.getLogger(PegasusClient.class);

  private final ClientOptions clientOptions;

  private final ConcurrentHashMap<String, PegasusTable> tableMap;
  private final Object tableMapLock;
  private Cluster cluster;

  private static class PegasusHasher implements KeyHasher {
    @Override
    public long hash(byte[] key) {
      Validate.isTrue(key != null && key.length >= 2);
      ByteBuffer buf = ByteBuffer.wrap(key);
      int hashKeyLen = 0xFFFF & buf.getShort();
      Validate.isTrue(hashKeyLen != 0xFFFF && (2 + hashKeyLen <= key.length));
      return hashKeyLen == 0
          ? Tools.dsn_crc64(key, 2, key.length - 2)
          : Tools.dsn_crc64(key, 2, hashKeyLen);
    }
  }

  private PegasusTable getTable(String tableName) throws PException {
    return getTable(tableName, new InternalTableOptions(new PegasusHasher(), new TableOptions()));
  }

  private PegasusTable getTable(String tableName, InternalTableOptions internalTableOptions)
      throws PException {
    PegasusTable table = tableMap.get(tableName);
    if (table == null) {
      synchronized (tableMapLock) {
        table = tableMap.get(tableName);
        if (table == null) {
          try {
            Table internalTable = cluster.openTable(tableName, internalTableOptions);
            table = new PegasusTable(this, internalTable);
          } catch (Throwable e) {
            throw new PException(e);
          }
          tableMap.put(tableName, table);
        }
      }
    }
    return table;
  }

  public PegasusClient(Properties properties) throws PException {
    this(ClientOptions.create(properties));
  }

  // configPath could be:
  // - zk path: zk://host1:port1,host2:port2,host3:port3/path/to/config
  // - local file path: file:///path/to/config
  // - resource path: resource:///path/to/config
  public PegasusClient(String configPath) throws PException {
    this(ClientOptions.create(configPath));
  }

  public PegasusClient(ClientOptions clientOptions) throws PException {
    this.clientOptions = clientOptions;
    this.cluster = Cluster.createCluster(clientOptions);
    this.tableMap = new ConcurrentHashMap<String, PegasusTable>();
    this.tableMapLock = new Object();
    LOGGER.info(getConfigurationString());
  }

  public boolean isWriteLimitEnabled() {
    return clientOptions.isWriteLimitEnabled();
  }

  String getMetaList() {
    return clientOptions.getMetaServers();
  }

  @Override
  public void finalize() {
    close();
  }

  // generate rocksdb key.
  public static byte[] generateKey(byte[] hashKey, byte[] sortKey) {
    int hashKeyLen = (hashKey == null ? 0 : hashKey.length);
    Validate.isTrue(hashKeyLen < 0xFFFF, "length of hash key must be less than UINT16_MAX");
    int sortKeyLen = (sortKey == null ? 0 : sortKey.length);
    // default byte order of ByteBuffer is BIG_ENDIAN
    ByteBuffer buf = ByteBuffer.allocate(2 + hashKeyLen + sortKeyLen);
    buf.putShort((short) hashKeyLen);
    if (hashKeyLen > 0) {
      buf.put(hashKey);
    }
    if (sortKeyLen > 0) {
      buf.put(sortKey);
    }
    return buf.array();
  }

  // generate the adjacent next rocksdb key according to hash key.
  public static byte[] generateNextBytes(byte[] hashKey) {
    int hashKeyLen = hashKey == null ? 0 : hashKey.length;
    Validate.isTrue(hashKeyLen < 0xFFFF, "length of hash key must be less than UINT16_MAX");
    ByteBuffer buf = ByteBuffer.allocate(2 + hashKeyLen);
    buf.putShort((short) hashKeyLen);
    if (hashKeyLen > 0) {
      buf.put(hashKey);
    }
    byte[] array = buf.array();
    int i = array.length - 1;
    for (; i >= 0; i--) {
      // 0xFF will look like -1
      if (array[i] != -1) {
        array[i]++;
        break;
      }
    }
    return Arrays.copyOf(array, i + 1);
  }

  // generate the adjacent next rocksdb key according to hash key and sort key.
  public static byte[] generateNextBytes(byte[] hashKey, byte[] sortKey) {
    byte[] array = generateKey(hashKey, sortKey);
    int i = array.length - 1;
    for (; i >= 0; i--) {
      // 0xFF will look like -1
      if (array[i] != -1) {
        array[i]++;
        break;
      }
    }
    return Arrays.copyOf(array, i + 1);
  }

  public static Pair<byte[], byte[]> restoreKey(byte[] key) {
    Validate.isTrue(key != null && key.length >= 2);
    ByteBuffer buf = ByteBuffer.wrap(key);
    int hashKeyLen = 0xFFFF & buf.getShort();
    Validate.isTrue(hashKeyLen != 0xFFFF && (2 + hashKeyLen <= key.length));
    return new ImmutablePair<byte[], byte[]>(
        Arrays.copyOfRange(key, 2, 2 + hashKeyLen),
        Arrays.copyOfRange(key, 2 + hashKeyLen, key.length));
  }

  public static int bytesCompare(byte[] left, byte[] right) {
    int len = Math.min(left.length, right.length);
    for (int i = 0; i < len; i++) {
      int ret = (0xFF & left[i]) - (0xFF & right[i]);
      if (ret != 0) return ret;
    }
    return left.length - right.length;
  }

  public String getConfigurationString() {
    return clientOptions.toString();
  }

  @Override
  public void close() {
    synchronized (this) {
      if (cluster != null) {
        String metaList = StringUtils.join(cluster.getMetaList(), ",");
        LOGGER.info("start to close pegasus client for [{}]", metaList);
        cluster.close();
        cluster = null;
        LOGGER.info("finish to close pegasus client for [{}]", metaList);
      }
    }
  }

  @Override
  public PegasusTableInterface openTable(String tableName) throws PException {
    return getTable(tableName);
  }

  @Override
  public PegasusTableInterface openTable(String tableName, int backupRequestDelayMs)
      throws PException {
    return getTable(
        tableName,
        new InternalTableOptions(
            new PegasusHasher(),
            new TableOptions().withBackupRequestDelayMs(backupRequestDelayMs)));
  }

  @Override
  public PegasusTableInterface openTable(String tableName, TableOptions tableOptions)
      throws PException {
    return getTable(tableName, new InternalTableOptions(new PegasusHasher(), tableOptions));
  }

  @Override
  public ClientOptions getConfiguration() {
    return clientOptions;
  }

  @Override
  public boolean exist(String tableName, byte[] hashKey, byte[] sortKey) throws PException {
    PegasusTable tb = getTable(tableName);
    return tb.exist(hashKey, sortKey, 0);
  }

  @Override
  public long sortKeyCount(String tableName, byte[] hashKey) throws PException {
    PegasusTable tb = getTable(tableName);
    return tb.sortKeyCount(hashKey, 0);
  }

  @Override
  public byte[] get(String tableName, byte[] hashKey, byte[] sortKey) throws PException {
    PegasusTable tb = getTable(tableName);
    return tb.get(hashKey, sortKey, 0);
  }

  @Override
  public void batchGet(String tableName, List<Pair<byte[], byte[]>> keys, List<byte[]> values)
      throws PException {
    PegasusTable tb = getTable(tableName);
    tb.batchGet(keys, values, 0);
  }

  @Override
  public int batchGet2(
      String tableName, List<Pair<byte[], byte[]>> keys, List<Pair<PException, byte[]>> values)
      throws PException {
    PegasusTable tb = getTable(tableName);
    return tb.batchGet2(keys, values, 0);
  }

  @Override
  public boolean multiGet(
      String tableName,
      byte[] hashKey,
      List<byte[]> sortKeys,
      int maxFetchCount,
      int maxFetchSize,
      List<Pair<byte[], byte[]>> values)
      throws PException {
    if (values == null) {
      throw new PException("Invalid parameter: values should not be null");
    }
    PegasusTable tb = getTable(tableName);
    PegasusTableInterface.MultiGetResult res =
        tb.multiGet(hashKey, sortKeys, maxFetchCount, maxFetchSize, 0);
    for (Pair<byte[], byte[]> kv : res.values) {
      values.add(kv);
    }
    return res.allFetched;
  }

  @Override
  public boolean multiGet(
      String tableName, byte[] hashKey, List<byte[]> sortKeys, List<Pair<byte[], byte[]>> values)
      throws PException {
    return multiGet(tableName, hashKey, sortKeys, 100, 1000000, values);
  }

  @Override
  public boolean multiGet(
      String tableName,
      byte[] hashKey,
      byte[] startSortKey,
      byte[] stopSortKey,
      MultiGetOptions options,
      int maxFetchCount,
      int maxFetchSize,
      List<Pair<byte[], byte[]>> values)
      throws PException {
    if (values == null) {
      throw new PException("Invalid parameter: values should not be null");
    }
    PegasusTable tb = getTable(tableName);
    PegasusTableInterface.MultiGetResult res =
        tb.multiGet(hashKey, startSortKey, stopSortKey, options, maxFetchCount, maxFetchSize, 0);
    values.addAll(res.values);
    return res.allFetched;
  }

  @Override
  public boolean multiGet(
      String tableName,
      byte[] hashKey,
      byte[] startSortKey,
      byte[] stopSortKey,
      MultiGetOptions options,
      List<Pair<byte[], byte[]>> values)
      throws PException {
    return multiGet(tableName, hashKey, startSortKey, stopSortKey, options, 100, 1000000, values);
  }

  @Override
  public void batchMultiGet(
      String tableName, List<Pair<byte[], List<byte[]>>> keys, List<HashKeyData> values)
      throws PException {
    PegasusTable tb = getTable(tableName);
    tb.batchMultiGet(keys, values, 0);
  }

  @Override
  public int batchMultiGet2(
      String tableName,
      List<Pair<byte[], List<byte[]>>> keys,
      List<Pair<PException, HashKeyData>> results)
      throws PException {
    PegasusTable tb = getTable(tableName);
    return tb.batchMultiGet2(keys, results, 0);
  }

  @Override
  public boolean multiGetSortKeys(
      String tableName, byte[] hashKey, int maxFetchCount, int maxFetchSize, List<byte[]> sortKeys)
      throws PException {
    if (sortKeys == null) {
      throw new PException("Invalid parameter: sortKeys should not be null");
    }
    PegasusTable table = getTable(tableName);
    MultiGetSortKeysResult result = table.multiGetSortKeys(hashKey, maxFetchCount, maxFetchSize, 0);
    sortKeys.addAll(result.keys);
    return result.allFetched;
  }

  @Override
  public boolean multiGetSortKeys(String tableName, byte[] hashKey, List<byte[]> sortKeys)
      throws PException {
    return multiGetSortKeys(tableName, hashKey, 100, 1000000, sortKeys);
  }

  @Override
  public void set(String tableName, byte[] hashKey, byte[] sortKey, byte[] value, int ttlSeconds)
      throws PException {
    PegasusTable tb = getTable(tableName);
    tb.set(hashKey, sortKey, value, ttlSeconds, 0);
  }

  @Override
  public void set(String tableName, byte[] hashKey, byte[] sortKey, byte[] value)
      throws PException {
    set(tableName, hashKey, sortKey, value, 0);
  }

  @Override
  public void batchSet(String tableName, List<SetItem> items) throws PException {
    PegasusTable tb = getTable(tableName);
    tb.batchSet(items, 0);
  }

  @Override
  public int batchSet2(String tableName, List<SetItem> items, List<PException> results)
      throws PException {
    PegasusTable tb = getTable(tableName);
    return tb.batchSet2(items, results, 0);
  }

  @Override
  public void multiSet(
      String tableName, byte[] hashKey, List<Pair<byte[], byte[]>> values, int ttlSeconds)
      throws PException {
    PegasusTable tb = getTable(tableName);
    tb.multiSet(hashKey, values, ttlSeconds, 0);
  }

  @Override
  public void multiSet(String tableName, byte[] hashKey, List<Pair<byte[], byte[]>> values)
      throws PException {
    multiSet(tableName, hashKey, values, 0);
  }

  @Override
  public void batchMultiSet(String tableName, List<HashKeyData> items, int ttlSeconds)
      throws PException {
    PegasusTable tb = getTable(tableName);
    tb.batchMultiSet(items, ttlSeconds, 0);
  }

  @Override
  public void batchMultiSet(String tableName, List<HashKeyData> items) throws PException {
    batchMultiSet(tableName, items, 0);
  }

  @Override
  public int batchMultiSet2(
      String tableName, List<HashKeyData> items, int ttlSeconds, List<PException> results)
      throws PException {
    PegasusTable tb = getTable(tableName);
    return tb.batchMultiSet2(items, ttlSeconds, results, 0);
  }

  @Override
  public int batchMultiSet2(String tableName, List<HashKeyData> items, List<PException> results)
      throws PException {
    return batchMultiSet2(tableName, items, 0, results);
  }

  @Override
  public void del(String tableName, byte[] hashKey, byte[] sortKey) throws PException {
    PegasusTable tb = getTable(tableName);
    tb.del(hashKey, sortKey, 0);
  }

  @Override
  public void batchDel(String tableName, List<Pair<byte[], byte[]>> keys) throws PException {
    PegasusTable tb = getTable(tableName);
    tb.batchDel(keys, 0);
  }

  @Override
  public int batchDel2(String tableName, List<Pair<byte[], byte[]>> keys, List<PException> results)
      throws PException {
    PegasusTable tb = getTable(tableName);
    return tb.batchDel2(keys, results, 0);
  }

  @Override
  public void multiDel(String tableName, byte[] hashKey, List<byte[]> sortKeys) throws PException {
    PegasusTable tb = getTable(tableName);
    tb.multiDel(hashKey, sortKeys, 0);
  }

  @Override
  public void delRange(
      String tableName,
      byte[] hashKey,
      byte[] startSortKey,
      byte[] stopSortKey,
      DelRangeOptions options)
      throws PException {
    PegasusTable tb = getTable(tableName);
    tb.delRange(hashKey, startSortKey, stopSortKey, options, 0);
  }

  @Override
  public void batchMultiDel(String tableName, List<Pair<byte[], List<byte[]>>> keys)
      throws PException {
    PegasusTable tb = getTable(tableName);
    tb.batchMultiDel(keys, 0);
  }

  @Override
  public int batchMultiDel2(
      String tableName, List<Pair<byte[], List<byte[]>>> keys, List<PException> results)
      throws PException {
    PegasusTable tb = getTable(tableName);
    return tb.batchMultiDel2(keys, results, 0);
  }

  @Override
  public long incr(String tableName, byte[] hashKey, byte[] sortKey, long increment, int ttlSeconds)
      throws PException {
    PegasusTable tb = getTable(tableName);
    return tb.incr(hashKey, sortKey, increment, ttlSeconds, 0);
  }

  @Override
  public long incr(String tableName, byte[] hashKey, byte[] sortKey, long increment)
      throws PException {
    PegasusTable tb = getTable(tableName);
    return tb.incr(hashKey, sortKey, increment, 0);
  }

  @Override
  public PegasusTableInterface.CheckAndSetResult checkAndSet(
      String tableName,
      byte[] hashKey,
      byte[] checkSortKey,
      CheckType checkType,
      byte[] checkOperand,
      byte[] setSortKey,
      byte[] setValue,
      CheckAndSetOptions options)
      throws PException {
    PegasusTable tb = getTable(tableName);
    return tb.checkAndSet(
        hashKey, checkSortKey, checkType, checkOperand, setSortKey, setValue, options, 0);
  }

  @Override
  public PegasusTableInterface.CheckAndMutateResult checkAndMutate(
      String tableName,
      byte[] hashKey,
      byte[] checkSortKey,
      CheckType checkType,
      byte[] checkOperand,
      Mutations mutations,
      CheckAndMutateOptions options)
      throws PException {
    PegasusTable tb = getTable(tableName);
    return tb.checkAndMutate(hashKey, checkSortKey, checkType, checkOperand, mutations, options, 0);
  }

  @Override
  public PegasusTableInterface.CompareExchangeResult compareExchange(
      String tableName,
      byte[] hashKey,
      byte[] sortKey,
      byte[] expectedValue,
      byte[] desiredValue,
      int ttlSeconds)
      throws PException {
    PegasusTable tb = getTable(tableName);
    return tb.compareExchange(hashKey, sortKey, expectedValue, desiredValue, ttlSeconds, 0);
  }

  @Override
  public int ttl(String tableName, byte[] hashKey, byte[] sortKey) throws PException {
    PegasusTable tb = getTable(tableName);
    return tb.ttl(hashKey, sortKey, 0);
  }

  @Override
  public PegasusScannerInterface getScanner(
      String tableName,
      byte[] hashKey,
      byte[] startSortKey,
      byte[] stopSortKey,
      ScanOptions options)
      throws PException {
    PegasusTable tb = getTable(tableName);
    return tb.getScanner(hashKey, startSortKey, stopSortKey, options);
  }

  @Override
  public List<PegasusScannerInterface> getUnorderedScanners(
      String tableName, int maxScannerCount, ScanOptions options) throws PException {
    PegasusTable tb = getTable(tableName);
    return tb.getUnorderedScanners(maxScannerCount, options);
  }
}
