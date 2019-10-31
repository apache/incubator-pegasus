// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.
package com.xiaomi.infra.pegasus.client;

import java.util.*;
import org.apache.commons.lang3.tuple.Pair;

/**
 * @author qinzuoyan
 *     <p>This class provides interfaces to access data of a specified cluster.
 */
public interface PegasusClientInterface {
  /**
   * Get pegasus configuration for client.
   *
   * @return config
   */
  public Properties getConfiguration();

  /** Close the client. The client can not be used again after closed. */
  public void close();

  /**
   * Open a table, and prepare the sessions and route-table to the replica-servers.
   *
   * <p>Please notice that pegasus support two kinds of API: 1. the client-interface way, which is
   * provided in this class. 2. the table-interface way, which is provided by {@link
   * PegasusTableInterface}. With the client-interface, you don't need to create
   * PegasusTableInterface by openTable, so you can access the pegasus cluster conveniently.
   * However, the client-interface's api also has some restrictions: 1. we don't provide async
   * methods in client-interface. 2. the timeout in client-interface isn't as accurate as the
   * table-interface. 3. the client-interface may throw an exception when open table fails. It means
   * that you may need to handle this exception in every data access operation, which is annoying.
   * 4. You can't specify a per-operation timeout. So we recommend you to use the table-interface.
   *
   * @param tableName the table should be exist on the server, which is created before by the system
   *     administrator
   * @return the table handler
   * @throws PException throws exception if any error occurs.
   */
  public PegasusTableInterface openTable(String tableName) throws PException;

  /**
   * Check value exist by key from the cluster
   *
   * @param tableName TableHandler name
   * @param hashKey used to decide which partition the key may exist.
   * @param sortKey all keys under the same hashKey will be sorted by sortKey
   * @return true if exist, false if not exist
   * @throws PException throws exception if any error occurs.
   */
  public boolean exist(String tableName, byte[] hashKey, byte[] sortKey) throws PException;

  /**
   * @param tableName TableHandler name
   * @param hashKey used to decide which partition the key may exist.
   * @return the count result for the hashKey
   * @throws PException throws exception if any error occurs.
   */
  public long sortKeyCount(String tableName, byte[] hashKey) throws PException;

  /**
   * Get value.
   *
   * @param tableName TableHandler name
   * @param hashKey used to decide which partition to get this k-v, if null or length == 0, means no
   *     hash key.
   * @param sortKey all the k-v under hashKey will be sorted by sortKey, if null or length == 0,
   *     means no sort key.
   * @return value; null if not found
   * @throws PException throws exception if any error occurs.
   */
  public byte[] get(String tableName, byte[] hashKey, byte[] sortKey) throws PException;

  /**
   * Batch get values of different keys. Will terminate immediately if any error occurs.
   *
   * @param tableName table name
   * @param keys hashKey and sortKey pair list.
   * @param values output values; should be created by caller; if succeed, the size of values will
   *     be same with keys; the value of keys[i] is stored in values[i]; if the value of keys[i] is
   *     not found, then values[i] will be set to null.
   * @throws PException throws exception if any error occurs.
   *     <p>Notice: the method is not atomic, that means, maybe some keys succeed but some keys
   *     failed.
   */
  public void batchGet(String tableName, List<Pair<byte[], byte[]>> keys, List<byte[]> values)
      throws PException;

  /**
   * Batch get values of different keys. Will wait for all requests done even if some error occurs.
   *
   * @param tableName table name
   * @param keys hashKey and sortKey pair list.
   * @param results output results; should be created by caller; after call done, the size of
   *     results will be same with keys; the results[i] is a Pair: - if Pair.left != null : means
   *     query keys[i] failed, Pair.left is the exception. - if Pair.left == null : means query
   *     keys[i] succeed, Pair.right is the result value.
   * @return succeed count.
   * @throws PException throws exception if any error occurs.
   *     <p>Notice: the method is not atomic, that means, maybe some keys succeed but some keys
   *     failed.
   */
  public int batchGet2(
      String tableName, List<Pair<byte[], byte[]>> keys, List<Pair<PException, byte[]>> results)
      throws PException;

  /**
   * Get multiple values under the same hash key.
   *
   * @param tableName table name
   * @param hashKey used to decide which partition to put this k-v, should not be null or empty.
   * @param sortKeys all the k-v under hashKey will be sorted by sortKey, if null or empty, means
   *     fetch all sortKeys under the hashKey.
   * @param maxFetchCount max count of k-v pairs to be fetched. max_fetch_count <= 0 means no limit.
   *     default value is 100.
   * @param maxFetchSize max size of k-v pairs to be fetched. max_fetch_size <= 0 means no limit.
   *     default value is 1000000.
   * @param values output values; if sortKey in the input sortKeys is not found, it won't be in
   *     values. if sortKeys is null or empty, then the returned values will be ascending ordered by
   *     sortKey.
   * @return true if all data is fetched; false if only partial data is fetched.
   * @throws PException throws exception if any error occurs.
   */
  public boolean multiGet(
      String tableName,
      byte[] hashKey,
      List<byte[]> sortKeys,
      int maxFetchCount,
      int maxFetchSize,
      List<Pair<byte[], byte[]>> values)
      throws PException;

  public boolean multiGet(
      String tableName, byte[] hashKey, List<byte[]> sortKeys, List<Pair<byte[], byte[]>> values)
      throws PException;

  /**
   * Get multiple key-values under the same hashKey with sortKey range limited.
   *
   * @param tableName table name
   * @param hashKey used to decide which partition the key may exist should not be null or empty.
   * @param startSortKey the start sort key. null means "".
   * @param stopSortKey the stop sort key. null or "" means fetch to the last sort key.
   * @param options multi-get options.
   * @param maxFetchCount max count of kv pairs to be fetched maxFetchCount <= 0 means no limit.
   *     default value is 100
   * @param maxFetchSize max size of kv pairs to be fetched. maxFetchSize <= 0 means no limit.
   *     default value is 1000000.
   * @param values output values; the returned values will be ascending ordered by sortKey.
   * @return true if all data is fetched; false if only partial data is fetched.
   * @throws PException throws exception if any error occurs.
   */
  public boolean multiGet(
      String tableName,
      byte[] hashKey,
      byte[] startSortKey,
      byte[] stopSortKey,
      MultiGetOptions options,
      int maxFetchCount,
      int maxFetchSize,
      List<Pair<byte[], byte[]>> values)
      throws PException;

  public boolean multiGet(
      String tableName,
      byte[] hashKey,
      byte[] startSortKey,
      byte[] stopSortKey,
      MultiGetOptions options,
      List<Pair<byte[], byte[]>> values)
      throws PException;

  /**
   * Batch get multiple values under the same hash key. Will terminate immediately if any error
   * occurs.
   *
   * @param tableName table name
   * @param keys List{hashKey,List{sortKey}}; if List{sortKey} is null or empty, means fetch all
   *     sortKeys under the hashKey.
   * @param values output values; should be created by caller; if succeed, the size of values will
   *     be same with keys; the data for keys[i] is stored in values[i].
   * @throws PException throws exception if any error occurs.
   *     <p>Notice: the method is not atomic, that means, maybe some keys succeed but some keys
   *     failed.
   */
  public void batchMultiGet(
      String tableName, List<Pair<byte[], List<byte[]>>> keys, List<HashKeyData> values)
      throws PException;

  /**
   * Batch get multiple values under the same hash key. Will wait for all requests done even if some
   * error occurs.
   *
   * @param tableName table name
   * @param keys List{hashKey,List{sortKey}}; if List{sortKey} is null or empty, means fetch all
   *     sortKeys under the hashKey.
   * @param results output results; should be created by caller; after call done, the size of
   *     results will be same with keys; the results[i] is a Pair: - if Pair.left != null : means
   *     query keys[i] failed, Pair.left is the exception. - if Pair.left == null : means query
   *     keys[i] succeed, Pair.right is the result value.
   * @return succeed count.
   * @throws PException throws exception if any error occurs.
   *     <p>Notice: the method is not atomic, that means, maybe some keys succeed but some keys
   *     failed.
   */
  public int batchMultiGet2(
      String tableName,
      List<Pair<byte[], List<byte[]>>> keys,
      List<Pair<PException, HashKeyData>> results)
      throws PException;

  /**
   * Get multiple sort keys under the same hash key.
   *
   * @param tableName table name
   * @param hashKey used to decide which partition to put this k-v, should not be null or empty.
   * @param maxFetchCount max count of k-v pairs to be fetched. max_fetch_count <= 0 means no limit.
   *     default value is 100.
   * @param maxFetchSize max size of k-v pairs to be fetched. max_fetch_size <= 0 means no limit.
   *     default value is 1000000.
   * @param sortKeys output sort keys.
   * @return true if all data is fetched; false if only partial data is fetched.
   * @throws PException throws exception if any error occurs.
   */
  public boolean multiGetSortKeys(
      String tableName, byte[] hashKey, int maxFetchCount, int maxFetchSize, List<byte[]> sortKeys)
      throws PException;

  public boolean multiGetSortKeys(String tableName, byte[] hashKey, List<byte[]> sortKeys)
      throws PException;

  /**
   * Set value.
   *
   * @param tableName TableHandler name
   * @param hashKey used to decide which partition to put this k-v, if null or length == 0, means no
   *     hash key.
   * @param sortKey all the k-v under hashKey will be sorted by sortKey, if null or length == 0,
   *     means no sort key.
   * @param value should not be null
   * @param ttlSeconds time to live in seconds, 0 means no ttl. default value is 0.
   * @throws PException throws exception if any error occurs.
   */
  public void set(String tableName, byte[] hashKey, byte[] sortKey, byte[] value, int ttlSeconds)
      throws PException;

  public void set(String tableName, byte[] hashKey, byte[] sortKey, byte[] value) throws PException;

  /**
   * Batch set lots of values. Will terminate immediately if any error occurs.
   *
   * @param tableName TableHandler name
   * @param items list of items.
   * @throws PException throws exception if any error occurs.
   *     <p>Notice: the method is not atomic, that means, maybe some keys succeed but some keys
   *     failed.
   */
  public void batchSet(String tableName, List<SetItem> items) throws PException;

  /**
   * Batch set lots of values. Will wait for all requests done even if some error occurs.
   *
   * @param tableName table name
   * @param items list of items.
   * @param results output results; should be created by caller; after call done, the size of
   *     results will be same with items; the results[i] is a PException: - if results[i] != null :
   *     means set items[i] failed, results[i] is the exception. - if results[i] == null : means set
   *     items[i] succeed.
   * @return succeed count.
   * @throws PException throws exception if any error occurs.
   *     <p>Notice: the method is not atomic, that means, maybe some keys succeed but some keys
   *     failed.
   */
  public int batchSet2(String tableName, List<SetItem> items, List<PException> results)
      throws PException;

  /**
   * Set multiple value under the same hash key.
   *
   * @param tableName table name
   * @param hashKey used to decide which partition to put this k-v, should not be null or empty.
   * @param values all {@literal <sortkey,value>} pairs to be set, should not be null or empty.
   * @param ttlSeconds time to live in seconds, 0 means no ttl. default value is 0.
   * @throws PException throws exception if any error occurs.
   */
  public void multiSet(
      String tableName, byte[] hashKey, List<Pair<byte[], byte[]>> values, int ttlSeconds)
      throws PException;

  public void multiSet(String tableName, byte[] hashKey, List<Pair<byte[], byte[]>> values)
      throws PException;

  /**
   * Batch set multiple value under the same hash key. Will terminate immediately if any error
   * occurs.
   *
   * @param tableName TableHandler name
   * @param items list of items.
   * @param ttlSeconds time to live in seconds, 0 means no ttl. default value is 0.
   * @throws PException throws exception if any error occurs.
   *     <p>Notice: the method is not atomic, that means, maybe some keys succeed but some keys
   *     failed.
   */
  public void batchMultiSet(String tableName, List<HashKeyData> items, int ttlSeconds)
      throws PException;

  public void batchMultiSet(String tableName, List<HashKeyData> items) throws PException;

  /**
   * Batch set multiple value under the same hash key. Will wait for all requests done even if some
   * error occurs.
   *
   * @param tableName table name
   * @param items list of items.
   * @param ttlSeconds time to live in seconds, 0 means no ttl. default value is 0.
   * @param results output results; should be created by caller; after call done, the size of
   *     results will be same with items; the results[i] is a PException: - if results[i] != null :
   *     means set items[i] failed, results[i] is the exception. - if results[i] == null : means set
   *     items[i] succeed.
   * @return succeed count.
   * @throws PException throws exception if any error occurs.
   *     <p>Notice: the method is not atomic, that means, maybe some keys succeed but some keys
   *     failed.
   */
  public int batchMultiSet2(
      String tableName, List<HashKeyData> items, int ttlSeconds, List<PException> results)
      throws PException;

  public int batchMultiSet2(String tableName, List<HashKeyData> items, List<PException> results)
      throws PException;

  /**
   * Delete value.
   *
   * @param tableName TableHandler name
   * @param hashKey used to decide which partition to put this k-v, if null or length == 0, means no
   *     hash key.
   * @param sortKey all the k-v under hashKey will be sorted by sortKey, if null or length == 0,
   *     means no sort key.
   * @throws PException throws exception if any error occurs.
   */
  public void del(String tableName, byte[] hashKey, byte[] sortKey) throws PException;

  /**
   * Batch delete values of different keys. Will terminate immediately if any error occurs.
   *
   * @param tableName table name
   * @param keys hashKey and sortKey pair list.
   * @throws PException throws exception if any error occurs.
   *     <p>Notice: the method is not atomic, that means, maybe some keys succeed but some keys
   *     failed.
   */
  public void batchDel(String tableName, List<Pair<byte[], byte[]>> keys) throws PException;

  /**
   * Batch delete values of different keys. Will wait for all requests done even if some error
   * occurs.
   *
   * @param tableName table name
   * @param keys hashKey and sortKey pair list.
   * @param results output results; should be created by caller; after call done, the size of
   *     results will be same with keys; the results[i] is a PException: - if results[i] != null :
   *     means del keys[i] failed, results[i] is the exception. - if results[i] == null : means del
   *     keys[i] succeed.
   * @return succeed count.
   * @throws PException throws exception if any error occurs.
   *     <p>Notice: the method is not atomic, that means, maybe some keys succeed but some keys
   *     failed.
   */
  public int batchDel2(String tableName, List<Pair<byte[], byte[]>> keys, List<PException> results)
      throws PException;

  /**
   * Delete specified sort keys under the same hash key.
   *
   * @param tableName table name
   * @param hashKey used to decide which partition to put this k-v, should not be null or empty.
   * @param sortKeys specify sort keys to be deleted. should not be empty.
   * @throws PException throws exception if any error occurs.
   */
  public void multiDel(String tableName, byte[] hashKey, List<byte[]> sortKeys) throws PException;

  /**
   * Delete key-values within range of startSortKey and stopSortKey under hashKey. Will terminate
   * immediately if any error occurs.
   *
   * @param tableName table name
   * @param hashKey used to decide which partition the key may exist should not be null or empty.
   * @param startSortKey the start sort key. null means "".
   * @param stopSortKey the stop sort key. null or "" means fetch to the last sort key.
   * @param options del range options.
   * @throws PException throws exception if any error occurs.
   */
  public void delRange(
      String tableName,
      byte[] hashKey,
      byte[] startSortKey,
      byte[] stopSortKey,
      DelRangeOptions options)
      throws PException;

  /**
   * Batch delete specified sort keys under the same hash key. Will terminate immediately if any
   * error occurs.
   *
   * @param tableName table name
   * @param keys List{hashKey,List{sortKey}}
   * @throws PException throws exception if any error occurs.
   *     <p>Notice: the method is not atomic, that means, maybe some keys succeed but some keys
   *     failed.
   */
  public void batchMultiDel(String tableName, List<Pair<byte[], List<byte[]>>> keys)
      throws PException;

  /**
   * Batch delete specified sort keys under the same hash key. Will wait for all requests done even
   * if some error occurs.
   *
   * @param tableName table name
   * @param keys List{hashKey,List{sortKey}}
   * @param results output results; should be created by caller; after call done, the size of
   *     results will be same with keys; the results[i] is a PException: - if results[i] != null :
   *     means del keys[i] failed, results[i] is the exception. - if results[i] == null : means del
   *     keys[i] succeed.
   * @return succeed count.
   * @throws PException throws exception if any error occurs.
   *     <p>Notice: the method is not atomic, that means, maybe some keys succeed but some keys
   *     failed.
   */
  public int batchMultiDel2(
      String tableName, List<Pair<byte[], List<byte[]>>> keys, List<PException> results)
      throws PException;

  /**
   * Get ttl time.
   *
   * @param tableName TableHandler name
   * @param hashKey used to decide which partition to put this k-v, if null or length == 0, means no
   *     hash key.
   * @param sortKey all the k-v under hashKey will be sorted by sortKey, if null or length == 0,
   *     means no sort key.
   * @return ttl time in seconds; -1 if no ttl set; -2 if not exist.
   * @throws PException throws exception if any error occurs.
   */
  public int ttl(String tableName, byte[] hashKey, byte[] sortKey) throws PException;

  /**
   * Atomically increment value.
   *
   * @param tableName the table name.
   * @param hashKey the hash key to increment.
   * @param sortKey the sort key to increment.
   * @param increment the increment to be added to the old value.
   * @param ttlSeconds time to live in seconds for the new value. should be no less than -1. for the
   *     second method, the ttlSeconds is 0. - if ttlSeconds == 0, the semantic is the same as
   *     redis: - normally, increment will preserve the original ttl. - if old data is expired by
   *     ttl, then set initial value to 0 and set no ttl. - if ttlSeconds > 0, then update with the
   *     new ttl if increment succeed. - if ttlSeconds == -1, then update to no ttl if increment
   *     succeed.
   * @return the new value.
   * @throws PException throws exception if any error occurs.
   */
  public long incr(String tableName, byte[] hashKey, byte[] sortKey, long increment, int ttlSeconds)
      throws PException;

  public long incr(String tableName, byte[] hashKey, byte[] sortKey, long increment)
      throws PException;

  /**
   * Atomically check and set value by key. If the check condition is satisfied, then apply to set
   * value.
   *
   * @param tableName the table name.
   * @param hashKey the hash key to check and set.
   * @param checkSortKey the sort key to check.
   * @param checkType the check type.
   * @param checkOperand the check operand.
   * @param setSortKey the sort key to set value if check condition is satisfied.
   * @param setValue the value to set if check condition is satisfied.
   * @param options the check-and-set options.
   * @return CheckAndSetResult
   * @throws PException throws exception if any error occurs.
   */
  public PegasusTableInterface.CheckAndSetResult checkAndSet(
      String tableName,
      byte[] hashKey,
      byte[] checkSortKey,
      CheckType checkType,
      byte[] checkOperand,
      byte[] setSortKey,
      byte[] setValue,
      CheckAndSetOptions options)
      throws PException;

  /**
   * Atomically check and mutate by key. If the check condition is satisfied, then apply to mutate.
   *
   * @param tableName the table name.
   * @param hashKey the hash key to check and mutate.
   * @param checkSortKey the sort key to check.
   * @param checkType the check type.
   * @param checkOperand the check operand.
   * @param mutations the list of mutations to perform if check condition is satisfied.
   * @param options the check-and-mutate options.
   * @return CheckAndMutateResult
   * @throws PException throws exception if any error occurs.
   */
  PegasusTableInterface.CheckAndMutateResult checkAndMutate(
      String tableName,
      byte[] hashKey,
      byte[] checkSortKey,
      CheckType checkType,
      byte[] checkOperand,
      Mutations mutations,
      CheckAndMutateOptions options)
      throws PException;

  /**
   * Atomically compare and exchange value by key.
   *
   * <p>- if the original value for the key is equal to the expected value, then update it with the
   * desired value, set CompareExchangeResult.setSucceed to true, and set
   * CompareExchangeResult.actualValue to null because the actual value must be equal to the desired
   * value. - if the original value for the key is not exist or not equal to the expected value,
   * then set CompareExchangeResult.setSucceed to false, and set the actual value in
   * CompareExchangeResult.actualValue.
   *
   * <p>This method is very like the C++ function in
   * {https://en.cppreference.com/w/cpp/atomic/atomic_compare_exchange}.
   *
   * @param tableName the table name.
   * @param hashKey the hash key to compare and exchange.
   * @param sortKey the sort key to compare and exchange.
   * @param expectedValue the value expected to be found for the key.
   * @param desiredValue the desired value to set if the original value for the key is equal to the
   *     expected value.
   * @param ttlSeconds time to live in seconds of the desired value, 0 means no ttl.
   * @return CompareExchangeResult
   * @throws PException throws exception if any error occurs.
   */
  public PegasusTableInterface.CompareExchangeResult compareExchange(
      String tableName,
      byte[] hashKey,
      byte[] sortKey,
      byte[] expectedValue,
      byte[] desiredValue,
      int ttlSeconds)
      throws PException;

  /**
   * Get Scanner for {startSortKey, stopSortKey} within hashKey
   *
   * @param tableName TableHandler name
   * @param hashKey used to decide which partition to put this k-v,
   * @param startSortKey start sort key scan from if null or length == 0, means start from begin
   * @param stopSortKey stop sort key scan to if null or length == 0, means stop to end
   * @param options scan options like endpoint inclusive/exclusive
   * @return scanner
   * @throws PException throws exception if any error occurs.
   */
  public PegasusScannerInterface getScanner(
      String tableName,
      byte[] hashKey,
      byte[] startSortKey,
      byte[] stopSortKey,
      ScanOptions options)
      throws PException;

  /**
   * Get Scanners for all data in database
   *
   * @param tableName TableHandler name
   * @param maxSplitCount how many scanner expected
   * @param options scan options like batchSize
   * @return scanners, count of which would be no more than maxSplitCount
   * @throws PException throws exception if any error occurs.
   */
  public List<PegasusScannerInterface> getUnorderedScanners(
      String tableName, int maxSplitCount, ScanOptions options) throws PException;
}
