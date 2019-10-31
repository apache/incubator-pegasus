// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.
package com.xiaomi.infra.pegasus.client;

import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;

/**
 * This class provides sync and async interfaces to access data of a specified table. All the async
 * interfaces use Future mode. Notice that it's {@link io.netty.util.concurrent.Future}, but not
 * {@link java.util.concurrent.Future}. You can wait the future to complete in a synchronous manner,
 * or add completion callback in an asynchronous way.
 *
 * <p>A synchronous example:
 *
 * <pre>{@code
 * PegasusTableInterface table;
 * ....
 * Future<Boolean> future = table.asyncExist(hashKey, sortKey, 0);
 * future.await();
 * if (future.isSuccess()) {
 *     Boolean result = future.getNow();
 * }
 * else {
 *     future.cause().printStackTrace();
 * }
 * }</pre>
 *
 * <p>An asynchronous example:
 *
 * <pre>{@code
 * PegasusTableInterface table;
 * ....
 * table.asyncExist(hashKey, sortKey, 0).addListener(
 *     new ExistListener() {
 *         public void operationComplete(Future<Boolean> future) throws Exception {
 *             if (future.isSuccess()) {
 *                 Boolean result = future.getNow();
 *             }
 *             else {
 *                 future.cause().printStackTrace();
 *             }
 *         }
 *     }
 * ).await();
 * }</pre>
 *
 * <p>Attention: when the future await() returns, it is guaranteed that the result data is ready and
 * you can fetch it by getNow(), but it is not guaranteed that the operationComplete() of listener
 * is already executed, because the callback is dispatched to an internal thread, so it depends on
 * thread scheduling.
 *
 * <p>Please refer to the netty document for the usage of Future.
 */
public interface PegasusTableInterface {

  /// < -------- Exist --------

  public static interface ExistListener extends GenericFutureListener<Future<Boolean>> {
    /**
     * This function will be called when listened asyncExist future is done.
     *
     * @param future the listened future
     * @throws Exception throw exception if any error occurs.
     *     <p>Notice: User shouldn't do any operations that may block or time-consuming
     */
    @Override
    public void operationComplete(Future<Boolean> future) throws Exception;
  }

  /**
   * Check value existence for a specific (hashKey, sortKey) pair of current table, async version
   *
   * @param hashKey used to decide which partition the key may exist if null or length==0, means no
   *     hash key.
   * @param sortKey all keys under the same hashKey will be sorted by sortKey if null or length==0,
   *     means no sort key.
   * @param timeout how long will the operation timeout in milliseconds. if timeout > 0, it is a
   *     timeout value for current op, else the timeout value in the configuration file will be
   *     used.
   * @return A future for current op.
   *     <p>Future return: On success: true if exist, false if not exist On failure: a throwable,
   *     which is an instance of PException
   *     <p>Thread safety: The api is thread safe. All the listeners for the same table are
   *     guaranteed to be dispatched in the same thread, so all the listeners for the same future
   *     are guaranteed to be executed as the same order as the listeners added. But listeners for
   *     different tables are not guaranteed to be dispatched in the same thread.
   */
  public Future<Boolean> asyncExist(byte[] hashKey, byte[] sortKey, int timeout /*ms*/);

  /// < -------- SortKeyCount --------

  public static interface SortKeyCountListener extends GenericFutureListener<Future<Long>> {
    /**
     * This function will be called when listened asyncSortKeyCount future is done.
     *
     * @param future the listened future
     * @throws Exception throw exception if any error occurs.
     *     <p>Notice: User shouldn't do any operations that may block or time-consuming
     */
    @Override
    public void operationComplete(Future<Long> future) throws Exception;
  }

  /**
   * Count the sortkeys for a specific hashKey, async version
   *
   * @param hashKey used to decide which partition the key may exist should not be null or empty
   * @param timeout how long will the operation timeout in milliseconds. if timeout > 0, it is a
   *     timeout value for current op, else the timeout value in the configuration file will be
   *     used.
   * @return the future for current op
   *     <p>Future return: On success: the count result for the hashKey On failure: a throwable,
   *     which is an instance of PException
   *     <p>Thread safety: The api is thread safe. All the listeners for the same table are
   *     guaranteed to be dispatched in the same thread, so all the listeners for the same future
   *     are guaranteed to be executed as the same order as the listeners added. But listeners for
   *     different tables are not guaranteed to be dispatched in the same thread.
   */
  public Future<Long> asyncSortKeyCount(byte[] hashKey, int timeout /*ms*/);

  /// < -------- Get --------

  public static interface GetListener extends GenericFutureListener<Future<byte[]>> {
    /**
     * This function will be called when listened asyncGet future is done.
     *
     * @param future the listened future
     * @throws Exception throw exception if any error occurs.
     *     <p>Notice: User shouldn't do any operations that may block or time-consuming
     */
    @Override
    public void operationComplete(Future<byte[]> future) throws Exception;
  }

  /**
   * Get value for a specific (hashKey, sortKey) pair, async version
   *
   * @param hashKey used to decide which partition the key may exist if null or empty, means no hash
   *     key.
   * @param sortKey all keys under the same hashKey will be sorted by sortKey if null or empty,
   *     means no sort key
   * @param timeout how long will the operation timeout in milliseconds. if timeout > 0, it is a
   *     timeout value for current op, else the timeout value in the configuration file will be
   *     used.
   * @return the future for current op
   *     <p>Future return: On success: the got value On failure: a throwable, which is an instance
   *     of PException
   *     <p>Thread safety: The api is thread safe. All the listeners for the same table are
   *     guaranteed to be dispatched in the same thread, so all the listeners for the same future
   *     are guaranteed to be executed as the same order as the listeners added. But listeners for
   *     different tables are not guaranteed to be dispatched in the same thread.
   */
  public Future<byte[]> asyncGet(byte[] hashKey, byte[] sortKey, int timeout /*ms*/);

  /// < -------- MultiGet --------

  public static class MultiGetResult {
    /**
     * return value for multiGet
     *
     * @param allFetched true if all data on the server are fetched; false if only partial data are
     *     fetched.
     * @param values the got values. if sortKey in the input sortKeys is not found, it won't be in
     *     values. if sortKeys is null or empty, then the returned values will be ascending ordered
     *     by sortKey.
     */
    public boolean allFetched;

    public List<Pair<byte[], byte[]>> values;
  }

  public static interface MultiGetListener extends GenericFutureListener<Future<MultiGetResult>> {
    /**
     * This function will be called when listened asyncMultiGet future is done.
     *
     * @param future the listened future
     * @throws Exception throw exception if any error occurs.
     *     <p>Notice: User shouldn't do any operations that may block or time-consuming
     */
    @Override
    public void operationComplete(Future<MultiGetResult> future) throws Exception;
  }

  /**
   * get multiple key-values under the same hashKey, async version
   *
   * @param hashKey used to decide which partition the key may exist should not be null or empty.
   * @param sortKeys try to get values of sortKeys under the hashKey if null or empty, try to get
   *     all (sortKey,value) pairs under hashKey
   * @param maxFetchCount max count of kv pairs to be fetched maxFetchCount <= 0 means no limit.
   *     default value is 100
   * @param maxFetchSize max size of kv pairs to be fetched. maxFetchSize <= 0 means no limit.
   *     default value is 1000000.
   * @param timeout how long will the operation timeout in milliseconds. if timeout > 0, it is a
   *     timeout value for current op, else the timeout value in the configuration file will be
   *     used.
   * @return the future for current op
   *     <p>Future return: On success: An object of type MultiGetResult On failure: a throwable,
   *     which is an instance of PException
   *     <p>Thread safety: All the listeners for the same table are guaranteed to be dispatched in
   *     the same thread, so all the listeners for the same future are guaranteed to be executed as
   *     the same order as the listeners added. But listeners for different tables are not
   *     guaranteed to be dispatched in the same thread.
   */
  public Future<MultiGetResult> asyncMultiGet(
      byte[] hashKey,
      List<byte[]> sortKeys,
      int maxFetchCount,
      int maxFetchSize,
      int timeout /*ms*/);

  public Future<MultiGetResult> asyncMultiGet(
      byte[] hashKey, List<byte[]> sortKeys, int timeout /*ms*/);

  /**
   * get multiple key-values under the same hashKey with sortKey range limited, async version
   *
   * @param hashKey used to decide which partition the key may exist should not be null or empty.
   * @param startSortKey the start sort key. null means "".
   * @param stopSortKey the stop sort key. null or "" means fetch to the last sort key.
   * @param options multi-get options.
   * @param maxFetchCount max count of kv pairs to be fetched maxFetchCount <= 0 means no limit.
   *     default value is 100
   * @param maxFetchSize max size of kv pairs to be fetched. maxFetchSize <= 0 means no limit.
   *     default value is 1000000.
   * @param timeout how long will the operation timeout in milliseconds. if timeout > 0, it is a
   *     timeout value for current op, else the timeout value in the configuration file will be
   *     used.
   * @return the future for current op
   *     <p>Future return: On success: An object of type MultiGetResult On failure: a throwable,
   *     which is an instance of PException
   *     <p>Thread safety: All the listeners for the same table are guaranteed to be dispatched in
   *     the same thread, so all the listeners for the same future are guaranteed to be executed as
   *     the same order as the listeners added. But listeners for different tables are not
   *     guaranteed to be dispatched in the same thread.
   */
  public Future<MultiGetResult> asyncMultiGet(
      byte[] hashKey,
      byte[] startSortKey,
      byte[] stopSortKey,
      MultiGetOptions options,
      int maxFetchCount,
      int maxFetchSize,
      int timeout /*ms*/);

  public Future<MultiGetResult> asyncMultiGet(
      byte[] hashKey,
      byte[] startSortKey,
      byte[] stopSortKey,
      MultiGetOptions options,
      int timeout /*ms*/);

  /// < -------- MultiGetSortKeys --------

  public static class MultiGetSortKeysResult {
    /**
     * return value for multiGetSortkeys
     *
     * @param allFetched true if all data on the server are fetched; false if only partial data are
     *     fetched.
     * @param keys the got keys. The output keys are in order.
     */
    public boolean allFetched;

    public List<byte[]> keys;
  }

  public static interface MultiGetSortKeysListener
      extends GenericFutureListener<Future<MultiGetSortKeysResult>> {
    /**
     * This function will be called when listened asyncMultiGetSortKeys future is done.
     *
     * @param future the listened future
     * @throws Exception throw exception if any error occurs.
     *     <p>Notice: User shouldn't do any operations that may block or time-consuming
     */
    @Override
    public void operationComplete(Future<MultiGetSortKeysResult> future) throws Exception;
  }

  /**
   * get all the sortKeys for the same hashKey
   *
   * @param hashKey used to decide which partition the key may exist should not be null or empty.
   * @param maxFetchCount max count of kv pairs to be fetched maxFetchCount <= 0 means no limit.
   *     default value is 100
   * @param maxFetchSize max size of kv pairs to be fetched. maxFetchSize <= 0 means no limit.
   *     default value is 1000000.
   * @param timeout how long will the operation timeout in milliseconds. if timeout > 0, it is a
   *     timeout value for current op, else the timeout value in the configuration file will be
   *     used.
   * @return the future for current op
   *     <p>Future return: On success: An object of type MultiGetSortKeysResult On failure: a
   *     throwable, which is an instance of PException
   *     <p>Thread safety: All the listeners for the same table are guaranteed to be dispatched in
   *     the same thread, so all the listeners for the same future are guaranteed to be executed as
   *     the same order as the listeners added. But listeners for different tables are not
   *     guaranteed to be dispatched in the same thread.
   */
  public Future<MultiGetSortKeysResult> asyncMultiGetSortKeys(
      byte[] hashKey, int maxFetchCount, int maxFetchSize, int timeout /*ms*/);

  public Future<MultiGetSortKeysResult> asyncMultiGetSortKeys(byte[] hashKey, int timeout /*ms*/);

  /// < -------- Set --------

  public static interface SetListener extends GenericFutureListener<Future<Void>> {
    /**
     * This function will be called when listened asyncSet future is done.
     *
     * @param future the listened future
     * @throws Exception throw exception if any error occurs.
     *     <p>Notice: User shouldn't do any operations that may block or time-consuming
     */
    @Override
    public void operationComplete(Future<Void> future) throws Exception;
  }

  /**
   * Set value for a specific (hashKey, sortKey) pair, async version
   *
   * @param hashKey used to decide which partition the key may exist if null or empty, means no hash
   *     key.
   * @param sortKey all keys under the same hashKey will be sorted by sortKey if null or empty,
   *     means no sort key
   * @param value should not be null
   * @param ttlSeconds time to live in seconds 0 means no ttl, default value is 0
   * @param timeout how long will the operation timeout in milliseconds. if timeout > 0, it is a
   *     timeout value for current op, else the timeout value in the configuration file will be
   *     used.
   * @return the future for current op
   *     <p>Future return: On success: no return On failure: a throwable, which is an instance of
   *     PException
   *     <p>Thread safety: The api is thread safe. All the listeners for the same table are
   *     guaranteed to be dispatched in the same thread, so all the listeners for the same future
   *     are guaranteed to be executed as the same order as the listeners added. But listeners for
   *     different tables are not guaranteed to be dispatched in the same thread.
   */
  public Future<Void> asyncSet(
      byte[] hashKey, byte[] sortKey, byte[] value, int ttlSeconds, int timeout /*ms*/);

  public Future<Void> asyncSet(byte[] hashKey, byte[] sortKey, byte[] value, int timeout /*ms*/);

  /// < -------- MultiGet --------

  public static interface MultiSetListener extends GenericFutureListener<Future<Void>> {
    /**
     * This function will be called when listened asyncMultiSet future is done.
     *
     * @param future the listened future
     * @throws Exception throw exception if any error occurs.
     *     <p>Notice: User shouldn't do any operations that may block or time-consuming
     */
    @Override
    public void operationComplete(Future<Void> future) throws Exception;
  }

  /**
   * Set key-values for a specific hashKey, async version
   *
   * @param hashKey used to decide which partition the key may exist if null or empty, means no hash
   *     key.
   * @param values all (sortKey, value) pairs should not be null or empty
   * @param ttlSeconds time to live in seconds 0 means no ttl, default value is 0
   * @param timeout how long will the operation timeout in milliseconds. if timeout > 0, it is a
   *     timeout value for current op, else the timeout value in the configuration file will be
   *     used.
   * @return the future for current op
   *     <p>Future return: On success: no return On failure: a throwable, which is an instance of
   *     PException
   *     <p>Thread safety: All the listeners for the same table are guaranteed to be dispatched in
   *     the same thread, so all the listeners for the same future are guaranteed to be executed as
   *     the same order as the listeners added. But listeners for different tables are not
   *     guaranteed to be dispatched in the same thread.
   */
  public Future<Void> asyncMultiSet(
      byte[] hashKey, List<Pair<byte[], byte[]>> values, int ttlSeconds, int timeout /*ms*/);

  public Future<Void> asyncMultiSet(
      byte[] hashKey, List<Pair<byte[], byte[]>> values, int timeout /*ms*/);

  /// < -------- Del --------

  public static interface DelListener extends GenericFutureListener<Future<Void>> {
    /**
     * This function will be called when listened asyncDel future is done.
     *
     * @param future the listened future
     * @throws Exception throw exception if any error occurs.
     *     <p>Notice: User shouldn't do any operations that may block or time-consuming
     */
    @Override
    public void operationComplete(Future<Void> future) throws Exception;
  }

  /**
   * delete value for a specific (hashKey, sortKey) pair, async version
   *
   * @param hashKey used to decide which partition the key may exist if null or empty, means no hash
   *     key.
   * @param sortKey all keys under the same hashKey will be sorted by sortKey if null or empty,
   *     means no sort key
   * @param timeout how long will the operation timeout in milliseconds. if timeout > 0, it is a
   *     timeout value for current op, else the timeout value in the configuration file will be
   *     used.
   * @return the future for current op
   *     <p>Future return: On success: no return On failure: a throwable, which is an instance of
   *     PException
   *     <p>Thread safety: All the listeners for the same table are guaranteed to be dispatched in
   *     the same thread, so all the listeners for the same future are guaranteed to be executed as
   *     the same order as the listeners added. But listeners for different tables are not
   *     guaranteed to be dispatched in the same thread.
   */
  public Future<Void> asyncDel(byte[] hashKey, byte[] sortKey, int timeout /*ms*/);

  /// < -------- MultiDel --------

  public static interface MultiDelListener extends GenericFutureListener<Future<Void>> {
    /**
     * This function will be called when listened asyncMultiDel future is done.
     *
     * @param future the listened future
     * @throws Exception throw exception if any error occurs.
     *     <p>Notice: User shouldn't do any operations that may block or time-consuming
     */
    @Override
    public void operationComplete(Future<Void> future) throws Exception;
  }

  /**
   * delete mutiple values for a specific hashKey, async version
   *
   * @param hashKey used to decide which partition the key may exist if null or empty, means no hash
   *     key.
   * @param sortKeys all the sortKeys need to be deleted should not be null or empty
   * @param timeout how long will the operation timeout in milliseconds. if timeout > 0, it is a
   *     timeout value for current op, else the timeout value in the configuration file will be
   *     used.
   * @return the future for current op
   *     <p>Future return: On success: no return On failure: a throwable, which is an instance of
   *     PException
   *     <p>Thread safety: All the listeners for the same table are guaranteed to be dispatched in
   *     the same thread, so all the listeners for the same future are guaranteed to be executed as
   *     the same order as the listeners added. But listeners for different tables are not
   *     guaranteed to be dispatched in the same thread.
   */
  public Future<Void> asyncMultiDel(byte[] hashKey, List<byte[]> sortKeys, int timeout /*ms*/);

  /// < -------- Incr --------

  public static interface IncrListener extends GenericFutureListener<Future<Long>> {
    /**
     * This function will be called when listened asyncIncr future is done.
     *
     * @param future the listened future
     * @throws Exception throw exception if any error occurs.
     *     <p>Notice: User shouldn't do any operations that may block or time-consuming
     */
    @Override
    public void operationComplete(Future<Long> future) throws Exception;
  }

  /**
   * atomically increment value by key, async version
   *
   * @param hashKey the hash key to increment.
   * @param sortKey the sort key to increment.
   * @param increment the increment to be added to the old value.
   * @param ttlSeconds time to live in seconds for the new value. for the second method, the
   *     ttlSeconds is 0. should be no less than -1. for the second method, the ttlSeconds is 0. -
   *     if ttlSeconds == 0, the semantic is the same as redis: - normally, increment will preserve
   *     the original ttl. - if old data is expired by ttl, then set initial value to 0 and set no
   *     ttl. - if ttlSeconds > 0, then update with the new ttl if increment succeed. - if
   *     ttlSeconds == -1, then update to no ttl if increment succeed.
   * @param timeout how long will the operation timeout in milliseconds. if timeout > 0, it is a
   *     timeout value for current op, else the timeout value in the configuration file will be
   *     used.
   * @return the future for current op
   *     <p>Future return: On success: return new value. On failure: a throwable, which is an
   *     instance of PException
   *     <p>Thread safety: All the listeners for the same table are guaranteed to be dispatched in
   *     the same thread, so all the listeners for the same future are guaranteed to be executed as
   *     the same order as the listeners added. But listeners for different tables are not
   *     guaranteed to be dispatched in the same thread.
   */
  public Future<Long> asyncIncr(
      byte[] hashKey, byte[] sortKey, long increment, int ttlSeconds, int timeout /*ms*/);

  public Future<Long> asyncIncr(byte[] hashKey, byte[] sortKey, long increment, int timeout /*ms*/);

  /// < -------- CheckAndSet --------

  public static class CheckAndSetResult {
    /**
     * return value for checkAndSet
     *
     * @param setSucceed true if set value succeed.
     * @param checkValueReturned true if the check value is returned.
     * @param checkValueExist true if the check value is exist; can be used only when
     *     checkValueReturned is true.
     * @param checkValue return the check value if exist; can be used only when checkValueExist is
     *     true.
     */
    public boolean setSucceed;

    public boolean checkValueReturned;
    public boolean checkValueExist;
    public byte[] checkValue;
  }

  public static interface CheckAndSetListener
      extends GenericFutureListener<Future<CheckAndSetResult>> {
    /**
     * This function will be called when listened asyncCheckAndSet future is done.
     *
     * @param future the listened future
     * @throws Exception throw exception if any error occurs.
     *     <p>Notice: User shouldn't do any operations that may block or time-consuming
     */
    @Override
    public void operationComplete(Future<CheckAndSetResult> future) throws Exception;
  }

  /**
   * atomically check and set value by key, async version. if the check condition is satisfied, then
   * apply to set value.
   *
   * @param hashKey the hash key to check and set.
   * @param checkSortKey the sort key to check.
   * @param checkType the check type.
   * @param checkOperand the check operand.
   * @param setSortKey the sort key to set value if check condition is satisfied.
   * @param setValue the value to set if check condition is satisfied.
   * @param options the check-and-set options.
   * @param timeout how long will the operation timeout in milliseconds. if timeout > 0, it is a
   *     timeout value for current op, else the timeout value in the configuration file will be
   *     used.
   * @return the future for current op
   *     <p>Future return: On success: return CheckAndSetResult. On failure: a throwable, which is
   *     an instance of PException
   *     <p>Thread safety: All the listeners for the same table are guaranteed to be dispatched in
   *     the same thread, so all the listeners for the same future are guaranteed to be executed as
   *     the same order as the listeners added. But listeners for different tables are not
   *     guaranteed to be dispatched in the same thread.
   */
  public Future<CheckAndSetResult> asyncCheckAndSet(
      byte[] hashKey,
      byte[] checkSortKey,
      CheckType checkType,
      byte[] checkOperand,
      byte[] setSortKey,
      byte[] setValue,
      CheckAndSetOptions options,
      int timeout /*ms*/);

  /// < -------- CheckAndMutate --------

  class CheckAndMutateResult {
    /**
     * return value for checkAndMutate
     *
     * @param mutateSucceed true if mutate succeed.
     * @param checkValueReturned true if the check value is returned.
     * @param checkValueExist true if the check value is exist; can be used only when
     *     checkValueReturned is true.
     * @param checkValue return the check value if exist; can be used only when checkValueExist is
     *     true.
     */
    public boolean mutateSucceed;

    public boolean checkValueReturned;
    public boolean checkValueExist;
    public byte[] checkValue;
  }

  /**
   * atomically check and mutate by key, async version. if the check condition is satisfied, then
   * apply to mutate.
   *
   * @param hashKey the hash key to check and mutate.
   * @param checkSortKey the sort key to check.
   * @param checkType the check type.
   * @param checkOperand the check operand.
   * @param mutations the list of mutations to perform if check condition is satisfied.
   * @param options the check-and-mutate options.
   * @param timeout how long will the operation timeout in milliseconds. if timeout > 0, it is a
   *     timeout value for current op, else the timeout value in the configuration file will be
   *     used.
   * @return the future for current op
   *     <p>Future return: On success: return CheckAndMutateResult. On failure: a throwable, which
   *     is an instance of PException
   *     <p>Thread safety: All the listeners for the same table are guaranteed to be dispatched in
   *     the same thread, so all the listeners for the same future are guaranteed to be executed as
   *     the same order as the listeners added. But listeners for different tables are not
   *     guaranteed to be dispatched in the same thread.
   */
  Future<CheckAndMutateResult> asyncCheckAndMutate(
      byte[] hashKey,
      byte[] checkSortKey,
      CheckType checkType,
      byte[] checkOperand,
      Mutations mutations,
      CheckAndMutateOptions options,
      int timeout /*ms*/);

  /// < -------- CompareExchange --------

  public static class CompareExchangeResult {
    /**
     * return value for CompareExchange
     *
     * @param setSucceed true if set value succeed.
     * @param actualValue return the actual value if set value failed; null means the actual value
     *     is not exist.
     */
    public boolean setSucceed;

    public byte[] actualValue;
  }

  public static interface CompareExchangeListener
      extends GenericFutureListener<Future<CompareExchangeResult>> {
    /**
     * This function will be called when listened asyncCompareExchange future is done.
     *
     * @param future the listened future
     * @throws Exception throw exception if any error occurs.
     *     <p>Notice: User shouldn't do any operations that may block or time-consuming
     */
    @Override
    public void operationComplete(Future<CompareExchangeResult> future) throws Exception;
  }

  /**
   * atomically compare and exchange value by key, async version.
   *
   * <p>- if the original value for the key is equal to the expected value, then update it with the
   * desired value, set CompareExchangeResult.setSucceed to true, and set
   * CompareExchangeResult.actualValue to null because the actual value must be equal to the desired
   * value. - if the original value for the key is not exist or not equal to the expected value,
   * then set CompareExchangeResult.setSucceed to false, and set the actual value in
   * CompareExchangeResult.actualValue.
   *
   * <p>this method is very like the C++ function in
   * {https://en.cppreference.com/w/cpp/atomic/atomic_compare_exchange}.
   *
   * @param hashKey the hash key to compare and exchange.
   * @param sortKey the sort key to compare and exchange.
   * @param expectedValue the value expected to be found for the key.
   * @param desiredValue the desired value to set if the original value for the key is equal to the
   *     expected value.
   * @param ttlSeconds time to live in seconds of the desired value, 0 means no ttl.
   * @param timeout how long will the operation timeout in milliseconds. if timeout > 0, it is a
   *     timeout value for current op, else the timeout value in the configuration file will be
   *     used.
   * @return the future for current op
   *     <p>Future return: On success: return CompareExchangeResult. On failure: a throwable, which
   *     is an instance of PException
   *     <p>Thread safety: All the listeners for the same table are guaranteed to be dispatched in
   *     the same thread, so all the listeners for the same future are guaranteed to be executed as
   *     the same order as the listeners added. But listeners for different tables are not
   *     guaranteed to be dispatched in the same thread.
   */
  public Future<CompareExchangeResult> asyncCompareExchange(
      byte[] hashKey,
      byte[] sortKey,
      byte[] expectedValue,
      byte[] desiredValue,
      int ttlSeconds,
      int timeout /*ms*/);

  /// < -------- TTL --------

  public static interface TTLListener extends GenericFutureListener<Future<Integer>> {
    /**
     * This function will be called when listened asyncTTL future is done.
     *
     * @param future the listened future
     * @throws Exception throw exception if any error occurs.
     *     <p>Notice: User shouldn't do any operations that may block or time-consuming
     */
    @Override
    public void operationComplete(Future<Integer> future) throws Exception;
  }

  /**
   * get TTL value for a specific (hashKey, sortKey) pair, async version
   *
   * @param hashKey used to decide which partition the key may exist if null or empty, means no hash
   *     key.
   * @param sortKey all keys under the same hashKey will be sorted by sortKey if null or empty,
   *     means no sort key
   * @param timeout how long will the operation timeout in milliseconds. if timeout > 0, it is a
   *     timeout value for current op, else the timeout value in the configuration file will be
   *     used.
   * @return the future for current op
   *     <p>Future return: On success: ttl time in seconds; -1 if no ttl set; -2 if not exist. On
   *     failure: a throwable, which is an instance of PException
   *     <p>Thread safety: All the listeners for the same table are guaranteed to be dispatched in
   *     the same thread, so all the listeners for the same future are guaranteed to be executed as
   *     the same order as the listeners added. But listeners for different tables are not
   *     guaranteed to be dispatched in the same thread.
   */
  public Future<Integer> asyncTTL(byte[] hashKey, byte[] sortKey, int timeout /*ms*/);

  /// < -------- Sync Methods --------

  /**
   * sync version of Exist, please refer to the async version {@link #asyncExist(byte[], byte[],
   * int)}
   */
  public boolean exist(byte[] hashKey, byte[] sortKey, int timeout /*ms*/) throws PException;

  /**
   * sync version of SortKeyCount, please refer to the async version {@link
   * #asyncSortKeyCount(byte[], int)}
   */
  public long sortKeyCount(byte[] hashKey, int timeout /*ms*/) throws PException;

  /**
   * sync version of Get, please refer to the async version {@link #asyncGet(byte[], byte[], int)}
   */
  public byte[] get(byte[] hashKey, byte[] sortKey, int timeout /*ms*/) throws PException;

  /**
   * Batch get values of different keys. Will terminate immediately if any error occurs.
   *
   * @param keys hashKey and sortKey pair list.
   * @param values output values; should be created by caller; if succeed, the size of values will
   *     be same with keys; the value of keys[i] is stored in values[i]; if the value of keys[i] is
   *     not found, then values[i] will be set to null.
   * @param timeout how long will the operation timeout in milliseconds. if timeout > 0, it is a
   *     timeout value for current op, else the timeout value in the configuration file will be
   *     used.
   * @throws PException throws exception if any error occurs.
   *     <p>Notice: the method is not atomic, that means, maybe some keys succeed but some keys
   *     failed.
   */
  public void batchGet(List<Pair<byte[], byte[]>> keys, List<byte[]> values, int timeout /*ms*/)
      throws PException;

  /**
   * Batch get values of different keys. Will wait for all requests done even if some error occurs.
   *
   * @param keys hashKey and sortKey pair list.
   * @param results output results; should be created by caller; after call done, the size of
   *     results will be same with keys; the results[i] is a Pair: - if Pair.left != null : means
   *     query keys[i] failed, Pair.left is the exception. - if Pair.left == null : means query
   *     keys[i] succeed, Pair.right is the result value.
   * @param timeout how long will the operation timeout in milliseconds. if timeout > 0, it is a
   *     timeout value for current op, else the timeout value in the configuration file will be
   *     used.
   * @return succeed count.
   * @throws PException throw exception if any error occurs.
   *     <p>Notice: the method is not atomic, that means, maybe some keys succeed but some keys
   *     failed.
   */
  public int batchGet2(
      List<Pair<byte[], byte[]>> keys, List<Pair<PException, byte[]>> results, int timeout /*ms*/)
      throws PException;

  /**
   * sync version of MultiGet, please refer to the async version {@link #asyncMultiGet(byte[], List,
   * int, int, int)} and {@link #asyncMultiGet(byte[], List, int)}
   */
  public MultiGetResult multiGet(
      byte[] hashKey,
      List<byte[]> sortKeys,
      int maxFetchCount,
      int maxFetchSize,
      int timeout /*ms*/)
      throws PException;

  public MultiGetResult multiGet(byte[] hashKey, List<byte[]> sortKeys, int timeout /*ms*/)
      throws PException;

  /**
   * sync version of MultiGet, please refer to the async version {@link #asyncMultiGet(byte[],
   * byte[], byte[], MultiGetOptions, int, int, int)} and {@link #asyncMultiGet(byte[], byte[],
   * byte[], MultiGetOptions, int)}
   */
  public MultiGetResult multiGet(
      byte[] hashKey,
      byte[] startSortKey,
      byte[] stopSortKey,
      MultiGetOptions options,
      int maxFetchCount,
      int maxFetchSize,
      int timeout /*ms*/)
      throws PException;

  public MultiGetResult multiGet(
      byte[] hashKey,
      byte[] startSortKey,
      byte[] stopSortKey,
      MultiGetOptions options,
      int timeout /*ms*/)
      throws PException;

  /**
   * Batch get multiple values under the same hash key. Will terminate immediately if any error
   * occurs.
   *
   * @param keys List{hashKey,List{sortKey}}
   * @param values output values; should be created by caller; if succeed, the size of values will
   *     be same with keys; the data for keys[i] is stored in values[i].
   * @param timeout how long will the operation timeout in milliseconds. if timeout > 0, it is a
   *     timeout value for current op, else the timeout value in the configuration file will be
   *     used.
   * @throws PException throws exception if any error occurs.
   *     <p>Notice: the method is not atomic, that means, maybe some keys succeed but some keys
   *     failed.
   */
  public void batchMultiGet(
      List<Pair<byte[], List<byte[]>>> keys, List<HashKeyData> values, int timeout /*ms*/)
      throws PException;

  /**
   * Batch get multiple values under the same hash key. Will wait for all requests done even if some
   * error occurs.
   *
   * @param keys List{hashKey,List{sortKey}}; if List{sortKey} is null or empty, means fetch all
   *     sortKeys under the hashKey.
   * @param results output results; should be created by caller; after call done, the size of
   *     results will be same with keys; the results[i] is a Pair: - if Pair.left != null : means
   *     query keys[i] failed, Pair.left is the exception. - if Pair.left == null : means query
   *     keys[i] succeed, Pair.right is the result value.
   * @return succeed count.
   * @throws PException throw exception if any error occurs.
   *     <p>Notice: the method is not atomic, that means, maybe some keys succeed but some keys
   *     failed.
   */
  public int batchMultiGet2(
      List<Pair<byte[], List<byte[]>>> keys,
      List<Pair<PException, HashKeyData>> results,
      int timeout /*ms*/)
      throws PException;

  /**
   * sync version of MultiGetSortKeys, please refer to the async version {@link
   * #asyncMultiGetSortKeys(byte[], int, int, int)} and {@link #asyncMultiGetSortKeys(byte[], int)}
   */
  public MultiGetSortKeysResult multiGetSortKeys(
      byte[] hashKey, int maxFetchCount, int maxFetchSize, int timeout /*ms*/) throws PException;

  public MultiGetSortKeysResult multiGetSortKeys(byte[] hashKey, int timeout /*ms*/)
      throws PException;

  /**
   * sync version of Set, please refer to the async version {@link #asyncSet(byte[], byte[], byte[],
   * int, int)} and {@link #asyncSet(byte[], byte[], byte[], int)}
   */
  public void set(byte[] hashKey, byte[] sortKey, byte[] value, int ttlSeconds, int timeout /*ms*/)
      throws PException;

  public void set(byte[] hashKey, byte[] sortKey, byte[] value, int timeout /*ms*/)
      throws PException;

  /**
   * Batch set lots of values. Will terminate immediately if any error occurs.
   *
   * @param items list of items.
   * @param timeout how long will the operation timeout in milliseconds. if timeout > 0, it is a
   *     timeout value for current op, else the timeout value in the configuration file will be
   *     used.
   * @throws PException throws exception if any error occurs.
   *     <p>Notice: the method is not atomic, that means, maybe some keys succeed but some keys
   *     failed.
   */
  public void batchSet(List<SetItem> items, int timeout /*ms*/) throws PException;

  /**
   * Batch set lots of values. Will wait for all requests done even if some error occurs.
   *
   * @param items list of items.
   * @param results output results; should be created by caller; after call done, the size of
   *     results will be same with items; the results[i] is a PException: - if results[i] != null :
   *     means set items[i] failed, results[i] is the exception. - if results[i] == null : means set
   *     items[i] succeed.
   * @param timeout how long will the operation timeout in milliseconds. if timeout > 0, it is a
   *     timeout value for current op, else the timeout value in the configuration file will be
   *     used.
   * @return succeed count.
   * @throws PException throw exception if any error occurs.
   *     <p>Notice: the method is not atomic, that means, maybe some keys succeed but some keys
   *     failed.
   */
  public int batchSet2(List<SetItem> items, List<PException> results, int timeout /*ms*/)
      throws PException;

  /**
   * sync version of MultiSet, please refer to the async version {@link #asyncMultiSet(byte[], List,
   * int, int)} and {@link #asyncMultiSet(byte[], List, int)}
   */
  public void multiSet(
      byte[] hashKey, List<Pair<byte[], byte[]>> values, int ttlSeconds, int timeout /*ms*/)
      throws PException;

  public void multiSet(byte[] hashKey, List<Pair<byte[], byte[]>> values, int timeout /*ms*/)
      throws PException;

  /**
   * Batch set multiple value under the same hash key. Will terminate immediately if any error
   * occurs.
   *
   * @param items list of items.
   * @param ttlSeconds time to live in seconds, 0 means no ttl.
   * @param timeout how long will the operation timeout in milliseconds. if timeout > 0, it is a
   *     timeout value for current op, else the timeout value in the configuration file will be
   *     used.
   * @throws PException throws exception if any error occurs.
   *     <p>Notice: the method is not atomic, that means, maybe some keys succeed but some keys
   *     failed.
   */
  public void batchMultiSet(List<HashKeyData> items, int ttlSeconds, int timeout /*ms*/)
      throws PException;

  /**
   * Batch set multiple value under the same hash key. Will wait for all requests done even if some
   * error occurs.
   *
   * @param items list of items.
   * @param ttlSeconds time to live in seconds, 0 means no ttl. default value is 0.
   * @param results output results; should be created by caller; after call done, the size of
   *     results will be same with items; the results[i] is a PException: - if results[i] != null :
   *     means set items[i] failed, results[i] is the exception. - if results[i] == null : means set
   *     items[i] succeed.
   * @param timeout how long will the operation timeout in milliseconds. if timeout > 0, it is a
   *     timeout value for current op, else the timeout value in the configuration file will be
   *     used.
   * @return succeed count.
   * @throws PException throw exception if any error occurs.
   *     <p>Notice: the method is not atomic, that means, maybe some keys succeed but some keys
   *     failed.
   */
  public int batchMultiSet2(
      List<HashKeyData> items, int ttlSeconds, List<PException> results, int timeout /*ms*/)
      throws PException;

  /**
   * sync version of Del, please refer to the async version {@link #asyncDel(byte[], byte[], int)}
   */
  public void del(byte[] hashKey, byte[] sortKey, int timeout /*ms*/) throws PException;

  /**
   * Batch delete values of different keys. Will terminate immediately if any error occurs.
   *
   * @param keys hashKey and sortKey pair list.
   * @param timeout how long will the operation timeout in milliseconds. if timeout > 0, it is a
   *     timeout value for current op, else the timeout value in the configuration file will be
   *     used.
   * @throws PException throws exception if any error occurs.
   *     <p>Notice: the method is not atomic, that means, maybe some keys succeed but some keys
   *     failed.
   */
  public void batchDel(List<Pair<byte[], byte[]>> keys, int timeout /*ms*/) throws PException;

  /**
   * Batch delete values of different keys. Will wait for all requests done even if some error
   * occurs.
   *
   * @param keys hashKey and sortKey pair list.
   * @param results output results; should be created by caller; after call done, the size of
   *     results will be same with keys; the results[i] is a PException: - if results[i] != null :
   *     means del keys[i] failed, results[i] is the exception. - if results[i] == null : means del
   *     keys[i] succeed.
   * @param timeout how long will the operation timeout in milliseconds. if timeout > 0, it is a
   *     timeout value for current op, else the timeout value in the configuration file will be
   *     used.
   * @return succeed count.
   * @throws PException throw exception if any error occurs.
   *     <p>Notice: the method is not atomic, that means, maybe some keys succeed but some keys
   *     failed.
   */
  public int batchDel2(
      List<Pair<byte[], byte[]>> keys, List<PException> results, int timeout /*ms*/)
      throws PException;

  /**
   * sync version of MultiDel, please refer to the async version {@link #asyncMultiDel(byte[], List,
   * int)}
   */
  public void multiDel(byte[] hashKey, List<byte[]> sortKeys, int timeout /*ms*/) throws PException;

  /**
   * Delete key-values within range of startSortKey and stopSortKey under hashKey. Will terminate
   * immediately if any error occurs.
   *
   * @param hashKey used to decide which partition the key may exist should not be null or empty.
   * @param startSortKey the start sort key. null means "".
   * @param stopSortKey the stop sort key. null or "" means fetch to the last sort key.
   * @param options del range options.
   * @param timeout how long will the operation timeout in milliseconds. if timeout > 0, it is a
   *     timeout value for current op, else the timeout value in the configuration file will be
   *     used.
   * @throws PException throws exception if any error occurs.
   */
  public void delRange(
      byte[] hashKey,
      byte[] startSortKey,
      byte[] stopSortKey,
      DelRangeOptions options,
      int timeout /*ms*/)
      throws PException;

  /**
   * Batch delete specified sort keys under the same hash key. Will terminate immediately if any
   * error occurs.
   *
   * @param keys List{hashKey,List{sortKey}}
   * @param timeout how long will the operation timeout in milliseconds. if timeout > 0, it is a
   *     timeout value for current op, else the timeout value in the configuration file will be
   *     used.
   * @throws PException throws exception if any error occurs.
   *     <p>Notice: the method is not atomic, that means, maybe some keys succeed but some keys
   *     failed.
   */
  public void batchMultiDel(List<Pair<byte[], List<byte[]>>> keys, int timeout /*ms*/)
      throws PException;

  /**
   * Batch delete specified sort keys under the same hash key. Will wait for all requests done even
   * if some error occurs.
   *
   * @param keys List{hashKey,List{sortKey}}
   * @param results output results; should be created by caller; after call done, the size of
   *     results will be same with keys; the results[i] is a PException: - if results[i] != null :
   *     means del keys[i] failed, results[i] is the exception. - if results[i] == null : means del
   *     keys[i] succeed.
   * @param timeout how long will the operation timeout in milliseconds. if timeout > 0, it is a
   *     timeout value for current op, else the timeout value in the configuration file will be
   *     used.
   * @return succeed count.
   * @throws PException throw exception if any error occurs.
   *     <p>Notice: the method is not atomic, that means, maybe some keys succeed but some keys
   *     failed.
   */
  public int batchMultiDel2(
      List<Pair<byte[], List<byte[]>>> keys, List<PException> results, int timeout /*ms*/)
      throws PException;

  /**
   * sync version of Incr, please refer to the async version {@link #asyncIncr(byte[], byte[], long,
   * int, int)}
   */
  public long incr(
      byte[] hashKey, byte[] sortKey, long increment, int ttlSeconds, int timeout /*ms*/)
      throws PException;

  /**
   * sync version of Incr, please refer to the async version {@link #asyncIncr(byte[], byte[], long,
   * int)}
   */
  public long incr(byte[] hashKey, byte[] sortKey, long increment, int timeout /*ms*/)
      throws PException;

  /**
   * sync version of CheckAndSet, please refer to the async version {@link #asyncCheckAndSet(byte[],
   * byte[], CheckType, byte[], byte[], byte[], CheckAndSetOptions, int)}
   */
  public CheckAndSetResult checkAndSet(
      byte[] hashKey,
      byte[] checkSortKey,
      CheckType checkType,
      byte[] checkOperand,
      byte[] setSortKey,
      byte[] setValue,
      CheckAndSetOptions options,
      int timeout /*ms*/)
      throws PException;

  /**
   * sync version of CheckAndMutate, please refer to the async version {@link
   * #asyncCheckAndMutate(byte[], byte[], CheckType, byte[], Mutations, CheckAndMutateOptions, int)}
   */
  public CheckAndMutateResult checkAndMutate(
      byte[] hashKey,
      byte[] checkSortKey,
      CheckType checkType,
      byte[] checkOperand,
      Mutations mutations,
      CheckAndMutateOptions options,
      int timeout)
      throws PException;

  /**
   * sync version of CompareExchange, please refer to the async version {@link
   * #asyncCompareExchange(byte[], byte[], byte[], byte[], int, int)}
   */
  public CompareExchangeResult compareExchange(
      byte[] hashKey,
      byte[] sortKey,
      byte[] expectedValue,
      byte[] desiredValue,
      int ttlSeconds,
      int timeout /*ms*/)
      throws PException;

  /**
   * sync version of TTL, please refer to the async version {@link #asyncTTL(byte[], byte[], int)}
   */
  public int ttl(byte[] hashKey, byte[] sortKey, int timeout /*ms*/) throws PException;

  /**
   * Get Scanner for {startSortKey, stopSortKey} within hashKey
   *
   * @param hashKey used to decide which partition to put this k-v,
   * @param startSortKey start sort key scan from if null or length == 0, means start from begin
   * @param stopSortKey stop sort key scan to if null or length == 0, means stop to end
   * @param options scan options like endpoint inclusive/exclusive
   * @return scanner
   * @throws PException throw exception if any error occurs.
   */
  public PegasusScannerInterface getScanner(
      byte[] hashKey, byte[] startSortKey, byte[] stopSortKey, ScanOptions options)
      throws PException;

  /**
   * Get Scanners for all data in database
   *
   * @param maxSplitCount how many scanner expected
   * @param options scan options like batchSize
   * @return scanners, count of which would be no more than maxSplitCount
   * @throws PException throw exception if any error occurs.
   */
  public List<PegasusScannerInterface> getUnorderedScanners(int maxSplitCount, ScanOptions options)
      throws PException;
}
