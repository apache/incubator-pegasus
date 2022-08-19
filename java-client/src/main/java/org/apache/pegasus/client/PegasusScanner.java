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

import io.netty.util.concurrent.DefaultPromise;
import io.netty.util.concurrent.Future;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pegasus.apps.filter_type;
import org.apache.pegasus.apps.get_scanner_request;
import org.apache.pegasus.apps.key_value;
import org.apache.pegasus.apps.scan_request;
import org.apache.pegasus.apps.scan_response;
import org.apache.pegasus.base.blob;
import org.apache.pegasus.base.error_code;
import org.apache.pegasus.base.gpid;
import org.apache.pegasus.operator.rrdb_clear_scanner_operator;
import org.apache.pegasus.operator.rrdb_get_scanner_operator;
import org.apache.pegasus.operator.rrdb_scan_operator;
import org.apache.pegasus.rpc.ReplicationException;
import org.apache.pegasus.rpc.Table;
import org.slf4j.Logger;

/**
 * @author shenyuannan
 *     <p>Implementation of {@link PegasusScannerInterface}.
 */
public class PegasusScanner implements PegasusScannerInterface {

  private static final Logger LOGGER = org.slf4j.LoggerFactory.getLogger(PegasusScanner.class);

  private static final blob MIN = new blob(new byte[] {0, 0});
  private static final blob MAX = new blob(new byte[] {-1, -1});
  private static final int CONTEXT_ID_VALID_MIN = 0;
  private static final int CONTEXT_ID_COMPLETED = -1;
  private static final int CONTEXT_ID_NOT_EXIST = -2;

  private final Table table;
  private final blob startKey;
  private final blob stopKey;
  private final ScanOptions options;
  private final gpid[] partitions;
  private final long[] partitionHashes;
  private int partitionIter;

  private gpid gpid;
  private long hash;

  private List<key_value> kvs;
  private int readKvIter;

  private long contextId;

  private final Object promisesLock = new Object();
  private final Deque<DefaultPromise<Pair<Pair<byte[], byte[]>, byte[]>>> promises;
  private boolean rpcRunning;
  // mark whether scan operation encounter error
  private boolean encounterError;
  private boolean rpcFailed;
  Throwable cause;

  private final boolean needCheckHash;
  // whether scan operation got incomplete error
  private boolean incomplete;

  private final boolean fullScan;

  private final Object nextItemLock = new Object();
  private Pair<Pair<byte[], byte[]>, byte[]> nextItem;

  public PegasusScanner(
      Table table,
      gpid[] partitions,
      ScanOptions options,
      long[] partitionHashes,
      boolean needCheckHash,
      boolean fullScan) {
    this(table, partitions, options, MIN, MAX, partitionHashes, needCheckHash, fullScan);
    options.startInclusive = true;
    options.stopInclusive = false;
  }

  public PegasusScanner(
      Table table,
      gpid[] partitions,
      ScanOptions options,
      blob startKey,
      blob stopKey,
      long[] partitionHashes,
      boolean needCheckHash,
      boolean fullScan) {
    this.table = table;
    this.partitionHashes = partitionHashes;
    this.partitions = partitions == null ? new gpid[0] : partitions;
    this.options = options;
    this.startKey = startKey;
    this.stopKey = stopKey;
    this.readKvIter = -1;
    this.contextId = CONTEXT_ID_COMPLETED;
    this.partitionIter = this.partitions.length;
    this.kvs = new ArrayList<>();
    this.promises = new LinkedList<>();
    this.rpcRunning = false;
    this.encounterError = false;
    this.needCheckHash = needCheckHash;
    this.incomplete = false;
    this.fullScan = fullScan;
    this.nextItem = null;
  }

  public boolean hasNext() throws PException {
    synchronized (nextItemLock) {
      if (nextItem != null) {
        return true;
      }
      nextItem = next();
      return nextItem != null;
    }
  }

  public Pair<Pair<byte[], byte[]>, byte[]> next() throws PException {
    synchronized (nextItemLock) {
      if (nextItem != null) {
        Pair<Pair<byte[], byte[]>, byte[]> item = nextItem;
        nextItem = null;
        return item;
      }
    }
    try {
      return asyncNext().get(options.timeoutMillis, TimeUnit.MILLISECONDS);
    } catch (InterruptedException | TimeoutException e) {
      throw new PException(new ReplicationException(error_code.error_types.ERR_TIMEOUT));
    } catch (ExecutionException e) {
      throw new PException(e);
    }
  }

  public Future<Pair<Pair<byte[], byte[]>, byte[]>> asyncNext() {
    final DefaultPromise<Pair<Pair<byte[], byte[]>, byte[]>> promise = table.newPromise();
    synchronized (promisesLock) {
      if (promises.isEmpty()) {
        promises.add(promise);
        asyncNextInternal();
      } else {
        // rpc is running, callback will be executed in the callback of rpc
        promises.add(promise);
      }
    }
    return promise;
  }

  @Override
  public void close() {
    if (contextId >= CONTEXT_ID_VALID_MIN) {
      try {
        rrdb_clear_scanner_operator op =
            new rrdb_clear_scanner_operator(gpid, table.getTableName(), contextId, hash);
        table.operate(op, 0);
      } catch (Throwable e) {
        // ignore
      }
      contextId = CONTEXT_ID_COMPLETED;
    }
    partitionIter = 0;
  }

  private void asyncStartScan() {
    if (rpcRunning) {
      LOGGER.error(
          "scan rpc already be running, encounter logic error, we just abandon this scan, "
              + "tableName({}), appId({})",
          table.getTableName(),
          table.getAppID());
      encounterError = true;
      cause = new PException("scan internal error, rpc is already started");
      return;
    }
    rpcRunning = true;
    get_scanner_request request = new get_scanner_request();
    if (kvs.isEmpty()) {
      request.start_key = startKey;
      request.start_inclusive = options.startInclusive;
    } else {
      request.start_key = kvs.get(kvs.size() - 1).key;
      request.start_inclusive = false;
    }
    request.stop_key = stopKey;
    request.stop_inclusive = options.stopInclusive;
    request.batch_size = options.batchSize;
    request.no_value = options.noValue;
    request.hash_key_filter_type = filter_type.findByValue(options.hashKeyFilterType.getValue());
    request.hash_key_filter_pattern =
        (options.hashKeyFilterPattern == null ? null : new blob(options.hashKeyFilterPattern));
    request.sort_key_filter_type = filter_type.findByValue(options.sortKeyFilterType.getValue());
    request.sort_key_filter_pattern =
        (options.sortKeyFilterPattern == null ? null : new blob(options.sortKeyFilterPattern));
    request.need_check_hash = needCheckHash;
    request.full_scan = fullScan;

    rrdb_get_scanner_operator op =
        new rrdb_get_scanner_operator(gpid, table.getTableName(), request, hash);
    Table.ClientOPCallback callback =
        clientOP -> {
          if (!(clientOP instanceof rrdb_get_scanner_operator)) {
            LOGGER.error(
                "scan rpc callback, encounter logic error, we just abandon this scan, "
                    + "tableName({}), appId({})",
                table.getTableName(),
                table.getAppID());
            encounterError = true;
            cause = new PException("scan internal error, rpc callback error");
            return;
          }
          rrdb_get_scanner_operator op1 = (rrdb_get_scanner_operator) (clientOP);
          scan_response response = op1.get_response();
          synchronized (promisesLock) {
            onRecvRpcResponse(op1.rpc_error, response);
            asyncNextInternal();
          }
        };
    table.asyncOperate(op, callback, options.timeoutMillis);
  }

  private void asyncNextBatch() {
    if (rpcRunning) {
      LOGGER.error(
          "scan rpc already be running, encounter logic error, we just abandon this scan, "
              + "tableName({}), appId({})",
          table.getTableName(),
          table.getAppID());
      encounterError = true;
      cause = new PException("scan internal error, rpc is already started");
      return;
    }
    rpcRunning = true;
    scan_request request = new scan_request(contextId);
    rrdb_scan_operator op = new rrdb_scan_operator(gpid, table.getTableName(), request, hash);
    Table.ClientOPCallback callback =
        clientOP -> {
          if (!(clientOP instanceof rrdb_scan_operator)) {
            LOGGER.error(
                "scan rpc callback, encounter logic error, we just abandon this scan, "
                    + "tableName({}), appId({})",
                table.getTableName(),
                table.getAppID());
            encounterError = true;
            cause = new PException("scan internal error, rpc callback error");
            return;
          }
          rrdb_scan_operator op1 = (rrdb_scan_operator) (clientOP);
          scan_response response = op1.get_response();
          synchronized (promisesLock) {
            onRecvRpcResponse(op1.rpc_error, response);
            asyncNextInternal();
          }
        };
    table.asyncOperate(op, callback, options.timeoutMillis);
  }

  private void onRecvRpcResponse(error_code err, scan_response response) {
    if (!rpcRunning) {
      LOGGER.error(
          "scan rpc haven't been started, encounter logic error, we just abandon this scan, "
              + "tableName({}), appId({})",
          table.getTableName(),
          table.getAppID());
      encounterError = true;
      cause = new PException("scan internal error, rpc haven't been started");
      return;
    }
    rpcRunning = false;

    if (err.errno == error_code.error_types.ERR_OK) {
      if (response.error == 0) { // ERR_OK
        kvs = response.kvs;
        readKvIter = -1;
        contextId = response.context_id;
      } else if (response.error
          == 1) { // rocksDB error kNotFound, that scan context has been removed
        contextId = CONTEXT_ID_NOT_EXIST;
      } else if (response.error == 7) { // rocksDB error kIncomplete
        kvs = response.kvs;
        readKvIter = -1;
        contextId = CONTEXT_ID_COMPLETED;
        incomplete = true;
      } else { // rpc succeed, but operation encounter some error in server side
        encounterError = true;
        cause = new PException("rocksDB error: " + response.error);
      }
    } else { // rpc failed
      encounterError = true;
      rpcFailed = true;
      cause = new PException("scan failed with error: " + err.errno);
    }
  }

  private void asyncNextInternal() {
    if (encounterError) {
      for (DefaultPromise<Pair<Pair<byte[], byte[]>, byte[]>> p : promises) {
        p.setFailure(cause);
      }
      promises.clear();
      if (rpcFailed) { // reset _encounterError so that next loop will recall server
        // for read, if error is equal with:
        // - ERR_SESSION_RESET,ERR_OBJECT_NOT_FOUND,ERR_INVALID_STATE: the meta config must have
        // been updated, next loop will use new config and try recover.
        // - ERR_TIMEOUT or other error: meta config not be updated, next loop will only be retry.
        // detail see TableHandler#onRpcReplay
        encounterError = false;
        rpcFailed = false;
      }
      // rpc succeed but still encounter unknown error in server side, not reset _encounterError and
      // abandon the scanner
      return;
    }
    while (!promises.isEmpty()) {
      while (++readKvIter >= kvs.size()) {
        if (contextId == CONTEXT_ID_COMPLETED) {
          // this scan operation got incomplete from server, abandon scan operation
          if (incomplete) {
            for (DefaultPromise<Pair<Pair<byte[], byte[]>, byte[]>> p : promises) {
              LOGGER.error(
                  "scan got incomplete error, " + "tableName({}), {}",
                  table.getTableName(),
                  gpid.toString());
              p.setFailure(new PException("scan got incomplete error, retry later"));
            }
            promises.clear();
            return;
          }

          // reach the end of one partition, finish scan operation
          if (partitionIter <= 0) {
            for (DefaultPromise<Pair<Pair<byte[], byte[]>, byte[]>> p : promises) {
              p.setSuccess(null);
            }
            promises.clear();
            return;
          }

          gpid = partitions[--partitionIter];
          hash = partitionHashes[partitionIter];
          contextReset();
        } else if (contextId == CONTEXT_ID_NOT_EXIST) {
          // no valid context_id found
          asyncStartScan();
          return;
        } else {
          asyncNextBatch();
          return;
        }
      }
      DefaultPromise<Pair<Pair<byte[], byte[]>, byte[]>> p = promises.getFirst();
      p.setSuccess(
          new ImmutablePair<>(
              PegasusClient.restoreKey(kvs.get(readKvIter).key.data),
              kvs.get(readKvIter).value.data));
      promises.removeFirst();
    }
  }

  private void contextReset() {
    kvs.clear();
    readKvIter = -1;
    contextId = CONTEXT_ID_NOT_EXIST;
  }
}
