// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.
package com.xiaomi.infra.pegasus.client;

import com.xiaomi.infra.pegasus.apps.*;
import com.xiaomi.infra.pegasus.base.blob;
import com.xiaomi.infra.pegasus.base.error_code;
import com.xiaomi.infra.pegasus.base.gpid;
import com.xiaomi.infra.pegasus.operator.client_operator;
import com.xiaomi.infra.pegasus.operator.rrdb_clear_scanner_operator;
import com.xiaomi.infra.pegasus.operator.rrdb_get_scanner_operator;
import com.xiaomi.infra.pegasus.operator.rrdb_scan_operator;
import com.xiaomi.infra.pegasus.rpc.ReplicationException;
import com.xiaomi.infra.pegasus.rpc.Table;
import io.netty.util.concurrent.*;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;

/**
 * @author shenyuannan
 *     <p>Implementation of {@link PegasusScannerInterface}.
 */
public class PegasusScanner implements PegasusScannerInterface {
  private static final blob min = new blob(new byte[] {0, 0});
  private static final blob max = new blob(new byte[] {-1, -1});
  private static final int CONTEXT_ID_VALID_MIN = 0;
  private static final int CONTEXT_ID_COMPLETED = -1;
  private static final int CONTEXT_ID_NOT_EXIST = -2;

  public PegasusScanner(
      Table table,
      gpid[] partitions,
      ScanOptions options,
      long[] partitionHashes,
      boolean needCheckHash) {
    this(table, partitions, options, min, max, partitionHashes, needCheckHash);
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
      boolean needCheckHash) {
    _table = table;
    _partitionHashes = partitionHashes;
    _partitions = partitions == null ? new gpid[0] : partitions;
    _options = options;
    _startKey = startKey;
    _stopKey = stopKey;
    _readKvIter = -1;
    _contextId = CONTEXT_ID_COMPLETED;
    _partitionIter = _partitions.length;
    _kvs = new ArrayList<key_value>();
    _promises = new LinkedList<DefaultPromise<Pair<Pair<byte[], byte[]>, byte[]>>>();
    _rpcRunning = false;
    _encounterError = false;
    _needCheckHash = needCheckHash;
    _incomplete = false;
  }

  public Pair<Pair<byte[], byte[]>, byte[]> next() throws PException {
    try {
      return asyncNext().get(_options.timeoutMillis, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      throw new PException(new ReplicationException(error_code.error_types.ERR_TIMEOUT));
    } catch (TimeoutException e) {
      throw new PException(new ReplicationException(error_code.error_types.ERR_TIMEOUT));
    } catch (ExecutionException e) {
      throw new PException(e);
    }
  }

  public Future<Pair<Pair<byte[], byte[]>, byte[]>> asyncNext() {
    final DefaultPromise<Pair<Pair<byte[], byte[]>, byte[]>> promise = _table.newPromise();
    synchronized (_promisesLock) {
      if (_promises.isEmpty()) {
        _promises.add(promise);
        asyncNextInternal();
      } else {
        // rpc is running, callback will be executed in the callback of rpc
        _promises.add(promise);
      }
    }
    return promise;
  }

  @Override
  public void close() {
    if (_contextId >= CONTEXT_ID_VALID_MIN) {
      try {
        rrdb_clear_scanner_operator op =
            new rrdb_clear_scanner_operator(_gpid, _table.getTableName(), _contextId, _hash);
        _table.operate(op, 0);
      } catch (Throwable e) {
        // ignore
      }
      _contextId = CONTEXT_ID_COMPLETED;
    }
    _partitionIter = 0;
  }

  private void asyncStartScan() {
    if (_rpcRunning) {
      logger.error(
          "scan rpc already be running, encounter logic error, we just abandon this scan, "
              + "tableName({}), appId({})",
          _table.getTableName(),
          _table.getAppID());
      _encounterError = true;
      _cause = new PException("scan internal error, rpc is already started");
      return;
    }
    _rpcRunning = true;
    get_scanner_request request = new get_scanner_request();
    if (_kvs.isEmpty()) {
      request.start_key = _startKey;
      request.start_inclusive = _options.startInclusive;
    } else {
      request.start_key = _kvs.get(_kvs.size() - 1).key;
      request.start_inclusive = false;
    }
    request.stop_key = _stopKey;
    request.stop_inclusive = _options.stopInclusive;
    request.batch_size = _options.batchSize;
    request.no_value = _options.noValue;
    request.hash_key_filter_type = filter_type.findByValue(_options.hashKeyFilterType.getValue());
    request.hash_key_filter_pattern =
        (_options.hashKeyFilterPattern == null ? null : new blob(_options.hashKeyFilterPattern));
    request.sort_key_filter_type = filter_type.findByValue(_options.sortKeyFilterType.getValue());
    request.sort_key_filter_pattern =
        (_options.sortKeyFilterPattern == null ? null : new blob(_options.sortKeyFilterPattern));
    request.need_check_hash = _needCheckHash;

    rrdb_get_scanner_operator op =
        new rrdb_get_scanner_operator(_gpid, _table.getTableName(), request, _hash);
    Table.ClientOPCallback callback =
        new Table.ClientOPCallback() {
          @Override
          public void onCompletion(client_operator clientOP) throws Throwable {
            rrdb_get_scanner_operator op = (rrdb_get_scanner_operator) (clientOP);
            scan_response response = op.get_response();
            synchronized (_promisesLock) {
              onRecvRpcResponse(op.rpc_error, response);
              asyncNextInternal();
            }
          }
        };
    _table.asyncOperate(op, callback, _options.timeoutMillis);
  }

  private void asyncNextBatch() {
    if (_rpcRunning) {
      logger.error(
          "scan rpc already be running, encounter logic error, we just abandon this scan, "
              + "tableName({}), appId({})",
          _table.getTableName(),
          _table.getAppID());
      _encounterError = true;
      _cause = new PException("scan internal error, rpc is already started");
      return;
    }
    _rpcRunning = true;
    scan_request request = new scan_request(_contextId);
    rrdb_scan_operator op = new rrdb_scan_operator(_gpid, _table.getTableName(), request, _hash);
    Table.ClientOPCallback callback =
        new Table.ClientOPCallback() {
          @Override
          public void onCompletion(client_operator clientOP) throws Throwable {
            rrdb_scan_operator op = (rrdb_scan_operator) (clientOP);
            scan_response response = op.get_response();
            synchronized (_promisesLock) {
              onRecvRpcResponse(op.rpc_error, response);
              asyncNextInternal();
            }
          }
        };
    _table.asyncOperate(op, callback, _options.timeoutMillis);
  }

  private void onRecvRpcResponse(error_code err, scan_response response) {
    if (!_rpcRunning) {
      logger.error(
          "scan rpc haven't been started, encounter logic error, we just abandon this scan, "
              + "tableName({}), appId({})",
          _table.getTableName(),
          _table.getAppID());
      _encounterError = true;
      _cause = new PException("scan internal error, rpc haven't been started");
      return;
    }
    _rpcRunning = false;

    if (err.errno == error_code.error_types.ERR_OK) {
      if (response.error == 0) { // ERR_OK
        _kvs = response.kvs;
        _readKvIter = -1;
        _contextId = response.context_id;
      } else if (response.error
          == 1) { // rocksDB error kNotFound, that scan context has been removed
        _contextId = CONTEXT_ID_NOT_EXIST;
      } else if (response.error == 7) { // rocksDB error kIncomplete
        _kvs = response.kvs;
        _readKvIter = -1;
        _contextId = CONTEXT_ID_COMPLETED;
        _incomplete = true;
      } else { // rpc succeed, but operation encounter some error in server side
        _encounterError = true;
        _cause = new PException("rocksDB error: " + response.error);
      }
    } else { // rpc failed
      _encounterError = true;
      _cause = new PException("scan failed with error: " + err.errno);
    }
  }

  private void asyncNextInternal() {
    if (_encounterError) {
      for (DefaultPromise<Pair<Pair<byte[], byte[]>, byte[]>> p : _promises) {
        p.setFailure(_cause);
      }
      _promises.clear();
      // we don't reset the flag, just abandon this scan operation
      return;
    }
    while (!_promises.isEmpty()) {
      while (++_readKvIter >= _kvs.size()) {
        if (_contextId == CONTEXT_ID_COMPLETED) {
          // this scan operation got incomplete from server, abandon scan operation
          if (_incomplete) {
            for (DefaultPromise<Pair<Pair<byte[], byte[]>, byte[]>> p : _promises) {
              logger.error(
                  "scan got incomplete error, " + "tableName({}), {}",
                  _table.getTableName(),
                  _gpid.toString());
              p.setFailure(new PException("scan got incomplete error, retry later"));
            }
            _promises.clear();
            return;
          }

          // reach the end of one partition, finish scan operation
          if (_partitionIter <= 0) {
            for (DefaultPromise<Pair<Pair<byte[], byte[]>, byte[]>> p : _promises) {
              p.setSuccess(null);
            }
            _promises.clear();
            return;
          }

          _gpid = _partitions[--_partitionIter];
          _hash = _partitionHashes[_partitionIter];
          contextReset();
        } else if (_contextId == CONTEXT_ID_NOT_EXIST) {
          // no valid context_id found
          asyncStartScan();
          return;
        } else {
          asyncNextBatch();
          return;
        }
      }
      DefaultPromise<Pair<Pair<byte[], byte[]>, byte[]>> p = _promises.getFirst();
      p.setSuccess(
          new ImmutablePair<Pair<byte[], byte[]>, byte[]>(
              PegasusClient.restoreKey(_kvs.get(_readKvIter).key.data),
              _kvs.get(_readKvIter).value.data));
      _promises.removeFirst();
    }
  }

  private void contextReset() {
    _kvs.clear();
    _readKvIter = -1;
    _contextId = CONTEXT_ID_NOT_EXIST;
  }

  private Table _table;
  private blob _startKey;
  private blob _stopKey;
  private ScanOptions _options;
  private gpid[] _partitions;
  private long[] _partitionHashes;
  private int _partitionIter;

  private gpid _gpid;
  private long _hash;

  private List<key_value> _kvs;
  private int _readKvIter;

  private long _contextId;

  private final Object _promisesLock = new Object();
  private Deque<DefaultPromise<Pair<Pair<byte[], byte[]>, byte[]>>> _promises;
  private boolean _rpcRunning;
  // mark whether scan operation encounter error
  private boolean _encounterError;
  Throwable _cause;

  private boolean _needCheckHash;
  // whether scan operation got incomplete error
  private boolean _incomplete;

  private static final Logger logger = org.slf4j.LoggerFactory.getLogger(PegasusScanner.class);
}
