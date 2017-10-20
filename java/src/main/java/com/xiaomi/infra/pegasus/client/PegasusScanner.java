// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

package com.xiaomi.infra.pegasus.client;

import java.util.ArrayList;
import java.util.List;

import dsn.apps.get_scanner_request;
import dsn.apps.key_value;
import dsn.apps.scan_request;
import dsn.apps.scan_response;
import dsn.api.Table;
import dsn.base.blob;
import dsn.base.gpid;
import dsn.operator.rrdb_clear_scanner_operator;
import dsn.operator.rrdb_get_scanner_operator;
import dsn.operator.rrdb_scan_operator;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

/**
 * @author shenyuannan
 *
 * Implementation of {@link PegasusScannerInterface}.
 */
public class PegasusScanner implements PegasusScannerInterface {
    private static final blob min = new blob(new byte[]{0, 0});
    private static final blob max = new blob(new byte[]{-1, -1});
    private static final int CONTEXT_ID_VALID_MIN = 0;
    private static final int CONTEXT_ID_COMPLETED = -1;
    private static final int CONTEXT_ID_NOT_EXIST = -2;

    public PegasusScanner(Table table, gpid[] splitHash, ScanOptions options) {
        this(table, splitHash, options, min, max);
        options.startInclusive = true;
        options.stopInclusive = false;
    }
    
    public PegasusScanner(Table table, gpid[] splitHash, ScanOptions options,
                          blob startKey, blob stopKey) {
        _table = table;
        _split_gpid = splitHash == null ? new gpid[0] : splitHash;
        _options = options;
        _startKey = startKey;
        _stopKey = stopKey;
        _p = -1;
        _context = CONTEXT_ID_COMPLETED;
        _hash_p = _split_gpid.length;
        _kvs = new ArrayList<key_value>();
    }
    
    public Pair<Pair<byte[], byte[]>, byte[]> next() throws PException {
        while (++_p >= _kvs.size()) {
            if (_context == CONTEXT_ID_COMPLETED) {
                // reach the end of one partition
                if (_hash_p <= 0) {
                    return null;
                } else {
                    _gpid = _split_gpid[--_hash_p];
                    splitReset();
                }
            } else if (_context == CONTEXT_ID_NOT_EXIST) {
                // no valid context_id found
                startScan();
            } else {
                int ret = nextBatch();
                if (ret == 1) {
                    // context not found
                    _context = CONTEXT_ID_NOT_EXIST;
                } else if (ret != 0) {
                    throw new PException("Rocksdb error: " + ret);
                }
            }
        }
        return new ImmutablePair<Pair<byte[], byte[]>, byte[]>(
                PegasusClient.restoreKey(_kvs.get(_p).key.data), 
                _kvs.get(_p).value.data);
    }

    @Override
    public void close() {
        if (_context >= CONTEXT_ID_VALID_MIN) {
            try {
                rrdb_clear_scanner_operator op = new rrdb_clear_scanner_operator(_gpid, _context);
                _table.operate(op, 0);
            } catch (Throwable e) {
                // ignore
            }
            _context = CONTEXT_ID_COMPLETED;
        }
        _hash_p = 0;
    }

    private void startScan() throws PException {
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

        try {
            rrdb_get_scanner_operator op = new rrdb_get_scanner_operator(_gpid, request);
            _table.operate(op, _options.timeoutMillis);
            scan_response response = op.get_response();
            if (response.error != 0) {
                throw new PException("Rocksdb error: " + response);
            }
            
            _kvs = response.kvs;
            _p = -1;
            _context = response.context_id;
        } catch (Throwable e) {
            throw new PException(e);
        }
    }
    
    private int nextBatch() throws PException {
        scan_request request = new scan_request(_context);
        try {
            rrdb_scan_operator op = new rrdb_scan_operator(_gpid, request);
            _table.operate(op, _options.timeoutMillis);
            scan_response response = op.get_response();
            if (response.error == 0) {
                _kvs = response.kvs;
                _p = -1;
                _context = response.context_id;
            }
            return response.error;
        } catch (Throwable e) {
            throw new PException(e);
        }
    }
    
    private void splitReset() {
        _kvs.clear();
        _p = -1;
        _context = CONTEXT_ID_NOT_EXIST;
    }
    
    private Table _table;
    private blob _startKey;
    private blob _stopKey;
    private ScanOptions _options;
    private gpid[] _split_gpid;
    private int _hash_p;
    
    private gpid _gpid;
    private List<key_value> _kvs;
    private int _p;
    
    private long _context;
}
