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

"use strict";

const type = require('./dsn/dsn_types');
const tools = require('./tools');
const Int64 = require('node-int64');
const Long = require('long');

const meta = require('./dsn/meta');
const rrdb = require('./dsn/rrdb');

const util = require('util');
const thrift = require('thrift');
const Exception = require('./errors');

const HEADER_LEN = 48;
const HEADER_TYPE = 'THFT';

/**
 * Constructor of thrift header
 * @param {gpid}    gpid
 * @param {Long}    partition_hash
 * @constructor
 */
function ThriftHeader(gpid, partition_hash) {
    this.hdr_version = 0;
    this.header_length = HEADER_LEN;
    this.header_crc32 = 0;
    this.body_length = 0;
    this.body_crc32 = 0;
    this.app_id = gpid.get_app_id();
    this.partition_index = gpid.get_pidx();
    this.client_timeout = 0;
    this.thread_hash = 0;
    this.partition_hash = partition_hash;
}

/**
 * Covert header to Buffer
 * @return {Buffer}
 */
ThriftHeader.prototype.toBuffer = function () {
    let buffer = Buffer.alloc(HEADER_LEN - 8);
    buffer.write(HEADER_TYPE, 0);
    buffer.writeInt32BE(this.hdr_version, 4);
    buffer.writeInt32BE(this.header_length, 8);
    buffer.writeInt32BE(this.header_crc32, 12);
    buffer.writeInt32BE(this.body_length, 16);
    buffer.writeInt32BE(this.body_crc32, 20);
    buffer.writeInt32BE(this.app_id, 24);
    buffer.writeInt32BE(this.partition_index, 28);
    buffer.writeInt32BE(this.client_timeout, 32);
    buffer.writeInt32BE(this.thread_hash, 36);
    let buf = new Buffer(8);
    buf.writeInt32BE(this.partition_hash.getHighBits(), 0);
    buf.writeInt32BE(this.partition_hash.getLowBits(), 4);
    return (Buffer.concat([buffer, buf]));
};

/**
 * Constructor of base operator
 * @param {gpid}    gpid
 * @param {Long}    partition_hash
 * @constructor
 */
function Operator(gpid, partition_hash) {
    this.header = new ThriftHeader(gpid, partition_hash);
    this.pid = gpid;
    this.rpc_error = new type.error_code();
    this.request = null;
    this.response = null;
}

/**
 * Set body_length and thread_hash params in header and return header
 * @param  {Number} body_length
 * @return {Buffer}
 */
Operator.prototype.prepare_thrift_header = function (body_length) {
    this.header.body_length = body_length;
    this.header.thread_hash = tools.dsn_gpid_to_thread_hash(this.pid.get_app_id(), this.pid.get_pidx());
    return (this.header.toBuffer());
};

/**
 * Constructor of queryCfgOperator
 * @param  gpid
 * @param  request
 * @param  timeout
 * @constructor
 * @extends Operator
 */
function QueryCfgOperator(gpid, request, timeout) {
    QueryCfgOperator.super_.call(this, gpid, new Long(0, 0), this.constructor);
    this.request = request;
    this.timeout = timeout;
}

util.inherits(QueryCfgOperator, Operator);

/**
 * Send data
 * @param protocol
 * @param seqid
 */
QueryCfgOperator.prototype.send_data = function (protocol, seqid) {
    protocol.writeMessageBegin('RPC_CM_QUERY_PARTITION_CONFIG_BY_INDEX', thrift.Thrift.MessageType.CALL, seqid);
    let args = new meta.meta_query_cfg_args({'query': this.request});
    args.write(protocol);
    protocol.writeMessageEnd();
};

/**
 * Receive data
 * @param protocol
 */
QueryCfgOperator.prototype.recv_data = function (protocol) {
    let result = new meta.meta_query_cfg_result();
    result.read(protocol);
    if (result.success !== null && result.success !== undefined) {
        this.response = result.success;
    } else {
        console.error("Fail to receive result while query config from meta.");
    }
};

/**
 * Constructor of RrdbGetOperator
 * @param  gpid
 * @param  request
 * @param  hashKey
 * @param  sortKey
 * @param  partition_hash
 * @param  timeout
 * @param  callback
 * @constructor
 * @extends Operator
 */
function RrdbGetOperator(gpid, request, hashKey, sortKey, partition_hash, timeout, callback) {
    RrdbGetOperator.super_.call(this, gpid, partition_hash, this.constructor);
    this.request = request;
    this.hashKey = hashKey;
    this.sortKey = sortKey;
    this.timeout = timeout;
    this.userCallback = callback;
}

util.inherits(RrdbGetOperator, Operator);

/**
 * Send data
 * @param protocol
 * @param seqid
 */
RrdbGetOperator.prototype.send_data = function (protocol, seqid) {
    protocol.writeMessageBegin('RPC_RRDB_RRDB_GET', thrift.Thrift.MessageType.CALL, seqid);
    let args = new rrdb.rrdb_get_args({'key': this.request});
    args.write(protocol);
    protocol.writeMessageEnd();
};

/**
 * Receive data
 * @param protocol
 */
RrdbGetOperator.prototype.recv_data = function (protocol) {
    let result = new rrdb.rrdb_get_result();
    result.read(protocol);
    if (result.success !== null && result.success !== undefined) {
        this.response = result.success;
    } else {
        console.error('Fail to receive result while get data');
    }
};

/**
 * Handle response error and result
 * @param err
 * @param op
 */
RrdbGetOperator.prototype.handleResult = function (err, op) {
    let result = op.response;
    // rpc request succeed
    if (err === null) {
        if (result.error !== 0 && result.error !== 1) {
            err = new Exception.RocksDBException(result.error, 'Failed to get result');
            result = null;
        } else {
            let value = result.value.data;
            result = {
                'hashKey': this.hashKey,
                'sortKey': this.sortKey,
                'value': value,
            };
        }
    }
    this.userCallback(err, result);

};

/**
 * Constructor of RrdbPutOperator
 * @param gpid
 * @param request
 * @param partition_hash
 * @param timeout
 * @param callback
 * @constructor
 */
function RrdbPutOperator(gpid, request, partition_hash, timeout, callback) {
    RrdbPutOperator.super_.call(this, gpid, partition_hash, this.constructor);
    this.request = request;
    this.timeout = timeout;
    this.userCallback = callback;
}

util.inherits(RrdbPutOperator, Operator);

/**
 * Send data
 * @param protocol
 * @param seqid
 */
RrdbPutOperator.prototype.send_data = function (protocol, seqid) {
    protocol.writeMessageBegin('RPC_RRDB_RRDB_PUT', thrift.Thrift.MessageType.CALL, seqid);
    let args = new rrdb.rrdb_put_args({'update': this.request});
    args.write(protocol);
    protocol.writeMessageEnd();
};

/**
 * Receive data
 * @param protocol
 */
RrdbPutOperator.prototype.recv_data = function (protocol) {
    let result = new rrdb.rrdb_put_result();
    result.read(protocol);
    if (result.success !== null && result.success !== undefined) {
        this.response = result.success;
    } else {
        console.error('Fail to receive result while set data');
    }
};

/**
 * Handle response error and result
 * @param err
 * @param op
 */
RrdbPutOperator.prototype.handleResult = function (err, op) {
    let result = op.response;
    // rpc request succeed
    if (err === null) {
        if (result.error !== 0) {
            err = new Exception.RocksDBException(result.error, 'Failed to set result');
            result = null;
        }
    }
    this.userCallback(err, result);
};

/**
 * Constructor of RrdbRemoveOperator
 * @param gpid
 * @param partition_hash
 * @param request
 * @param timeout
 * @param callback
 * @constructor
 */
function RrdbRemoveOperator(gpid, request, partition_hash, timeout, callback) {
    RrdbPutOperator.super_.call(this, gpid, partition_hash, this.constructor);
    this.request = request;
    this.timeout = timeout;
    this.userCallback = callback;
}

util.inherits(RrdbRemoveOperator, Operator);

/**
 * Send data
 * @param protocol
 * @param seqid
 */
RrdbRemoveOperator.prototype.send_data = function (protocol, seqid) {
    protocol.writeMessageBegin('RPC_RRDB_RRDB_REMOVE', thrift.Thrift.MessageType.CALL, seqid);
    let args = new rrdb.rrdb_remove_args({'key': this.request});
    args.write(protocol);
    protocol.writeMessageEnd();
};

/**
 * Receive data
 * @param protocol
 */
RrdbRemoveOperator.prototype.recv_data = function (protocol) {
    let result = new rrdb.rrdb_remove_result();
    result.read(protocol);
    if (result.success !== null && result.success !== undefined) {
        this.response = result.success;
    } else {
        console.error('Fail to receive result while set data');
    }
};

/**
 * Handle response error and result
 * @param err
 * @param op
 */
RrdbRemoveOperator.prototype.handleResult = function (err, op) {
    let result = op.response;
    // rpc request succeed
    if (err === null) {
        if (result.error !== 0) {
            err = new Exception.RocksDBException(result.error, 'Failed to set result');
            result = null;
        }
    }
    this.userCallback(err, result);

};

/**
 * Constructor of RrdbMultiGetOperator
 * @param gpid
 * @param request
 * @param hashKey
 * @param partition_hash
 * @param timeout
 * @param callback
 * @constructor
 */
function RrdbMultiGetOperator(gpid, request, hashKey, partition_hash, timeout, callback) {
    RrdbMultiGetOperator.super_.call(this, gpid, partition_hash, this.constructor);
    this.request = request;
    this.hashKey = hashKey;
    this.timeout = timeout;
    this.userCallback = callback;
}

util.inherits(RrdbMultiGetOperator, Operator);

/**
 * Send data
 * @param protocol
 * @param seqid
 */
RrdbMultiGetOperator.prototype.send_data = function (protocol, seqid) {
    protocol.writeMessageBegin('RPC_RRDB_RRDB_MULTI_GET', thrift.Thrift.MessageType.CALL, seqid);
    let args = new rrdb.rrdb_multi_get_args({'request': this.request});
    args.write(protocol);
    protocol.writeMessageEnd();
};

/**
 * Receive data
 * @param protocol
 */
RrdbMultiGetOperator.prototype.recv_data = function (protocol) {
    let result = new rrdb.rrdb_multi_get_result();
    result.read(protocol);
    if (result.success !== null && result.success !== undefined) {
        this.response = result.success;
    } else {
        console.error('Fail to receive result while set data');
    }
};

/**
 * Handle response error and result
 * @param err
 * @param op
 */
RrdbMultiGetOperator.prototype.handleResult = function (err, op) {
    let result = op.response, data = [];
    // rpc request succeed
    if (err === null) {
        if (result.error !== 0) {
            err = new Exception.RocksDBException(result.error, 'Failed to set result');
            result = null;
        } else {
            let kvs = result.kvs, len = kvs.length, i;
            for (i = 0; i < len; ++i) {
                data.push({
                    'hashKey': this.hashKey,
                    'sortKey': kvs[i].key.data,
                    'value': kvs[i].value.data,
                });
            }
        }
    }
    this.userCallback(err, data);
};


/**
 * Constructor of RrdbMultiPutOperator
 * @param gpid
 * @param request
 * @param partition_hash
 * @param timeout
 * @param callback
 * @constructor
 */
function RrdbMultiPutOperator(gpid, request, partition_hash, timeout, callback) {
    RrdbMultiPutOperator.super_.call(this, gpid, partition_hash, this.constructor);
    this.request = request;
    this.timeout = timeout;
    this.userCallback = callback;
}

util.inherits(RrdbMultiPutOperator, Operator);

/**
 * Send data
 * @param protocol
 * @param seqid
 */
RrdbMultiPutOperator.prototype.send_data = function (protocol, seqid) {
    protocol.writeMessageBegin('RPC_RRDB_RRDB_MULTI_PUT', thrift.Thrift.MessageType.CALL, seqid);
    let args = new rrdb.rrdb_multi_put_args({'request': this.request});
    args.write(protocol);
    protocol.writeMessageEnd();
};

/**
 * Receive data
 * @param protocol
 */
RrdbMultiPutOperator.prototype.recv_data = function (protocol) {
    let result = new rrdb.rrdb_multi_put_result();
    result.read(protocol);
    if (result.success !== null && result.success !== undefined) {
        this.response = result.success;
    } else {
        console.error('Fail to receive result while set data');
    }
};

/**
 * Handle response error and result
 * @param err
 * @param op
 */
RrdbMultiPutOperator.prototype.handleResult = function (err, op) {
    let result = op.response;
    // rpc request succeed
    if (err === null) {
        if (result.error !== 0) {
            err = new Exception.RocksDBException(result.error, 'Failed to set result');
            result = null;
        }
    }
    this.userCallback(err, result);
};


module.exports = {
    ThriftHeader: ThriftHeader,
    Operator: Operator,
    QueryCfgOperator: QueryCfgOperator,
    RrdbGetOperator: RrdbGetOperator,
    RrdbPutOperator: RrdbPutOperator,
    RrdbRemoveOperator: RrdbRemoveOperator,
    RrdbMultiGetOperator: RrdbMultiGetOperator,
    RrdbMultiPutOperator: RrdbMultiPutOperator,
};