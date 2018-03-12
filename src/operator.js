/**
 * Created by heyuchen on 18-2-5
 */

"use strict";

const type = require('./dsn/base_types');
const tools = require('./tools');
//const Buffer = require('buffer');
const Int64 = require('node-int64');

const meta = require('./dsn/meta');
const rrdb = require('./dsn/rrdb');
//const replica = require('../dsn/replication_types');

const util = require('util');
const thrift = require('thrift');
const log = require('../log');
const Exception = require('./errors');

const HEADER_LEN = 48;
const HEADER_TYPE = 'THFT';
const META_TIMEOUT = 1000;  //ms

/**
 * Constructor of thrift header
 * @param gpid
 * @constructor
 */
function ThriftHeader(gpid){
    this.hdr_version = 0;
    this.header_length = HEADER_LEN;
    this.header_crc32 = 0;
    this.body_length = 0;
    this.body_crc32 = 0;
    this.app_id = gpid.get_app_id();
    this.partition_index = gpid.get_pidx();
    this.client_timeout = 0;
    this.thread_hash = 0;
    this.partition_hash = 0;
}

/**
 * Covert header to Buffer
 * @return {Buffer}
 */
ThriftHeader.prototype.toBuffer = function(){
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
    let buf = new Int64(this.partition_hash);
    return (Buffer.concat([buffer, buf.toBuffer()]));
};

/**
 * Constructor of base operator
 * @param {gpid} gpid
 * @constructor
 */
function Operator(gpid){
    this.header = new ThriftHeader(gpid);
    this.pid = gpid;
    this.rpc_error = new type.error_code();
    this.request = null;
    this.response = null;
}

/**
 * Set body_length and thread_hash params in header and return header
 * @param body_length
 * @return {Buffer}
 */
Operator.prototype.prepare_thrift_header = function(body_length){
    this.header.body_length = body_length;
    this.header.thread_hash = tools.dsn_gpid_to_thread_hash(this.pid.get_app_id(), this.pid.get_pidx());
    return (this.header.toBuffer());
};

/**
 * Constructor of queryCfgOperator
 * @param gpid
 * @param request
 * @constructor
 * @extends Operator
 */
function QueryCfgOperator(gpid, request){
    QueryCfgOperator.super_.call(this, gpid, this.constructor);
    this.request = request;
    this.timeout = META_TIMEOUT;
}
util.inherits(QueryCfgOperator, Operator);

/**
 * Send data
 * @param protocol
 * @param seqid
 */
QueryCfgOperator.prototype.send_data = function(protocol, seqid){
    protocol.writeMessageBegin('RPC_CM_QUERY_PARTITION_CONFIG_BY_INDEX', thrift.Thrift.MessageType.CALL, seqid);
    let args = new meta.meta_query_cfg_args({'query' : this.request});
    args.write(protocol);
    protocol.writeMessageEnd();
};

/**
 * Receive data
 * @param protocol
 */
QueryCfgOperator.prototype.recv_data = function(protocol){
    let result = new meta.meta_query_cfg_result();
    result.read(protocol);
    if (result.success !== null && result.success !== undefined){
        this.response = result.success;
    }else{
        //TODO: Thrift error check
        log.error("Fail to receive result while query config from meta.");
    }
};

/**
 * Constructor of RrdbGetOperator
 * @param gpid
 * @param request
 * @param timeout
 * @param {Function} callback
 * @constructor
 * @extends Operator
 */
function RrdbGetOperator(gpid, request, timeout, callback){
    RrdbGetOperator.super_.call(this, gpid, this.constructor);
    this.request = request;
    this.timeout = timeout;
    this.userCallback = callback;
}
util.inherits(RrdbGetOperator, Operator);

/**
 * Send data
 * @param protocol
 * @param seqid
 */
RrdbGetOperator.prototype.send_data = function(protocol, seqid){
    protocol.writeMessageBegin('RPC_RRDB_RRDB_GET', thrift.Thrift.MessageType.CALL, seqid);
    let args = new rrdb.rrdb_get_args({'key' : this.request});
    args.write(protocol);
    protocol.writeMessageEnd();
};

/**
 * Receive data
 * @param protocol
 */
RrdbGetOperator.prototype.recv_data = function(protocol){
    let result = new rrdb.rrdb_get_result();
    result.read(protocol);
    if(result.success !== null && result.success !== undefined){
        this.response = result.success;
    }else{
        //todo : check
        log.error('Fail to receive result while get data');
    }
};

/**
 * Handle response error and result
 * @param err
 * @param op
 */
RrdbGetOperator.prototype.handleResult = function(err, op){
    let result = op.response;
    // rpc request succeed
    if(err === null){
        if(result.error !== 0 && result.error !== 1){
            err = new Exception.RocksDBException(result.error, 'Failed to get result');
            result = null;
        }else{
            result = result.value.data;
        }
    }

    if(this.userCallback instanceof Function){
        this.userCallback(err, result);
    }else{
        //todo: check
        log.error('Can not handle result, lack of callback');
    }
};

/**
 * Constructor of RrdbPutOperator
 * @param gpid
 * @param request
 * @param timeout
 * @param callback
 * @constructor
 */
function RrdbPutOperator(gpid, request, timeout, callback){
    RrdbPutOperator.super_.call(this, gpid, this.constructor);
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
RrdbPutOperator.prototype.send_data = function(protocol, seqid){
    protocol.writeMessageBegin('RPC_RRDB_RRDB_PUT', thrift.Thrift.MessageType.CALL, seqid);
    let args = new rrdb.rrdb_put_args({'update' : this.request});
    args.write(protocol);
    protocol.writeMessageEnd();
};

/**
 * Receive data
 * @param protocol
 */
RrdbPutOperator.prototype.recv_data = function(protocol){
    let result = new rrdb.rrdb_put_result();
    result.read(protocol);
    if(result.success !== null && result.success !== undefined){
        this.response = result.success;
    }else{
        //todo: check
        log.error('Fail to receive result while set data');
    }
};

/**
 * Handle response error and result
 * @param err
 * @param op
 */
RrdbPutOperator.prototype.handleResult = function(err, op){
    let result = op.response;
    // rpc request succeed
    if(err === null){
        if(result.error !== 0){
            err = new Exception.RocksDBException(result.error, 'Failed to set result');
            result = null;
        }
    }

    if(this.userCallback instanceof Function){
        this.userCallback(err, result);
    }else{
        //todo: check
        log.error('Can not handle result, lack of callback');
    }
};

/**
 * Constructor of RrdbRemoveOperator
 * @param gpid
 * @param request
 * @param timeout
 * @param callback
 * @constructor
 */
function RrdbRemoveOperator(gpid, request, timeout, callback){
    RrdbPutOperator.super_.call(this, gpid, this.constructor);
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
RrdbRemoveOperator.prototype.send_data = function(protocol, seqid){
    protocol.writeMessageBegin('RPC_RRDB_RRDB_REMOVE', thrift.Thrift.MessageType.CALL, seqid);
    let args = new rrdb.rrdb_remove_args({'key' : this.request});
    args.write(protocol);
    protocol.writeMessageEnd();
};

/**
 * Receive data
 * @param protocol
 */
RrdbRemoveOperator.prototype.recv_data = function(protocol){
    let result = new rrdb.rrdb_remove_result();
    result.read(protocol);
    if(result.success !== null && result.success !== undefined){
        this.response = result.success;
    }else{
        //todo: check
        log.error('Fail to receive result while set data');
    }
};

/**
 * Handle response error and result
 * @param err
 * @param op
 */
RrdbRemoveOperator.prototype.handleResult = function(err, op){
    let result = op.response;
    // rpc request succeed
    if(err === null){
        if(result.error !== 0){
            err = new Exception.RocksDBException(result.error, 'Failed to set result');
            result = null;
        }
    }

    if(this.userCallback instanceof Function){
        this.userCallback(err, result);
    }else{
        //todo: check
        log.error('Can not handle result, lack of callback');
    }
};

/**
 * Constructor of RrdbMultiGetOperator
 * @param gpid
 * @param request
 * @param timeout
 * @param callback
 * @constructor
 */
function RrdbMultiGetOperator(gpid, request, timeout, callback){
    RrdbMultiGetOperator.super_.call(this, gpid, this.constructor);
    this.request = request;
    this.timeout = timeout;
    this.userCallback = callback;
}
util.inherits(RrdbMultiGetOperator, Operator);

/**
 * Send data
 * @param protocol
 * @param seqid
 */
RrdbMultiGetOperator.prototype.send_data = function(protocol, seqid){
    protocol.writeMessageBegin('RPC_RRDB_RRDB_MULTI_GET', thrift.Thrift.MessageType.CALL, seqid);
    let args = new rrdb.rrdb_multi_get_args({'request' : this.request});
    args.write(protocol);
    protocol.writeMessageEnd();
};

/**
 * Receive data
 * @param protocol
 */
RrdbMultiGetOperator.prototype.recv_data = function(protocol){
    let result = new rrdb.rrdb_multi_get_result();
    result.read(protocol);
    if(result.success !== null && result.success !== undefined){
        this.response = result.success;
    }else{
        //todo: check
        log.error('Fail to receive result while set data');
    }
};

/**
 * Handle response error and result
 * @param err
 * @param op
 */
RrdbMultiGetOperator.prototype.handleResult = function(err, op){
    let result = op.response;
    // rpc request succeed
    if(err === null){
        if(result.error !== 0){
            err = new Exception.RocksDBException(result.error, 'Failed to set result');
            result = null;
        }
    }

    if(this.userCallback instanceof Function){
        this.userCallback(err, result.kvs);
    }else{
        //todo: check
        log.error('Can not handle result, lack of callback');
    }
};


/**
 * Constructor of RrdbMultiPutOperator
 * @param gpid
 * @param request
 * @param timeout
 * @param callback
 * @constructor
 */
function RrdbMultiPutOperator(gpid, request, timeout, callback){
    RrdbMultiPutOperator.super_.call(this, gpid, this.constructor);
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
RrdbMultiPutOperator.prototype.send_data = function(protocol, seqid){
    protocol.writeMessageBegin('RPC_RRDB_RRDB_MULTI_PUT', thrift.Thrift.MessageType.CALL, seqid);
    let args = new rrdb.rrdb_multi_put_args({'request' : this.request});
    args.write(protocol);
    protocol.writeMessageEnd();
};

/**
 * Receive data
 * @param protocol
 */
RrdbMultiPutOperator.prototype.recv_data = function(protocol){
    let result = new rrdb.rrdb_multi_put_result();
    result.read(protocol);
    if(result.success !== null && result.success !== undefined){
        this.response = result.success;
    }else{
        //todo: check
        log.error('Fail to receive result while set data');
    }
};

/**
 * Handle response error and result
 * @param err
 * @param op
 */
RrdbMultiPutOperator.prototype.handleResult = function(err, op){
    let result = op.response;
    // rpc request succeed
    if(err === null){
        if(result.error !== 0){
            err = new Exception.RocksDBException(result.error, 'Failed to set result');
            result = null;
        }
    }

    if(this.userCallback instanceof Function){
        this.userCallback(err, result);
    }else{
        //todo: check
        log.error('Can not handle result, lack of callback');
    }
};




module.exports = {
    ThriftHeader : ThriftHeader,
    QueryCfgOperator : QueryCfgOperator,
    RrdbGetOperator : RrdbGetOperator,
    RrdbPutOperator : RrdbPutOperator,
    RrdbRemoveOperator : RrdbRemoveOperator,
    RrdbMultiGetOperator : RrdbMultiGetOperator,
    RrdbMultiPutOperator : RrdbMultiPutOperator,
};