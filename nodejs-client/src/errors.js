/**
 * Created by heyuchen on 18-2-8
 */

"use strict";

const util = require('util');
const ErrorType = require('./dsn/base_types').error_type;

//Base Error
function PException(msg) {
    this.message = msg || 'Error';
}

util.inherits(PException, Error);

//IOException
function IOException(msg) {
    IOException.super_.call(this, msg, this.constructor);
    this.name = 'IOException';
}

util.inherits(IOException, PException);

//InvalidParamException
function InvalidParamException(msg) {
    InvalidParamException.super_.call(this, msg, this.constructor);
    this.name = 'InvalidParamException';
}

util.inherits(InvalidParamException, PException);

//RPCException
function RPCException(err_type, msg) {
    RPCException.super_.call(this, msg, this.constructor);
    this.name = 'RPCException';
    this.err_type = err_type;
    this.err_code = ErrorType[this.err_type];
}

util.inherits(RPCException, IOException);

//MetaException
function MetaException(err_type, msg) {
    MetaException.super_.call(this, msg, this.constructor);
    this.name = 'MetaException';
    this.err_type = err_type;
    this.err_code = ErrorType[this.err_type];
}

util.inherits(MetaException, IOException);

let RocksDBErrorCode = {
    'kOk': 0,
    'kNotFound': 1,
    'kCorruption': 2,
    'kNotSupported': 3,
    'kInvalidArgument': 4,
    'kIOError': 5,
    'kMergeInProgress': 6,
    'kIncomplete': 7,
    'kShutdownInProgress': 8,
    'kTimedOut': 9,
    'kAborted': 10,
    'kBusy': 11,
    'kExpired': 12,
    'kTryAgain': 13,
    'kNoNeedOperate': 101,
};

//RocksDBException
function RocksDBException(err_code, msg) {
    let key, value;
    for (key in RocksDBErrorCode) {
        if (RocksDBErrorCode[key] === err_code) {
            value = key;
            break;
        }
    }
    msg = msg + ', RocksDB error ' + value;
    RocksDBException.super_.call(this, msg, this.constructor);
    this.name = 'RocksDBException';
    this.err_code = err_code;
}

util.inherits(RocksDBException, IOException);

//ConnectionClosedException
function ConnectionClosedException(msg) {
    ConnectionClosedException.super_.call(this, msg, this.constructor);
    this.name = 'ConnectionClosedException';
}

util.inherits(ConnectionClosedException, IOException);

//ThriftException
function ThriftException(msg) {
    ThriftException.super_.call(this, msg, this.constructor);
    this.name = 'ThriftException';
}

util.inherits(ThriftException, IOException);

module.exports = {
    PException: PException,
    IOException: IOException,
    InvalidParamException: InvalidParamException,
    RPCException: RPCException,
    MetaException: MetaException,
    RocksDBException: RocksDBException,
    ConnectionClosedException: ConnectionClosedException,
};

