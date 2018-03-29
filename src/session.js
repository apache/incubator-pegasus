/**
 * Created by heyuchen on 18-2-6
 */

"use strict";

const RpcAddress = require('./dsn/base_types').rpc_address;
const ErrorType = require('./dsn/base_types').error_type;
const Connection = require('./connection');
const log = require('../log');
const util = require('util');
const Exception = require('./errors');
// const EventEmitter = require('events').EventEmitter;
const deasync = require('deasync');
const META_DELAY = 500;

/**
 * Constructor of Cluster
 * @param {Object}  args                required
 *        {Array}   args.metaList       required
 *        {String}  args.metaList[i]    required
 *        {Number}  args.timeout(ms)    required
 * @constructor
 */
function Cluster(args){
    this.timeout = args.timeout;
    this.replicaSessions = [];      // {'key' :rpc_addr, 'value': ReplicaSession}
    this.metaSession = new MetaSession(args);
}

/**
 * Close all sessions
 */
Cluster.prototype.close = function(){
    let i, len = this.replicaSessions.length;
    let connection;
    for(i = 0; i < len; ++i){
        connection = this.replicaSessions[i].value.connection;
        connection.emit('close');
    }
    log.debug('Finish to close replica sessions');

    len = this.metaSession.metaList.length;
    for(i = 0; i < len; ++i){
        connection = this.metaSession.metaList[i];
        connection.emit('close');
    }
    log.debug('Finish to close meta sessions');
};

/**
 * Constructor of Session
 * @param {Object}  args
 *        {Number}  args.timeout(ms)    required
 * @constructor
 */
function Session(args){
    this.timeout = args.timeout;
    this.connection = null;
}

/**
 * Create new Connection by rpc_address
 * @param {Object}      args
 *        {rpc_address} args.rpc_address    required
 * @param {Function}    callback
 */
Session.prototype.getConnection = function(args, callback){
    let rpc_addr = args.rpc_address;
    if(rpc_addr.invalid()){
        log.error('invalid rpc address');
    }
    let connection = new Connection({
        'host' : rpc_addr.host,
        'port' : rpc_addr.port,
        'rpcTimeOut' : this.timeout,
    });

    let sync = true, error = null;
    connection.once('connectError', function(err){
        connection.emit('close');
        sync = false;
        error = err;
    });
    connection.once('connect', function(){
        sync = false;
    });
    while(sync){deasync.sleep(1);}
    if(error === null){
        callback(null, connection);
    }else{
        callback(error, null);
    }
};

/**
 * Constructor of MetaSession
 * @param {Object}  args
 *        {Array}   args.metaList       required
 *        {String}  args.metaList[i]    required
 *        {Number}  args.timeout(ms)    required
 * @constructor
 * @extends Session
 */
function MetaSession(args){ //todo: getConnection callback
    MetaSession.super_.call(this, args, this.constructor);

    this.metaList = [];
    this.curLeader = 0;
    this.maxRetryCounter = 5;

    let i;
    let self = this;

    for(i = 0; i < args.metaList.length; ++i){
        let address = new RpcAddress();
        if(address.fromString(args.metaList[i])){
            self.getConnection({
                'rpc_address' : address,
            }, function(err, connection){
                if(err === null && connection !== null) {
                    self.metaList.push(connection);
                    log.debug('Finish to get session to meta %s:%s', address.host, address.port);
                }else{
                    log.error('Failed to get meta connection, %s', err.message);
                }
            });
        }else{
            log.error('invalid meta server address %s', args.metaList[i]);
        }
    }
    if(this.metaList.length <= 0){
        log.error('No meta connection exist!');
        this.connectionError = new Exception.MetaException('ERR_NO_META_SERVER',
            'Failed to connect to meta server, error is ERR_NO_META_SERVER');
    }
}
util.inherits(MetaSession, Session);

/**
 * Send request to meta by leader connection
 * @param {MetaRequestRound} round
 */
MetaSession.prototype.query = function(round){
    if(this.metaList.length > 0) {
        let entry = new RequestEntry(round.operator, function(err, op){
            round.operator = op;
            this.onFinishQueryMeta(err, round);
        }.bind(this));
        round.lastConnection.call(entry);
    }else{
        log.error('There is no meta session exist');
    }
};

/**
 * Callback function after finishing query meta
 * @param {Error} err
 * @param {MetaRequestRound} round
 */
MetaSession.prototype.onFinishQueryMeta = function(err, round){
    let op = round.operator;
    let needSwitch = false, needDelay = false;
    let self = this;

    // if(op.timeout <= 0){
    //     let err_type = 'ERR_TIMEOUT';
    //     round.callback(new Exception.MetaException(err_type,
    //         'Failed to query meta server, error is ' + err_type
    //     ), op);
    //     return;
    // }

    //todo: test for timeout query meta
    // if(round.maxQueryCount === 5){
    //     op.rpc_error.errno = 'ERR_TIMEOUT';
    // }

    round.maxQueryCount--;
    if(round.maxQueryCount === 0){
        round.callback(err, op);
        return;
    }

    let rpcErr = op.rpc_error.errno;
    let metaErr = ErrorType.ERR_UNKNOWN;
    if(ErrorType[rpcErr] === ErrorType.ERR_OK){
        metaErr = op.response.err.errno;
        if(ErrorType[metaErr] === ErrorType.ERR_SERVICE_NOT_ACTIVE){   //meta server may be not ready, need to retry later
            needDelay = true;
            needSwitch = false;
        }else if(ErrorType[metaErr] === ErrorType.ERR_FORWARD_TO_OTHERS){  //current meta server is not leader, need to switch leader
            needDelay = false;
            needSwitch = true;
        }else{
            round.callback(err, op);
            return;
        }
    }else if(ErrorType[rpcErr] === ErrorType.ERR_SESSION_RESET || ErrorType[rpcErr] === ErrorType.ERR_TIMEOUT){
        needDelay = true;
        needSwitch = true;
    }else{
        log.error('Unknown error while query meta, %s', rpcErr);
        round.callback(err, op);
        return;
    }

    log.debug('%s, count %d, error is %s, meta error is %s',
        round.lastConnection.name,
        round.maxQueryCount,
        rpcErr,
        metaErr);

    if (needSwitch && this.metaList[this.curLeader] === round.lastConnection) {
        this.curLeader = (this.curLeader + 1) % this.metaList.length;
    }
    round.lastConnection = this.metaList[this.curLeader];

    let fun = function(){
        self.query(round);
    };
    if(needDelay){
        setTimeout(fun, META_DELAY);
    }else{
        this.query(round);
    }
};

/**
 * Constructor of ReplicaSession
 * @param   {Object}    args
 *          {string}    args.address        required
 *          {Number}    args.timeout(ms)    required
 * @constructor
 * @extends Session
 */
function ReplicaSession(args){
    ReplicaSession.super_.call(this, args, this.constructor);

    if(!args || !args.address){
        log.error('Invalid params, Missing rpc address while creating replica session');
        return;
    }

    let self = this;
    let addr = new RpcAddress(args.address);
    this.getConnection({
        'rpc_address' : addr,
    },function(err, connection){
        if(err === null && connection !== null) {
            self.connection = connection;
        }else{
            log.error('Failed to get replica connection, %s', err.message);
        }
    });
}
util.inherits(ReplicaSession, Session);

/**
 * Send user request by connection
 * @param {ClientRequestRound} round
 */
ReplicaSession.prototype.operate = function(round){
    let entry = new RequestEntry(round.operator, function(err, op){
        round.operator = op;
        this.onRpcReply(err, round);
    }.bind(this));
    this.connection.call(entry);
};


/**
 * Handle rpc error
 * @param {Error} err
 * @param {ClientRequestRound} round
 */
ReplicaSession.prototype.onRpcReply = function(err, round){
    let needQueryMeta = false;
    let op = round.operator;

    //todo:delete
    // round.count++;
    // if(round.count === 1){
    //     op.rpc_error.errno = 'ERR_TIMEOUT';
    //     op.timeout = 3;
    // }

    switch (ErrorType[op.rpc_error.errno]) {
    case ErrorType.ERR_OK:
        round.callback(err, op);
        return;
    case ErrorType.ERR_TIMEOUT:
        log.warn('Table %s: rpc timeout for gpid(%d, %d), err_code is %s',
            round.tableHandler.tableName,
            op.pid.get_app_id(),
            op.pid.get_pidx(),
            op.rpc_error.errno);
        break;
    case ErrorType.ERR_INVALID_DATA:    // maybe task code is invalid
        log.error('Table %s: invalid data for gpid(%d, %d), err_code is %s',
            round.tableHandler.tableName,
            op.pid.get_app_id(),
            op.pid.get_pidx(),
            op.rpc_error.errno);
        round.callback(new Exception.RPCException('ERR_INVALID_DATA',
            'Failed to query replica server, error is ' + 'ERR_INVALID_DATA'
        ), op);
        return;
    case ErrorType.ERR_SESSION_RESET:
    case ErrorType.ERR_OBJECT_NOT_FOUND: // replica server doesn't serve this gpid
    case ErrorType.ERR_INVALID_STATE:    // replica server is not primary
        log.warn('Table %s: replica server not serve for gpid(%d, %d), err_code is %s',
            round.tableHandler.tableName,
            op.pid.get_app_id(),
            op.pid.get_pidx(),
            op.rpc_error.errno);
        needQueryMeta = true;
        break;
    case ErrorType.ERR_NOT_ENOUGH_MEMBER:
    case ErrorType.ERR_CAPACITY_EXCEEDED:
        log.warn('Table %s: replica server cannot serve writing for gpid(%d, %d), err_code is %s',
            round.tableHandler.tableName,
            op.pid.get_app_id(),
            op.pid.get_pidx(),
            op.rpc_error.errno);
        break;
    default:
        log.error('Table %s: unexpected error for gpid(%d, %d), err_code is %s',
            round.tableHandler.tableName,
            op.pid.get_app_id(),
            op.pid.get_pidx(),
            op.rpc_error.errno);
        round.callback(new Exception.RPCException(ErrorType[op.rpc_error.errno],
            'Failed to query replica server, error is ' + ErrorType[op.rpc_error.errno]
        ), op);
        //round.callback(err, op);
        return;
    }

    if(needQueryMeta){
        round.tableHandler.callback = function(err){
            if(err !== null){
                round.callback(err, op);
            }
        }.bind(this);
        round.tableHandler.queryMeta(round.tableHandler.tableName, round.tableHandler.onUpdateResponse.bind(round.tableHandler));
    }

    if(op.timeout > 0) {
        this.operate(round);
    }else{
        let err_type = 'ERR_TIMEOUT';
        round.callback(new Exception.RPCException(err_type,
            'Failed to query server, error is ' + err_type
        ), op);
    }

};

/**
 * Constructor of RequestEntry
 * @param   {Operator}  operator
 * @param   {Function}  callback
 * @constructor
 */
function RequestEntry(operator, callback){
    this.operator = operator;
    this.callback = callback;
}

/**
 * Constructor of MetaRequestRound
 * @param   {Operator}    operator
 * @param   {Function}    callback
 * @param   {Number}      maxQueryCount
 * @param   {Connection}  lastConnection
 * @constructor
 */
function MetaRequestRound(operator, callback, maxQueryCount, lastConnection){
    this.operator = operator;
    this.callback = callback;
    this.maxQueryCount = maxQueryCount;
    this.lastConnection = lastConnection;
}

/**
 * Constructor of ClientRequestRound
 * @param   {TableHandler}  tableHandler
 * @param   {Operator}      operator
 * @param   {Function}      callback
 * @constructor
 */
function ClientRequestRound(tableHandler, operator, callback){
    this.tableHandler = tableHandler;
    this.operator = operator;
    this.callback = callback;
    this.count = 0; //todo:delete
}

module.exports = {
    Cluster : Cluster,
    MetaSession : MetaSession,
    ReplicaSession : ReplicaSession,
    RequestEntry : RequestEntry,
    MetaRequestRound: MetaRequestRound,
    ClientRequestRound : ClientRequestRound,
};
