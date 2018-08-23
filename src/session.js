/**
 * Created by heyuchen on 18-2-6
 */

"use strict";

const RpcAddress = require('./dsn/base_types').rpc_address;
const ErrorType = require('./dsn/base_types').error_type;
const Connection = require('./connection');
const util = require('util');
const Exception = require('./errors');
// const EventEmitter = require('events').EventEmitter;
const deasync = require('deasync');
const META_DELAY = 500;

let log = null;

/**
 * Constructor of Cluster
 * @param {Object}  args                required
 *        {Array}   args.metaList       required
 *        {String}  args.metaList[i]    required
 *        {Number}  args.timeout(ms)    required
 *        {Object}  args.log            required
 * @constructor
 */
function Cluster(args) {
    log = args.log;
    this.log = args.log;
    this.timeout = args.timeout;
    this.replicaSessions = [];      // {'key' :rpc_addr, 'value': ReplicaSession}
    this.metaSession = new MetaSession(args);
}

/**
 * Close all sessions
 */
Cluster.prototype.close = function () {
    let i, len = this.replicaSessions.length;
    let connection;
    for (i = 0; i < len; ++i) {
        connection = this.replicaSessions[i].value.connection;
        if (connection) {
            connection.emit('close');
        }
    }
    log.info('Finish to close replica sessions');

    len = this.metaSession.metaList.length;
    for (i = 0; i < len; ++i) {
        connection = this.metaSession.metaList[i];
        if (connection) {
            connection.emit('close');
        }
    }
    log.info('Finish to close meta sessions');
};

/**
 * Constructor of Session
 * @param {Object}  args
 *        {Number}  args.timeout(ms)    required
 * @constructor
 */
function Session(args) {
    this.timeout = args.timeout;
    this.connection = null;
}

/**
 * Create new Connection by rpc_address
 * @param {Object}      args
 *        {rpc_address} args.rpc_address    required
 * @param {Function}    callback
 */
Session.prototype.getConnection = function (args, callback) {
    let rpc_addr = args.rpc_address;
    if (rpc_addr.invalid()) {
        log.error('invalid rpc address');
    }
    let connection = new Connection({
        'host': rpc_addr.host,
        'port': rpc_addr.port,
        'rpcTimeOut': this.timeout,
        'log': log,
    });

    // let sync = true, error = null;
    // connection.once('connectError', function(err){
    //     connection.emit('close');
    //     error = err;
    //     sync = false;
    // });
    // connection.once('connect', function(){
    //     sync = false;
    // });
    // while(sync){deasync.sleep(1);}
    // if(error === null){
    //     callback(null, connection);
    // }else{
    //     callback(error, null);
    // }
    callback(null, connection);
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
function MetaSession(args) {
    MetaSession.super_.call(this, args, this.constructor);

    this.metaList = [];
    this.curLeader = 0;
    this.maxRetryCounter = 5;
    this.roundQueue = [];
    this.isQuery = false;

    let i;
    let self = this;

    for (i = 0; i < args.metaList.length; ++i) {
        let address = new RpcAddress();
        if (address.fromString(args.metaList[i])) {
            self.getConnection({
                'rpc_address': address,
            }, function (err, connection) {
                self.metaList.push(connection);
                // if(err === null && connection !== null) {
                //     self.metaList.push(connection);
                //     log.debug('Finish to get session to meta %s:%s', address.host, address.port);
                // }else{
                //     log.error('Failed to get meta connection, %s', err.message);
                // }
            });
        } else {
            log.error('invalid meta server address %s', args.metaList[i]);
        }
    }
    if (this.metaList.length <= 0) {
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
MetaSession.prototype.query = function (round) {
    if (this.metaList.length > 0) {
        let entry = new RequestEntry(round.operator, function (err, op) {
            round.operator = op;
            this.onFinishQueryMeta(err, round);
        }.bind(this));
        round.lastConnection.call(entry);
        //console.log('%s will handle querying meta request', round.lastConnection.name);
    } else {
        log.error('There is no meta session exist');
    }
};

/**
 * Callback function after finishing query meta
 * @param {Error} err
 * @param {MetaRequestRound} round
 */
MetaSession.prototype.onFinishQueryMeta = function (err, round) {
    let op = round.operator;
    let needSwitch = false, needDelay = false;
    let self = this;

    round.maxQueryCount--;
    if (round.maxQueryCount === 0) {
        // round.callback(err, op);
        // console.log('try query meta exceed, current leader is %d, %s',);
        this.completeQueryMeta(err, round);
        return;
    }

    let rpcErr = op.rpc_error.errno;
    let metaErr = ErrorType.ERR_UNKNOWN;

    // if(ErrorType[rpcErr] !== ErrorType.ERR_OK) {
    //     console.log(rpcErr);
    // }


    if (ErrorType[rpcErr] === ErrorType.ERR_OK) {
        metaErr = op.response.err.errno;
        if (ErrorType[metaErr] === ErrorType.ERR_SERVICE_NOT_ACTIVE) {   //meta server may be not ready, need to retry later
            needDelay = true;
            needSwitch = false;
        } else if (ErrorType[metaErr] === ErrorType.ERR_FORWARD_TO_OTHERS) {  //current meta server is not leader, need to switch leader
            needDelay = false;
            needSwitch = true;
        } else {
            // round.callback(err, op);

            this.completeQueryMeta(err, round);
            return;
        }
    } else if (ErrorType[rpcErr] === ErrorType.ERR_SESSION_RESET || ErrorType[rpcErr] === ErrorType.ERR_TIMEOUT) {
        needDelay = true;
        needSwitch = true;
    } else {
        log.error('Unknown error while query meta, %s', rpcErr);
        // round.callback(err, op);
        this.completeQueryMeta(err, round);
        return;
    }

    log.warn('%s, count %d, error is %s, meta error is %s',
        round.lastConnection.name,
        round.maxQueryCount,
        rpcErr,
        metaErr);

    // console.log('current leader is %d, connection is %s', this.curLeader, round.lastConnection.name);
    // log.info('current leader is %d, connection is %s', this.curLeader, round.lastConnection.name);

    if (needSwitch && this.metaList[this.curLeader] === round.lastConnection) {
        this.curLeader = (this.curLeader + 1) % this.metaList.length;
    }
    round.lastConnection = this.metaList[this.curLeader];

    // console.log('next leader is %d, connection is %s, query count is %d', this.curLeader, round.lastConnection.name, round.maxQueryCount);
    // log.info('next leader is %d, connection is %s, query count is %d', this.curLeader, round.lastConnection.name, round.maxQueryCount);

    let fun = function () {
        self.query(round);
    };
    if (needDelay) {
        setTimeout(fun, META_DELAY);
    } else {
        this.query(round);
    }
};

MetaSession.prototype.completeQueryMeta = function (err, round) {
    let op = round.operator;
    round.callback(err, op);

    let len = this.roundQueue.length, i;
    // console.log('%d requests for querying meta will return', len);
    for (i = 0; i < len; ++i) {
        this.roundQueue[i].callback(err, op);
    }
    this.roundQueue = [];
    this.isQuery = false;
};

/**
 * Constructor of ReplicaSession
 * @param   {Object}    args
 *          {string}    args.address        required
 *          {Number}    args.timeout(ms)    required
 * @constructor
 * @extends Session
 */
function ReplicaSession(args) {
    ReplicaSession.super_.call(this, args, this.constructor);
    this.operatorQueue = [];

    if (!args || !args.address) {
        log.error('Invalid params, Missing rpc address while creating replica session');
        return;
    }

    let self = this;
    let addr = new RpcAddress(args.address);
    this.getConnection({
        'rpc_address': addr,
    }, function (err, connection) {
        self.connection = connection;
        // if(err === null && connection !== null) {
        //     self.connection = connection;
        // }else{
        //     log.error('Failed to get replica connection, %s', err.message);
        // }
    });
}

util.inherits(ReplicaSession, Session);

/**
 * Send user request by connection
 * @param {ClientRequestRound} round
 */
ReplicaSession.prototype.operate = function (round) {
    let entry = new RequestEntry(round.operator, function (err, op) {
        round.operator = op;
        this.onRpcReply(err, round);
    }.bind(this));
    this.connection.call(entry);
};

//TODO: testing
ReplicaSession.prototype.handleConnectedError = function (tableHandler, address) {
    // 1. get requests in queue
    let oriConnection = this.connection, needQueryMeta = false;
    // if(requests.length > 0){
    //     for(let id in requests){
    //         let request = requests[id];
    //         this.operatorQueue.push(request.entry.operator);
    //     }
    // }
    // console.log('origin queue length is %d, current queue length is %d', requests.length, this.operatorQueue.length);
    // this.connection.requests = [];

    // 2. create new connection
    let self = this;
    this.getConnection({
        'rpc_address': address,
    }, function (err, connection) {
        if (err === null && connection !== null) {
            self.connection = connection;
            // console.log('%s build!', connection.name);
            // log.info('%s build!', connection.name);
            self.retryRequests(tableHandler);
            oriConnection.emit('close');
        } else {
            log.error('Failed to get replica connection, %s', err.message);
            needQueryMeta = true;
        }
    });

    // 3. retry = false
    this.retry = false;
    return needQueryMeta;
};

ReplicaSession.prototype.retryRequests = function (tableHandler) {
    let len = this.operatorQueue.length, i;
    if (len > 0) {
        // console.log('%d request ready to retry', len);
        // log.info('%d request ready to retry', len);
        for (i = 0; i < len; ++i) {   //retry requests
            let opRetry = this.operatorQueue[i];
            let clientRoundRetry = new ClientRequestRound(tableHandler, opRetry, opRetry.handleResult.bind(opRetry));
            this.operate(clientRoundRetry);
        }
        this.operatorQueue = [];
    }
};


/**
 * Handle rpc error
 * @param {Error} err
 * @param {ClientRequestRound} round
 */
ReplicaSession.prototype.onRpcReply = function (err, round) {
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
            break;
        // round.callback(new Exception.RPCException('ERR_INVALID_DATA',
        //     'Failed to query replica server, error is ' + 'ERR_INVALID_DATA'
        // ), op);
        // return;
        case ErrorType.ERR_SESSION_RESET:
        case ErrorType.ERR_OBJECT_NOT_FOUND: // replica server doesn't serve this gpid
        case ErrorType.ERR_INVALID_STATE:    // replica server is not primary
            //console.log(round);
            //TODO: remove timestamp, hashKey
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
            break;
        // round.callback(new Exception.RPCException(ErrorType[op.rpc_error.errno],
        //     'Failed to query replica server, error is ' + ErrorType[op.rpc_error.errno]
        // ), op);
        // round.callback(err, op);
        // return;
    }

    if (needQueryMeta) {
        round.tableHandler.callback = function (err) {
            if (err !== null) {
                //let hashKey = op.hashKey || new Buffer('');
                // console.log(hashKey);
                // console.log(err);
                //round.callback(err, op);
            }
        }.bind(this);
        round.tableHandler.queryMeta(round.tableHandler.tableName, round.tableHandler.onUpdateResponse.bind(round.tableHandler));
    }

    this.tryRetry(err, round);
    // if(op.timeout > 0) {
    //     this.operate(round);
    // }else{
    //     let err_type = 'ERR_TIMEOUT';
    //     round.callback(new Exception.RPCException(err_type,
    //         'Failed to query server, error is ' + err_type
    //     ), op);
    // }
};

ReplicaSession.prototype.tryRetry = function (err, round) {
    let op = round.operator;
    let self = this;
    let delayTime = op.timeout > 3 ? op.timeout / 3 : 1;
    if (op.timeout - delayTime > 0) {
        //console.log('hashKey %s will retry after %dms, error_code is %s', op.hashKey, delayTime, op.rpc_error.errno);
        //console.log('hashKey %s current timeout is %dms, should be set %dms', op.hashKey, op.timeout, op.timeout-delayTime);
        op.timeout -= delayTime;
        round.operator = op;
        setTimeout(function () {
            self.operate(round);
        }, delayTime);
    } else {
        if (ErrorType[op.rpc_error.errno] === ErrorType.ERR_UNKNOWN) {
            op.rpc_error.errno = ErrorType.ERR_TIMEOUT;
            round.callback(new Exception.RPCException('ERR_TIMEOUT', err.message), op);
        }
        // else{
        //     round.callback(err, op);
        // }
        else {
            round.callback(new Exception.RPCException(op.rpc_error.errno, 'Failed to query server, error is ' + op.rpc_error.errno), op);
        }
    }

};

/**
 * Constructor of RequestEntry
 * @param   {Operator}  operator
 * @param   {Function}  callback
 * @constructor
 */
function RequestEntry(operator, callback) {
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
function MetaRequestRound(operator, callback, maxQueryCount, lastConnection) {
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
function ClientRequestRound(tableHandler, operator, callback) {
    this.tableHandler = tableHandler;
    this.operator = operator;
    this.callback = callback;
    this.count = 0; //todo:delete
}

module.exports = {
    Cluster: Cluster,
    MetaSession: MetaSession,
    ReplicaSession: ReplicaSession,
    RequestEntry: RequestEntry,
    MetaRequestRound: MetaRequestRound,
    ClientRequestRound: ClientRequestRound,
};
