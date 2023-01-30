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

const RpcAddress = require('./dsn/dsn_types').rpc_address;
const ErrorType = require('./dsn/dsn_types').error_type;
const Connection = require('./connection');
const util = require('util');
const Exception = require('./errors');
const META_DELAY = 1000;

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
    this.queryMetaDelay = this.timeout > 3 ? this.timeout / 3 : 1;
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

    this.metaAddress = [];
    this.metaList = [];
    this.curLeader = 0;
    this.maxRetryCounter = 5;
    this.queryingTableName = {};    // tableName -> isQueryingMeta
    this.lastQueryTime = {}; // tableName -> lastQueryTime

    let i;
    let self = this;

    for (i = 0; i < args.metaList.length; ++i) {
        let address = new RpcAddress();
        if (address.fromString(args.metaList[i])) {
            self.getConnection({
                'rpc_address': address,
            }, function (err, connection) {
                self.metaList.push(connection);
                self.metaAddress.push(address);
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
        if (round.lastConnection.connectError || round.lastConnection.closed) {
            if (this.metaList[round.lastIndex] === round.lastConnection) {
                log.error('%s meet error, reconnect it', round.lastConnection.name);
                this.handleConnectedError(round.lastIndex);
            } else if (this.metaList[round.lastIndex].connectError || this.metaList[round.lastIndex].closed || !this.metaList[round.lastIndex].connected ){
                log.error('%s meet error, metaList[%d]: %s also meet error, reconnect lastIndex',
                    round.lastConnection.name,
                    round.lastIndex,
                    this.metaList[round.lastIndex].name);
                round.lastConnection = this.metaList[round.lastIndex];
                this.handleConnectedError(round.lastIndex);
            }else {
                log.info('%s meet error, but metaList[%d]: %s connected, use lastIndex connection',
                    round.lastConnection.name,
                    round.lastIndex,
                    this.metaList[round.lastIndex].name);
                round.lastConnection = this.metaList[round.lastIndex];
            }
        }
        round.lastConnection.call(entry);
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
        log.error('query meta exceed maxQueryCount, error is %s', err);
        this.completeQueryMeta(err, round);
        return;
    }

    let rpcErr = op.rpc_error.errno;
    let metaErr = ErrorType.ERR_UNKNOWN;

    if (ErrorType[rpcErr] === ErrorType.ERR_OK) {
        metaErr = op.response.err.errno;
        if (ErrorType[metaErr] === ErrorType.ERR_SERVICE_NOT_ACTIVE) {   //meta server may be not ready, need to retry later
            log.warn('meta server(%s) is not ready, try to query meta later', round.lastConnection.name);
            needDelay = true;
            needSwitch = false;
        } else if (ErrorType[metaErr] === ErrorType.ERR_FORWARD_TO_OTHERS) {  //current meta server is not leader, need to switch leader
            log.warn('meta server(%s) is not leader, try to switch meta immediatly', round.lastConnection.name);
            needDelay = false;
            needSwitch = true;
        } else {
            log.debug('query meta server(%s) succeed', round.lastConnection.name);
            this.completeQueryMeta(err, round);
            return;
        }
    } else if (ErrorType[rpcErr] === ErrorType.ERR_SESSION_RESET || ErrorType[rpcErr] === ErrorType.ERR_TIMEOUT) {
        log.warn('meta session(%s) not available, rpc error is %s, try to switch meta later', round.lastConnection.name, rpcErr);
        needDelay = true;
        needSwitch = true;
    } else {
        log.error('Unknown error while query meta(%s), rpc error is %s', round.lastConnection.name, rpcErr);
        this.completeQueryMeta(err, round);
        return;
    }

    // switch leader meta server
    if (needSwitch && this.metaList[this.curLeader].hostnamePort === round.lastConnection.hostnamePort) {
        this.curLeader = (this.curLeader + 1) % this.metaList.length;
    }
    round.lastIndex = this.curLeader;
    round.lastConnection = this.metaList[round.lastIndex];
    log.info("Will query meta index[%d]: %s", round.lastIndex, round.lastConnection.name);

    // delay to query meta
    let fun = function () {
        self.query(round);
    };
    if (needDelay) {
        setTimeout(fun, META_DELAY);
    } else {
        this.query(round);
    }
};

/**
 * Execute round callback function
 * @param {Error} err
 * @param {MetaRequestRound} round
 */
MetaSession.prototype.completeQueryMeta = function (err, round) {
    let op = round.operator;
    round.callback(err, op);
    this.queryingTableName[op.request.app_name] = false;
};

/**
 * Reconnect to session when original session meet error or close
 * @param   {Number}    index
 */
MetaSession.prototype.handleConnectedError = function (index) {
    let self = this;
    let oriConnection = self.metaList[index];

    this.getConnection({
        'rpc_address': self.metaAddress[index],
    }, function (err, connection) {
        if (err === null && connection !== null) {
            self.metaList[index] = connection;
            oriConnection.emit('close');
        } else {
            log.error('Failed to get meta connection, %s', err.message);
        }
    });
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

/**
 * Reconnect to session when original session meet error or close
 * @param address
 */
ReplicaSession.prototype.handleConnectedError = function (address) {
    let oriConnection = this.connection;

    let self = this;
    this.getConnection({
        'rpc_address': address,
    }, function (err, connection) {
        if (err === null && connection !== null) {
            self.connection = connection;
            oriConnection.emit('close');
        } else {
            log.error('Failed to get replica connection, %s', err.message);
        }
    });
};

/**
 * Handle rpc error
 * @param {Error} err
 * @param {ClientRequestRound} round
 */
ReplicaSession.prototype.onRpcReply = function (err, round) {
    let needQueryMeta = false;
    let op = round.operator;

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
    case ErrorType.ERR_SESSION_RESET:
    case ErrorType.ERR_OBJECT_NOT_FOUND: // replica server doesn't serve this gpid
    case ErrorType.ERR_INVALID_STATE:    // replica server is not primary
    case ErrorType.ERR_PARENT_PARTITION_MISUSED: // partition finish split, need query meta
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
    }

    if (needQueryMeta) {
        round.tableHandler.callback = function (err, handler) {
            // do nothing
        }.bind(this);
        round.tableHandler.queryMeta(round.tableHandler.tableName, round.tableHandler.onUpdateResponse.bind(round.tableHandler));
    }

    this.tryRetry(err, round);
};

ReplicaSession.prototype.tryRetry = function (err, round) {
    let op = round.operator;
    let self = this;
    let delayTime = op.timeout > 3 ? op.timeout / 3 : 1;
    if (op.timeout - delayTime > 0) {
        op.timeout -= delayTime;
        round.operator = op;
        setTimeout(function () {
            self.operate(round);
        }, delayTime);
    } else {
        if (ErrorType[op.rpc_error.errno] === ErrorType.ERR_UNKNOWN) {
            op.rpc_error.errno = ErrorType.ERR_TIMEOUT;
            round.callback(new Exception.RPCException('ERR_TIMEOUT', err.message), op);
        } else {
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
 * @param   {Number}      index
 * @param   {Connection}  lastConnection
 * @constructor
 */
function MetaRequestRound(operator, callback, maxQueryCount, index, lastConnection) {
    this.operator = operator;
    this.callback = callback;
    this.maxQueryCount = maxQueryCount;
    this.lastIndex = index;
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
}

module.exports = {
    Cluster: Cluster,
    MetaSession: MetaSession,
    ReplicaSession: ReplicaSession,
    RequestEntry: RequestEntry,
    MetaRequestRound: MetaRequestRound,
    ClientRequestRound: ClientRequestRound,
};
