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

const EventEmitter = require('events').EventEmitter;
const net = require('net');
const util = require('util');

const Exception = require('./errors');

const ErrorCode = require('./dsn/dsn_types').error_code;
const ErrorType = require('./dsn/dsn_types').error_type;

const thrift = require('thrift');
const InputBufferUnderrunError = require('thrift/lib/nodejs/lib/thrift/input_buffer_underrun_error');

let _seq_id = 0;
let log = null;

/**
 * Constructor of Connection
 * @param {Object}  options
 *        {String}  options.host            required
 *        {Number}  options.port            required
 *        {Number}  options.rpcTimeOut(ms)  required
 *        {Object}  options.log             required
 *        {Object}  options.transport       optional
 *        {Object}  options.protocol        optional
 * @constructor
 */
function Connection(options) {
    log = options.log;
    this.id = ++_seq_id;
    this.host = options.host;
    this.port = options.port;

    this.address = {host: options.host, port: options.port};
    this.hostnamePort = options.host + ':' + options.port;
    this.name = 'Connection(' + this.hostnamePort + ')#' + this.id;
    this.rpcTimeOut = options.rpcTimeOut;

    this.socket = null;
    this.out = null;
    this.connected = false;
    this.closed = false;
    this._requestNums = 0;
    this.requests = {}; //current active requests

    this.transport = options.transport || thrift.TBufferedTransport;
    this.protocol = options.protocol || thrift.TBinaryProtocol;

    EventEmitter.call(this);

    this.setupStream();
}

util.inherits(Connection, EventEmitter);

Connection.Request_counter = 0; //total request for one client

/**
 * Set up connection and register events
 */
Connection.prototype.setupConnection = function () {
    this.socket = net.connect(this.address);
    //register socket event
    this.socket.on('timeout', this._handleTimeout.bind(this));
    this.socket.on('close', this._handleClose.bind(this));
    this.socket.on('error', this._handleError.bind(this));
    //register connection event
    this.on('close', this._close.bind(this));
};

/**
 * Handle socket timeout
 * @private
 */
Connection.prototype._handleTimeout = function () {
    this._close(new Exception.ConnectionClosedException('%s socket closed by timeout', this.name));
};

/**
 * Handle socket close
 * @private
 */
Connection.prototype._handleClose = function () {
    log.debug('%s close event emit', this.name);
    this.closed = true;
    this.emit('close');
    let err = this._closeError || this._socketError;
    if (!err) {
        err = new Exception.RPCException('ERR_SESSION_RESET', this.name + ' closed with no error.');
        log.debug(err.message);
    }
    this._cleanupRequests(err);
};

/**
 * Handle socket error
 * @param err
 * @private
 * @emit {ConnectionClosedException} socket error
 */
Connection.prototype._handleError = function (err) {
    let errorName = 'ConnectionError';
    if (err.message.indexOf('ECONNREFUSED') >= 0) {
        errorName = 'ConnectionRefusedException';
        err = new Exception.RPCException('ERR_SESSION_RESET', this.name + ' error: ' + errorName + ', ' + err.message);
    } else if (err.message.indexOf('ECONNRESET') >= 0 || err.message.indexOf('This socket is closed') >= 0) {
        errorName = 'ConnectionResetException';
        err = new Exception.RPCException('ERR_SESSION_RESET', this.name + ' error: ' + errorName + ', ' + err.message);
    } else {
        err = new Exception.RPCException('ERR_SESSION_RESET', this.name + ' error: ' + errorName + ', ' + err.message);
    }

    log.error('%s sockect meet error %s', this.name, err.message);
    this._socketError = err;
    this.connectError = err;
    this.emit('connectError', err);
    this._cleanupRequests(err);
};

/**
 * Clean up all requests of connection
 * @param err
 * @private
 */
Connection.prototype._cleanupRequests = function (err) {
    let count = 0;
    let requests = this.requests;
    this.requests = {};
    for (let id in requests) {
        let request = requests[id];
        request.setException(err);
        count++;
    }
    log.info('%s cleanup %d requests', this.name, count);
};

/**
 * Close connection and destroy socket
 * @param {ConnectionClosedException} err
 * @private
 */
Connection.prototype._close = function (err) {
    this.closed = true;
    this._closeError = err;
    this.socket.destroy();
    log.info('%s close and socket destroy', this.name);
};

/**
 * Register data event for socket to receive response
 */
Connection.prototype.getResponse = function () {
    let self = this;
    this.socket.on('data', self.transport.receiver(function (transport_with_data) {
        let protocol = new self.protocol(transport_with_data);
        try {
            while (true) {
                let startReadCursor = transport_with_data.readCursor;
                //Response structure: total length + error code structure + TMessage
                let len = protocol.readI32();

                // current packet is NOT integrated
                if (transport_with_data.writeCursor - startReadCursor < len) {
                    transport_with_data.rollbackPosition();
                    break;
                }

                let ec = new ErrorCode();
                ec.read(protocol);

                let msgHeader = protocol.readMessageBegin();
                let request = self.requests[msgHeader.rseqid];
                if (request) {
                    let entry = request.entry;
                    entry.operator.rpc_error = ec;
                    if (ErrorType[ec.errno] === ErrorType.ERR_OK) {
                        entry.operator.recv_data(protocol);
                    } else {
                        log.error('%s request#%d failed, error code is %s',
                            self.name, msgHeader.rseqid, entry.operator.rpc_error.errno);
                    }
                    request.setResponse(entry.operator.response);
                } else {
                    log.error('%s request#%d does not exist, maybe timeout', self.name, msgHeader.rseqid);
                }

                transport_with_data.rollbackPosition();
                transport_with_data.consume(len);
                transport_with_data.commitPosition();
            }

        } catch (e) {
            if (e instanceof InputBufferUnderrunError) {
                transport_with_data.rollbackPosition();
            }
            else {
                self.socket.emit('error', e);
            }
        }
    }));
};

/**
 * Set up socket stream
 */
Connection.prototype.setupStream = function () {
    let self = this;
    log.debug('Connecting to %s', self.name);

    self.setupConnection();
    self.out = new DataOutputStream(this.socket);

    self.socket.on('connect', function () {
        self.connected = true;
        self.getResponse();
        self.emit('connect');
        log.info('Connected to %s', self.name);
    });
};

/**
 * Send request and register request events
 * @param entry
 */
Connection.prototype.call = function (entry) {
    let timeout = entry.operator.timeout;
    let self = this;
    let connectionRequestId = self._requestNums++;

    let rpcRequest = new Request(this, connectionRequestId, entry, timeout);
    self.requests[rpcRequest.id] = rpcRequest;

    rpcRequest.on('done', function (err, operator) {
        delete self.requests[rpcRequest.id];
        if (err) {
            log.debug(err.message);
        }
        entry.callback(err, operator);
    });

    if (self.closed) {
        return this._handleClose();
    }

    rpcRequest.on('timeout', function () {
        delete self.requests[rpcRequest.id];
    });

    this.sendRequest(rpcRequest);
};

/**
 * Send request
 * @param request
 */
Connection.prototype.sendRequest = function (request) {
    let transport = new this.transport();
    let protocol = new this.protocol(transport);

    request.entry.operator.send_data(protocol, request.id);

    let headerBuf = request.entry.operator.prepare_thrift_header(transport.outCount);
    transport.outBuffers.unshift(headerBuf);
    transport.outCount += request.entry.operator.header.header_length;
    transport.onFlush = this.send.bind(this);
    transport.flush();
};

/**
 * Write data to socket
 * @param msg
 */
Connection.prototype.send = function (msg) {
    this.out.write(msg);
};

/**
 * Swap class of socket
 * @param out
 * @constructor
 */
function DataOutputStream(out) {
    this.out = out;
    this.written = 0;
}

/**
 * Write data into stream
 * @param buffer
 */
DataOutputStream.prototype.write = function (buffer) {
    this.out.write(buffer);
    this.written += buffer.length;
};


/**
 * Constructor of request
 * @param {Connection}      connection
 * @param {number}          seqid
 * @param {RequestEntry}    entry
 * @param {number}          timeout
 * @constructor
 */
function Request(connection, seqid, entry, timeout) {
    EventEmitter.call(this);

    Connection.Request_counter++;
    this.connection = connection;
    this.id = seqid;    //seq id
    this.entry = entry;
    this.error = null;
    this.done = false;
    this.startTime = Date.now();
    this.timeout = timeout;

    if (timeout && timeout > 0) {
        log.debug('%s-request%d, timeout %d', this.connection.name, this.id, this.timeout);
        this.timer = setTimeout(this.handleTimeout.bind(this), parseInt(timeout));
    }
}

util.inherits(Request, EventEmitter);

/**
 * Handle request timeout
 */
Request.prototype.handleTimeout = function () {
    let msg = this.connection.name + ' request#' + this.id + ' timeout, use ' + (Date.now() - this.startTime) + 'ms';
    this.entry.operator.rpc_error = new ErrorCode({'errno': 'ERR_TIMEOUT'});
    log.debug('%s has %d requests now', this.connection.name, Object.keys(this.connection.requests).length);

    let err = new Exception.RPCException('ERR_TIMEOUT', msg);
    this.setException(err);
    this.emit('timeout');
};

/**
 * Set request exception
 * @param err
 */
Request.prototype.setException = function (err) {
    if (err.message.indexOf('no error') < 0) {
        log.error('setException: %s request#%d error: %s', this.connection.name, this.id, err.message);
    }
    this.error = err;
    if (err.err_type === 'ERR_SESSION_RESET') {
        this.entry.operator.rpc_error = new ErrorCode({'errno': 'ERR_SESSION_RESET'});
    }
    this.callComplete();
};

/**
 * Set request completed
 */
Request.prototype.callComplete = function () {
    if (this.timer) {
        clearTimeout(this.timer);
        this.timer = null;
    }
    //Done before
    if (this.done) {
        return;
    }
    this.done = true;

    let usedTime = Date.now() - this.startTime;
    this.entry.operator.timeout -= usedTime;
    log.debug('%s-request#%d use %dms, remain timeout %dms',
        this.connection.name,
        this.id,
        usedTime,
        this.entry.operator.timeout);
    this.emit('done', this.error, this.entry.operator);
};

/**
 * Set response
 * @param response
 */
Request.prototype.setResponse = function (response) {
    this.entry.operator.response = response;
    this.callComplete();
};

module.exports = Connection;