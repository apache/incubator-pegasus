/**
 * Created by heyuchen on 18-2-1
 */

"use strict";

const EventEmitter = require('events').EventEmitter;
const util = require('util');
const Cluster = require('./session').Cluster;
const TableHandler = require('./table_handler').TableHandler;
const TableInfo = require('./table_handler').TableInfo;
const tools = require('./tools');

const _OPERATION_TIMEOUT = 1000;
let log4js = require('log4js');
let logConfig = require('../log_config');

/**
 * Constructor of client
 * @param   {Object} configs
 * @constructor
 */
function Client(configs) {
    if (!(this instanceof Client)) {
        return new Client(configs);
    }
    EventEmitter.call(this);

    this.rpcTimeOut = configs.operationTimeout || _OPERATION_TIMEOUT;
    this.metaList = configs.metaServers;
    if (configs.log) {
        this.log = configs.log;
    } else {
        log4js.configure(logConfig);
        this.log = log4js.getLogger('pegasus');
    }

    this.cachedTableInfo = {};          //tableName -> tableInfo
    this.cluster = new Cluster({        //Current connections
        'metaList': this.metaList,
        'timeout': this.rpcTimeOut,
        'log': this.log,
    });
}

util.inherits(Client, EventEmitter);

/**
 * Create a client instance
 * @param   {Object}  configs
 *          {Array}   configs.metaServers          required
 *          {String}  configs.metaServers[i]       required
 *          {Number}  configs.operationTimeout(ms) optional
 *          {Object}  configs.log                  optional
 * @return  {Client}  client instance
 * @throws  {InvalidParamException}
 */
Client.create = function (configs) {
    tools.validateClientConfigs(configs);
    return new Client(configs);
};

/**
 * Close client and it sessions
 */
Client.prototype.close = function () {
    this.cluster.close();
};

/**
 * Get value
 * @param {String}      tableName
 * @param {Object}      args
 *        {Buffer}      args.hashKey      required
 *        {Buffer}      args.sortKey      required
 *        {Number}      args.timeout(ms)  optional
 * @param {Function}    callback
 * @throws{InvalidParamException} callback is not function
 */
Client.prototype.get = function (tableName, args, callback) {
    tools.validateFunction(callback, 'callback');
    if (tools.validateParam(args.hashKey, 'hashKey', Buffer, true, callback) ||
        tools.validateParam(args.sortKey, 'sortKey', Buffer, true, callback)) {
        return;
    }
    this.getTable(tableName, function (err, tableInfo) {
        if (err === null && tableInfo !== null) {
            tableInfo.get(args, callback);
        } else {
            callback(err, null);
        }
    });
};

/**
 * Set Value
 * @param {String}      tableName
 * @param {Object}      args
 *        {Buffer}      args.hashKey      required
 *        {Buffer}      args.sortKey      required
 *        {Buffer}      args.value        required
 *        {Number}      args.ttl(s)       optional
 *        {Number}      args.timeout(ms)  optional
 * @param {Function}    callback
 * @throws{InvalidParamException} callback is not function
 */
Client.prototype.set = function (tableName, args, callback) {
    tools.validateFunction(callback, 'callback');
    if (tools.validateParam(args.hashKey, 'hashKey', Buffer, true, callback) ||
        tools.validateParam(args.sortKey, 'sortKey', Buffer, true, callback) ||
        tools.validateParam(args.value, 'value', Buffer, true, callback)) {
        return;
    }
    this.getTable(tableName, function (err, tableInfo) {
        if (err === null && tableInfo !== null) {
            tableInfo.set(args, callback);
        } else {
            callback(err, null);
        }
    });
};

/**
 * Batch Set value
 * @param {String}      tableName
 * @param {Array}       argsArray
 *        {Buffer}      argsArray[i].hashKey      required
 *        {Buffer}      argsArray[i].sortKey      required
 *        {Buffer}      argsArray[i].value        required
 *        {Number}      argsArray[i].ttl          optional
 *        {Number}      argsArray[i].timeout(ms)  optional
 * @param {Function}    callback
 * @throws{InvalidParamException} callback is not function
 */
Client.prototype.batchSet = function (tableName, argsArray, callback) {
    tools.validateFunction(callback, 'callback');
    if (tools.validateParam(argsArray, 'argsArray', Array, true, callback)) {
        return;
    }
    let i, len = argsArray.length;
    for (i = 0; i < len; ++i) {
        if (tools.validateParam(argsArray[i].hashKey, 'hashKey', Buffer, true, callback) ||
            tools.validateParam(argsArray[i].sortKey, 'sortKey', Buffer, true, callback) ||
            tools.validateParam(argsArray[i].value, 'value', Buffer, true, callback)) {
            return;
        }
    }
    this.getTable(tableName, function (err, tableInfo) {
        if (err === null && tableInfo !== null) {
            tableInfo.batchSetPromise(argsArray, callback);
        } else {
            callback(err, null);
        }
    });
};

/**
 * Batch Get value
 * @param {String}      tableName
 * @param {Array}       argsArray
 *        {Buffer}      argsArray[i].hashKey      required
 *        {Buffer}      argsArray[i].sortKey      required
 *        {Number}      argsArray[i].timeout(ms)  optional
 * @param {Function}    callback
 * @throws{InvalidParamException} callback is not function
 */
Client.prototype.batchGet = function (tableName, argsArray, callback) {
    tools.validateFunction(callback, 'callback');
    if (tools.validateParam(argsArray, 'argsArray', Array, true, callback)) {
        return;
    }
    let i, len = argsArray.length;
    for (i = 0; i < len; ++i) {
        if (tools.validateParam(argsArray[i].hashKey, 'hashKey', Buffer, true, callback) ||
            tools.validateParam(argsArray[i].sortKey, 'sortKey', Buffer, true, callback)) {
            return;
        }
    }
    this.getTable(tableName, function (err, tableInfo) {
        if (err === null && tableInfo !== null) {
            tableInfo.batchGetPromise(argsArray, callback);
        } else {
            callback(err, null);
        }
    });
};

/**
 * Delete value
 * @param {String}      tableName
 * @param {Object}      args
 *        {Buffer}      args.hashKey      required
 *        {Buffer}      args.sortKey      required
 *        {Number}      args.timeout(ms)  optional
 * @param {Function}    callback
 * @throws{InvalidParamException} callback is not function
 */
Client.prototype.del = function (tableName, args, callback) {
    tools.validateFunction(callback, 'callback');
    if (tools.validateParam(args.hashKey, 'hashKey', Buffer, true, callback) ||
        tools.validateParam(args.sortKey, 'sortKey', Buffer, true, callback)) {
        return;
    }
    this.getTable(tableName, function (err, tableInfo) {
        if (err === null && tableInfo !== null) {
            tableInfo.del(args, callback);
        } else {
            callback(err, null);
        }
    });
};

/**
 * Multi Get
 * @param {String}      tableName
 * @param {Object}      args
 *        {Buffer}      args.hashKey         required
 *        {Array}       args.sortKeyArray    required
 *        {Buffer}      args.sortKeyArray[i] required
 *        {Number}      args.timeout(ms)     optional
 *        {Number}      args.maxFetchCount   optional
 *        {Number}      args.maxFetchSize    optional
 * @param {Function}    callback
 * @throws{InvalidParamException} callback is not function
 */
Client.prototype.multiGet = function (tableName, args, callback) {
    tools.validateFunction(callback, 'callback');
    if (tools.validateParam(args.hashKey, 'hashKey', Buffer, true, callback)) {
        return;
    }
    if (tools.validateParam(args.sortKeyArray, 'sortKeyArray', Array, true, callback)) {
        return;
    }
    let i, len = args.sortKeyArray.length;
    for (i = 0; i < len; ++i) {
        if (tools.validateParam(args.sortKeyArray[i], 'sortKey', Buffer, true, callback)) {
            return;
        }
    }
    this.getTable(tableName, function (err, tableInfo) {
        if (err === null && tableInfo !== null) {
            tableInfo.multiGet(args, callback);
        } else {
            callback(err, null);
        }
    });
};

/**
 * Multi Get
 * @param {String}      tableName
 * @param {Object}      args
 *        {Buffer}      args.hashKey           required
 *        {Array}       args.sortKeyValueArray required
 *                      {'key' : sortKey, 'value' : value}
 *        {Number}      args.timeout(ms)       optional
 *        {Number}      args.ttl(s)            optional
 * @param {Function}    callback
 * @throws{InvalidParamException} callback is not function
 */
Client.prototype.multiSet = function (tableName, args, callback) {
    tools.validateFunction(callback, 'callback');
    if (tools.validateParam(args.hashKey, 'hashKey', Buffer, true, callback)) {
        return;
    }
    if (tools.validateParam(args.sortKeyValueArray, 'sortKeyValueArray', Array, true, callback)) {
        return;
    }
    let i, len = args.sortKeyValueArray.length;
    for (i = 0; i < len; ++i) {
        if (tools.validateParam(args.sortKeyValueArray[i].key, 'sortKey', Buffer, true, callback) ||
            tools.validateParam(args.sortKeyValueArray[i].value, 'value', Buffer, true, callback)) {
            return;
        }
    }
    this.getTable(tableName, function (err, tableInfo) {
        if (err === null && tableInfo !== null) {
            tableInfo.multiSet(args, callback);
        } else {
            callback(err, null);
        }
    });
};

/**
 * Get TableInfo and execute callback
 * @param {String}      tableName
 * @param {Function}    callback
 */
Client.prototype.getTable = function (tableName, callback) {
    if (tools.validateParam(tableName, 'tableName', 'string', false, callback)) {
        return;
    }
    let self = this;
    let tableInfo = self.cachedTableInfo[tableName];
    if (!tableInfo) {
        new TableHandler(self.cluster, tableName, function (err, tableHandler) {
            if (err === null && tableHandler !== null) {
                tableInfo = new TableInfo(self, tableHandler, self.rpcTimeOut);
                self.cachedTableInfo[tableName] = tableInfo;
                callback(err, tableInfo);
            } else {
                callback(err, null);
            }
        });
    } else {  //use cached tableInfo structure
        callback(null, tableInfo);
    }
};

module.exports = Client;

