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

const InvalidParamException = require('./errors').InvalidParamException;
const net = require('net');

/**
 * Calculate hash by app_id and partition_index
 * @param {number} app_id
 * @param {number} partition_index
 * @return {number}
 */
function dsn_gpid_to_thread_hash(app_id, partition_index) {
    return (parseInt(app_id) * 7919 + parseInt(partition_index));
}

/**
 * Check if rpc_address exist in array
 * @param {Array} array
 * @param addr
 * @return {boolean}
 */
function isAddrExist(array, addr) {
    let i, len = array.length;
    let flag = false;
    for (i = 0; i < len; ++i) {
        if (addr.equals(array[i])) {
            flag = true;
            break;
        }
    }
    return flag;
}

/**
 * Get ReplicaSession by rpc_address in cluster.ReplicaSession
 * @param dic
 * @param address
 * @return {Session}
 */
function findSessionByAddr(dic, address) {
    let i, len = dic.length, session = null;
    for (i = 0; i < len; ++i) {
        if (dic[i].key.equals(address)) {
            session = dic[i].value;
            break;
        }
    }
    return session;
}

/**
 * Concat hashKey and sortKey
 * @param  {Buffer|String} hashKey
 * @param  {Buffer|String} sortKey
 * @return {Buffer}        hashKeyLen+hashKey+sortKey
 */
function generateKey(hashKey, sortKey) {
    let temp;
    if (!Buffer.isBuffer(hashKey)) {
        temp = new Buffer(hashKey);
        hashKey = temp;
    }
    if (!Buffer.isBuffer(sortKey)) {
        temp = new Buffer(sortKey);
        sortKey = temp;
    }

    let hashLen = hashKey.length;
    let sortLen = sortKey.length;

    let lenBuf = Buffer.alloc(2);
    lenBuf.writeInt16BE(hashLen, 0);

    return Buffer.concat([lenBuf, hashKey, sortKey], (2 + hashLen + sortLen));
}

/**
 * Validate configs while creating client
 * @param   {Object}  configs
 *          {Array}   configs.metaServers
 *          {String}  configs.metaServers[i]    ipv4:port
 * @throws  {InvalidParamException}
 */
function validateClientConfigs(configs) {
    if (!(configs instanceof Object)) {
        throw new InvalidParamException('configs should be instanceof Object');
    }
    if (!(configs.metaServers instanceof Array)) {
        throw new InvalidParamException('configs.metaServers should be instanceof Array');
    }
    let i, len = configs.metaServers.length;
    for (i = 0; i < len; ++i) {
        let ip_port = configs.metaServers[i].split(':');
        if (ip_port.length !== 2) {
            throw new InvalidParamException(configs.metaServers[i] + ' is not valid, correct format is ip_address:port');
        }
        if (!net.isIPv4(ip_port[0])) {
            throw new InvalidParamException(ip_port[0] + ' is not valid ipv4 address');
        }
        if (isNaN(ip_port[1]) || ip_port[1] < 0 || ip_port[1] > 65535) {
            throw new InvalidParamException(ip_port[1] + ' is not valid port');
        }
    }
}

/**
 * Check if obj is instance of Function, if not throw error
 * @param   {Object}    obj
 * @param   {String}    name
 * @throws  {InvalidParamException}
 */
function validateFunction(obj, name) {
    if (!(obj instanceof Function)) {
        throw new InvalidParamException('lack of ' + name + ' or ' + name + ' is not function');
    }
}

/**
 * Check if param is instanceof type(when isObject=true) or typeof(param)=type(when isObject=false)
 * @param   {Object}        param
 * @param   {String}        name
 * @param   {String|Object} type
 * @param   {Boolean}       isObject
 * @param   {Function}      callback
 * @return  {Boolean}       true means return callee function at once
 */
function validateParam(param, name, type, isObject, callback) {
    let flag = true;
    if (isObject) {
        flag = (param instanceof type);
        type = type.name;
    } else {
        flag = (typeof(param) === type);
    }
    if (!flag) {
        callback(new InvalidParamException(name + ':' + param + ' is not ' + type), null);
    }
    return (!flag);
}

module.exports = {
    dsn_gpid_to_thread_hash: dsn_gpid_to_thread_hash,
    isAddrExist: isAddrExist,
    findSessionByAddr: findSessionByAddr,
    generateKey: generateKey,
    validateClientConfigs: validateClientConfigs,
    validateFunction: validateFunction,
    validateParam: validateParam,
};


