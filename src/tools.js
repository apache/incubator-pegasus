/**
 * Created by hyc on 18-2-11
 */

"use strict";

//let InvalidParamException = require('./errors').InvalidParamException;

/**
 * Calculate hash by app_id and partition_index
 * @param {number} app_id
 * @param {number} partition_index
 * @return {number}
 */
function dsn_gpid_to_thread_hash(app_id, partition_index){
    return (parseInt(app_id)*7919 + parseInt(partition_index));
}

/**
 * Check if rpc_address exist in array
 * @param {Array} array
 * @param addr
 * @return {boolean}
 */
function isAddrExist(array, addr){
    let i, len = array.length;
    let flag = false;
    for(i = 0; i < len; ++i){
        if(addr.equals(array[i])){
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
function findSessionByAddr(dic, address){
    let i, len = dic.length, session = null;
    for(i = 0; i < len; ++i){
        if(dic[i].key.equals(address)){
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
function generateKey(hashKey, sortKey){
    let temp;
    if(!Buffer.isBuffer(hashKey)){
        temp = new Buffer(hashKey);
        hashKey = temp;
    }
    if(!Buffer.isBuffer(sortKey)){
        temp = new Buffer(sortKey);
        sortKey = temp;
    }

    let hashLen = hashKey.length;
    let sortLen = sortKey.length;

    let lenBuf = Buffer.alloc(2);
    lenBuf.writeInt16BE(hashLen, 0);

    return Buffer.concat([lenBuf, hashKey, sortKey], (2+hashLen+sortLen));
}

module.exports = {
    dsn_gpid_to_thread_hash : dsn_gpid_to_thread_hash,
    isAddrExist : isAddrExist,
    findSessionByAddr : findSessionByAddr,
    generateKey : generateKey,
};


