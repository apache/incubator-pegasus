//  Licensed to the Apache Software Foundation (ASF) under one
//  or more contributor license agreements.  See the NOTICE file
//  distributed with this work for additional information
//  regarding copyright ownership.  The ASF licenses this file
//  to you under the Apache License, Version 2.0 (the
//  "License"); you may not use this file except in compliance
//  with the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an
//  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
//  KIND, either express or implied.  See the License for the
//  specific language governing permissions and limitations
//  under the License.

"use strict";

var thrift = require('thrift');
var Thrift = thrift.Thrift;
var Q = thrift.Q;
var Int64 = thrift.Int64;

//var ttypes = module.exports = {};

var blob = function(args) {
    if(args && args.data){
        this.data = args.data;
    }
};
blob.prototype = {};

blob.prototype.read = function(input){
    this.data = input.readBinary();
};

blob.prototype.write = function(output){
    output.writeBinary(this.data);
};

//Error code enum
var error_type = {
    ERR_OK : 0,
    ERR_UNKNOWN : 1,
    ERR_SERVICE_NOT_FOUND : 2,
    ERR_SERVICE_ALREADY_RUNNING : 3,
    ERR_IO_PENDING : 4,
    ERR_TIMEOUT : 5,
    ERR_SERVICE_NOT_ACTIVE : 6,
    ERR_BUSY : 7,
    ERR_NETWORK_INIT_FAILED : 8,
    ERR_FORWARD_TO_OTHERS : 9,
    ERR_OBJECT_NOT_FOUND : 10,
    ERR_HANDLER_NOT_FOUND : 11,
    ERR_LEARN_FILE_FAILED : 12,
    ERR_GET_LEARN_STATE_FAILED : 13,
    ERR_INVALID_VERSION : 14,
    ERR_INVALID_PARAMETERS : 15,
    ERR_CAPACITY_EXCEEDED : 16,
    ERR_INVALID_STATE : 17,
    ERR_INACTIVE_STATE : 18,
    ERR_NOT_ENOUGH_MEMBER : 19,
    ERR_FILE_OPERATION_FAILED : 20,
    ERR_HANDLE_EOF : 21,
    ERR_WRONG_CHECKSUM : 22,
    ERR_INVALID_DATA : 23,
    ERR_INVALID_HANDLE : 24,
    ERR_INCOMPLETE_DATA : 25,
    ERR_VERSION_OUTDATED : 26,
    ERR_PATH_NOT_FOUND : 27,
    ERR_PATH_ALREADY_EXIST : 28,
    ERR_ADDRESS_ALREADY_USED : 29,
    ERR_STATE_FREEZED : 30,
    ERR_LOCAL_APP_FAILURE : 31,
    ERR_BIND_IOCP_FAILED : 32,
    ERR_NETWORK_START_FAILED : 33,
    ERR_NOT_IMPLEMENTED : 34,
    ERR_CHECKPOINT_FAILED : 35,
    ERR_WRONG_TIMING : 36,
    ERR_NO_NEED_OPERATE : 37,
    ERR_CORRUPTION : 38,
    ERR_TRY_AGAIN : 39,
    ERR_CLUSTER_NOT_FOUND : 40,
    ERR_CLUSTER_ALREADY_EXIST : 41,
    ERR_SERVICE_ALREADY_EXIST : 42,
    ERR_INJECTED : 43,
    ERR_REPLICATION_FAILURE : 44,
    ERR_APP_EXIST : 45,
    ERR_APP_NOT_EXIST : 46,
    ERR_BUSY_CREATING : 47,
    ERR_BUSY_DROPPING : 48,
    ERR_NETWORK_FAILURE : 49,
    ERR_UNDER_RECOVERY : 50,
    ERR_LEARNER_NOT_FOUND : 51,
    ERR_OPERATION_DISABLED : 52,
    ERR_EXPIRED : 53,
    ERR_LOCK_ALREADY_EXIST : 54,
    ERR_HOLD_BY_OTHERS : 55,
    ERR_RECURSIVE_LOCK : 56,
    ERR_NO_OWNER : 57,
    ERR_NODE_ALREADY_EXIST : 58,
    ERR_INCONSISTENT_STATE : 59,
    ERR_ARRAY_INDEX_OUT_OF_RANGE : 60,
    ERR_DIR_NOT_EMPTY : 61,
    ERR_FS_INTERNAL : 62,
    ERR_IGNORE_BAD_DATA : 63,
    ERR_APP_DROPPED : 64,
    ERR_MOCK_INTERNAL : 65,
    ERR_ZOOKEEPER_OPERATION : 66,
    ERR_CHILD_REGISTERED : 67,
    ERR_INGESTION_FAILED : 68,
    ERR_UNAUTHENTICATED : 69,
    ERR_KRB5_INTERNAL : 70,
    ERR_SASL_INTERNAL : 71,
    ERR_SASL_INCOMPLETE : 72,
    ERR_ACL_DENY : 73,
    ERR_SPLITTING : 74,
    ERR_PARENT_PARTITION_MISUSED : 75,
    ERR_CHILD_NOT_READY : 76,
    ERR_DISK_INSUFFICIENT : 77,
    // ERROR_CODE defined by client
    ERR_SESSION_RESET : 78,
    ERR_THREAD_INTERRUPTED : 79,
};

//Default err is ERR_UNKNOWN
var error_code = function(args) {
    this.errno = error_type.ERR_UNKNOWN;
    if(args){
        if(args.errno !== undefined && args.errno !== null){
            this.errno = args.errno;
        }
    }
};
error_code.prototype = {};

error_code.prototype.read = function(protocol){
    this.errno = protocol.readString();
};
error_code.prototype.write = function(protocol){
    protocol.writeString();
};

var task_code = function(args) {
};
task_code.prototype = {};
task_code.prototype.read = function(input) {
    input.readStructBegin();
    while (true)
    {
        var ret = input.readFieldBegin();
        var fname = ret.fname;
        var ftype = ret.ftype;
        var fid = ret.fid;
        if (ftype == Thrift.Type.STOP) {
            break;
        }
        input.skip(ftype);
        input.readFieldEnd();
    }
    input.readStructEnd();
    return;
};

task_code.prototype.write = function(output) {
    output.writeStructBegin('task_code');
    output.writeFieldStop();
    output.writeStructEnd();
    return;
};

var rpc_address = function(args) {
    this.address = 0;
    this.host = null;
    this.port = 0;
    if(args && args.address){
        this.address = args.address;
    }
    if(args && args.host){
        this.host = args.host;
    }
    if(args && args.port){
        this.port = args.port;
    }
};

rpc_address.prototype = {};
rpc_address.prototype.read = function(input){
    let buffer = input.readI64();
    let buf = buffer.buffer;
    let off = buffer.offset;

    let ipNum = (parseInt(buf[0+off]) << 24
        | parseInt(buf[1+off]) << 16
        | parseInt(buf[2+off]) << 8
        | parseInt(buf[3+off])) >>> 0;
    this.host = buf[0+off]+'.'+buf[1+off]+'.'+buf[2+off]+'.'+buf[3+off];
    this.port = parseInt(buf[4+off]) << 8 | parseInt(buf[5+off]) >>> 0;
    this.address = (ipNum << 32) + (parseInt(this.port) << 16) + 1;
};

rpc_address.prototype.write = function(output){
    output.writeI64(new Int64(this.address));
};

//Parse xx.xx.xx.xx:xx ip_address:port format to integer
//Calculate address by ip number and port
rpc_address.prototype.fromString = function(ip_port){
    if(ip_port != undefined && ip_port != null){
        let str = ip_port.split(':');
        if(str.length != 2){
            return false;
        }

        let ip = str[0];
        let port = str[1];
        this.host = ip;
        this.port = port;

        str = ip.split('.');
        if(str.length != 4){
            return false;
        }

        let ipNum = (parseInt(ip[0]) << 24
            | parseInt(ip[1]) << 16
            | parseInt(ip[2]) << 8
            | parseInt(ip[3])) >>> 0;

        this.address = (ipNum << 32) + (parseInt(port) << 16) + 1;

        return true;
    }
};

rpc_address.prototype.invalid = function(){
    return (this.address === 0);
};

rpc_address.prototype.equals = function(other){
    if(other === undefined || other === null){
        return false;
    }
    if(other instanceof rpc_address){
        if(other.address !== this.address){
            return false;
        }else if(other.host !== this.host){
            return false;
        }else if(other.port !== this.port){
            return false;
        }else{
            return true;
        }
    }
    return false;
};

//value, calculate by app_id and partition index
var gpid = function(args) {
    this.value = 0;
    this.app_id = 0;
    this.pidx = 0;
    if(args && args.app_id && args.pidx != undefined){
        this.app_id = args.app_id;
        this.pidx = args.pidx;
        this.value = ((args.pidx) << 32) + args.app_id;
    }
};
gpid.prototype = {};

gpid.prototype.read = function(input){
    let buffer = input.readI64();
    let buf = buffer.buffer;
    this.pidx = buf.readInt32BE(0);
    this.app_id = buf.readInt32BE(4);
    this.value = this.pidx << 32 + this.app_id;
    //console.log('value is %d, app id is %d, pidx is %d', this.value, app_id, pidx);
};

gpid.prototype.write = function(output){
    output.writeI64(this.value);
};

//Get app_id
gpid.prototype.get_app_id = function(){
    return this.app_id;
};

//Get partition index
gpid.prototype.get_pidx = function(){
    return this.pidx;
};

module.exports = {
    blob : blob,
    error_type : error_type,
    error_code : error_code,
    task_code : task_code,
    rpc_address : rpc_address,
    gpid : gpid,
};

