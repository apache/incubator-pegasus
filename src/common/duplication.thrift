// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

include "../../idl/dsn.thrift"
include "../../idl/dsn.layer2.thrift"

namespace cpp dsn.replication

//  - INIT  -> PREPARE
//  - PREPARE -> APP
//  - APP -> LOG
//  NOTE: Just LOG and PAUSE can be transferred states to each other
//  - LOG -> PAUSE
//  - PAUSE -> LOG
//  - ALL -> REMOVED
enum duplication_status
{
    DS_INIT = 0,
    DS_PREPARE,// replica prepare latest checkpoint for follower
    DS_APP,// follower start duplicate checkpoint
    DS_LOG,// master start batch send plog to follower
    DS_PAUSE,
    DS_REMOVED,
}

// How duplication reacts on permanent failure.
enum duplication_fail_mode
{
    // The default mode. If some permanent failure occurred that makes duplication
    // blocked, it will retry forever until external interference.
    FAIL_SLOW = 0,

    // Skip the writes that failed to duplicate, which means minor data loss on the remote cluster.
    // This will certainly achieve better stability of the system.
    FAIL_SKIP,

    // Stop immediately after it ensures itself unable to duplicate.
    // WARN: this mode kills the server process, replicas on the server will all be effected.
    FAIL_FAST
}

// This request is sent from client to meta.
struct duplication_add_request
{
    1:string  app_name;
    2:string  remote_cluster_name;
    // whether to duplicate checkpoint.
    // - if true, duplication start state=DS_PREPARE,
    // server will use nfs duplicate checkpoint to follower cluster,
    // - if false, duplication start state=DS_LOG,
    // server will replay and send plog mutation to follower cluster derectly
    3:optional bool is_duplicating_checkpoint = true;
}

struct duplication_add_response
{
    // Possible errors:
    // - ERR_INVALID_PARAMETERS:
    //   the address of remote cluster is not well configured in meta sever.
    1:dsn.error_code   err;
    2:i32              appid;
    3:i32              dupid;
    4:optional string  hint;
}

// This request is sent from client to meta.
struct duplication_modify_request
{
    1:string                    app_name;
    2:i32                       dupid;
    3:optional duplication_status status;
    4:optional duplication_fail_mode fail_mode;
}

struct duplication_modify_response
{
    // Possible errors:
    // - ERR_APP_NOT_EXIST: app is not found
    // - ERR_OBJECT_NOT_FOUND: duplication is not found
    // - ERR_BUSY: busy for updating state
    // - ERR_INVALID_PARAMETERS: illegal request
    1:dsn.error_code   err;
    2:i32              appid;
}

struct duplication_entry
{
    1:i32                  dupid;
    2:duplication_status   status;
    3:string               remote;
    4:i64                  create_ts;

    // partition_index => confirmed decree
    5:optional map<i32, i64> progress;

    7:optional duplication_fail_mode fail_mode;
}

// This request is sent from client to meta.
struct duplication_query_request
{
    1:string                    app_name;
}

struct duplication_query_response
{
    // Possible errors:
    // - ERR_APP_NOT_EXIST: app is not found
    1:dsn.error_code             err;
    3:i32                        appid;
    4:list<duplication_entry>    entry_list;
}

struct duplication_confirm_entry
{
    1:i32       dupid;
    2:i64       confirmed_decree;
    3:optional bool checkpoint_prepared = false;
}

// This is an internal RPC sent from replica server to meta.
// It's a server-level RPC.
// After starts up, the replica server periodically collects and uploads confirmed points
// to meta server, so that clients can directly query through meta for the current progress
// of a duplication.
// Moreover, if a primary replica is detected to be crashed, the duplication will be restarted
// on the new primary, continuing from the progress persisted on meta.
// Another function of this rpc is that it synchronizes duplication metadata updates
// (like addition or removal of duplication) between meta and replica.
struct duplication_sync_request
{
    // the address of of the replica server who sends this request
    // TODO(wutao1): remove this field and get the source address by dsn_msg_from_address
    1:dsn.rpc_address                                   node;

    2:map<dsn.gpid, list<duplication_confirm_entry>>    confirm_list;
}

struct duplication_sync_response
{
    // Possible errors:
    // - ERR_OBJECT_NOT_FOUND: node is not found
    1:dsn.error_code                                   err;

    // appid -> map<dupid, dup_entry>
    // this rpc will not return the apps that were not assigned duplication.
    2:map<i32, map<i32, duplication_entry>>            dup_map;
}
