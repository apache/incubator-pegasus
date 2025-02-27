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

include "dsn.thrift"
include "dsn.layer2.thrift"
include "utils.thrift"

namespace cpp dsn.replication
namespace go admin
namespace java org.apache.pegasus.replication

// Indicate which data of a table needs to be duplicated:
// * FULL: all of the data of the table needs to be duplicated.
// * INCREMENTAL: only incremental data of the table would be duplicated.
enum duplication_mode
{
    FULL = 0,
    INCREMENTAL,
}

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

    // Since v2.6.0.
    // Specify the app name of remote cluster.
    4:optional string remote_app_name;

    // Since v2.6.0.
    // Specify the replica count of remote app.
    // 0 means that the replica count of the remote app would be the same as
    // the source app.
    5:optional i32 remote_replica_count;
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

    // Since v2.6.0.
    //
    // If new duplication is created, this would be requested remote_app_name in
    // duplication_add_request; otherwise, once the duplication has existed, this
    // would be the remote app name with which the duplication has been created.
    //
    // This field could also be used to check if the meta server supports
    // remote_app_name(i.e. the version of meta server must be >= v2.6.0).
    5:optional string remote_app_name;

    // Since v2.6.0.
    //
    // If new duplication is created, this would be requested remote_replica_count in
    // duplication_add_request; otherwise, once the duplication has existed, this would
    // be the remote replica count with which the duplication has been created.
    //
    // This field could also be used to check if the meta server supports
    // remote_replica_count(i.e. the version of meta server must be >= v2.6.0).
    6:optional i32 remote_replica_count;
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

// The states tracking each partition for duplication.
struct duplication_partition_state
{
    // The max decree of this partition that has been confirmed to be received by follower.
    1:i64 confirmed_decree;

    // The max decree that has been committed by this partition.
    2:i64 last_committed_decree;
}

struct duplication_entry
{
    1:i32                  dupid;
    2:duplication_status   status;
    3:string               remote;
    4:i64                  create_ts;

    // Used for syncing duplications with partition-level progress (replica server -> meta server).
    // partition index => confirmed decree.
    5:optional map<i32, i64> progress;

    7:optional duplication_fail_mode fail_mode;

    // Since v2.6.0.
    // For versions >= v2.6.0, this could be specified by client.
    // For versions < v2.6.0, this must be the same with source app_name.
    8:optional string remote_app_name;

    // Since v2.6.0.
    // For versions >= v2.6.0, this could be specified by client.
    // For versions < v2.6.0, this must be the same with source replica_count.
    9:optional i32 remote_replica_count;

    // TODO(wangdan): would be supported later.
    10:optional duplication_mode mode;

    // Used for listing duplications with partition-level details (client -> meta server).
    // partition index => partition states.
    11:optional map<i32, duplication_partition_state> partition_states;
}

// States for the duplications of a table.
struct duplication_app_state
{
    1:i32                               appid;

    // dup id => per-duplication properties
    2:map<i32, duplication_entry>       duplications;

    3:string                            app_name;

    // The number of partitions for this table.
    4:i32                               partition_count;
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

    // Last committed decree from the primary replica of each partition, collected by
    // meta server and used to be compared with duplicating progress of follower table.
    4:optional i64 last_committed_decree;
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
    3:dsn.host_port                                     hp_node;
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

// This request is sent from client to meta server, to list duplications with their
// per-duplication info and progress of each partition for one or multiple tables.
struct duplication_list_request
{
    // The pattern is used to match an app name, whose type is specified by `match_type`.
    1:string                            app_name_pattern;
    2:utils.pattern_match_type          match_type;
}

struct duplication_list_response
{
    1:dsn.error_code                        err;
    2:string                                hint_message;

    // app name => duplications owned by an app
    3:map<string, duplication_app_state>    app_states;
}
