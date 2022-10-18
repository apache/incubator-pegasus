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
include "metadata.thrift"

namespace cpp dsn.replication

// client to meta server to start partition split
struct start_partition_split_request
{
    1:string    app_name;
    2:i32       new_partition_count;
}

struct start_partition_split_response
{
    // Possible errors:
    // - ERR_APP_NOT_EXIST: app not exist
    // - ERR_APP_DROPPED: app has been dropped
    // - ERR_INVALID_PARAMETERS: if the given new_partition_count != old_partition_count * 2
    // - ERR_BUSY - if app is already executing partition split
    1:dsn.error_code    err;
    2:string            hint_msg;
}

enum split_control_type
{
    PAUSE,
    RESTART,
    CANCEL
}

// client to meta server to control partition split
// support three control type: pause, restart, cancel
struct control_split_request
{
    1:string                app_name;
    2:split_control_type    control_type
    // for pause, parent_pidx >= 0, pause specific partition, parent_pidx = -1, pause all splitting partition
    // for restart, parent_pidx >= 0, restart specific partition, parent_pidx = -1, restart all paused partition
    // for cancel, parent_pidx will always be -1
    3:i32                   parent_pidx;
    // only used for cancel
    4:optional i32          old_partition_count;
}

struct control_split_response
{
    // Possible errors:
    // - ERR_APP_NOT_EXIST: app not exist
    // - ERR_APP_DROPPED: app has been dropped
    // - ERR_INVALID_STATE: wrong partition split_status
    // - ERR_INVALID_PARAMETERS: invalid parent_pidx or old_partition_count
    // - ERR_CHILD_REGISTERED: child partition has been registered, pause partition split or cancel split failed
    1:dsn.error_code    err;
    2:optional string   hint_msg;
}

// client->meta server to query partition split status
struct query_split_request
{
    1:string    app_name;
}

struct query_split_response
{
    // Possible errors:
    // - ERR_APP_NOT_EXIST: app not exist
    // - ERR_APP_DROPPED: app has been dropped
    // - ERR_INVALID_STATE: app is not splitting
    1:dsn.error_code            err;
    2:i32                       new_partition_count;
    3:map<i32,metadata.split_status>     status;
    4:optional string           hint_msg;
}

// child to primary parent, notifying that itself has caught up with parent
struct notify_catch_up_request
{
    1:dsn.gpid          parent_gpid;
    2:dsn.gpid          child_gpid;
    3:i64               child_ballot;
    4:dsn.rpc_address   child_address;
}

struct notify_cacth_up_response
{
    // Possible errors:
    // - ERR_OBJECT_NOT_FOUND: replica can not be found
    // - ERR_INVALID_STATE: replica is not primary or ballot not match or child_gpid not match
    1:dsn.error_code    err;
}

// primary parent -> child replicas to update partition count
struct update_child_group_partition_count_request
{
    1:dsn.rpc_address   target_address;
    2:i32               new_partition_count;
    3:dsn.gpid          child_pid;
    4:i64               ballot;
}

struct update_child_group_partition_count_response
{
    // Possible errors:
    // - ERR_OBJECT_NOT_FOUND: replica can not be found
    // - ERR_VERSION_OUTDATED: request is outdated
    1:dsn.error_code    err;
}

// primary parent -> meta server, register child on meta_server
struct register_child_request
{
    1:dsn.layer2.app_info                   app;
    2:dsn.layer2.partition_configuration    parent_config;
    3:dsn.layer2.partition_configuration    child_config;
    4:dsn.rpc_address                       primary_address;
}

struct register_child_response
{
    // Possible errors:
    // - ERR_INVALID_VERSION: request is out-dated
    // - ERR_CHILD_REGISTERED: child has been registered
    // - ERR_IO_PENDING: meta is executing another remote sync task
    // - ERR_INVALID_STATE: parent partition is not splitting
    1:dsn.error_code                        err;
    2:dsn.layer2.app_info                   app;
    3:dsn.layer2.partition_configuration    parent_config;
    4:dsn.layer2.partition_configuration    child_config;
}

// primary -> meta to report pause or cancel split succeed
struct notify_stop_split_request
{
    1:string        app_name;
    2:dsn.gpid      parent_gpid;
    3:metadata.split_status  meta_split_status;
    4:i32           partition_count;
}

struct notify_stop_split_response
{
    // Possible errors:
    // - ERR_INVALID_VERSION: request is out-dated
    1:dsn.error_code    err;
}

// primary parent -> meta server, query child state on meta server
struct query_child_state_request
{
    1:string    app_name
    2:dsn.gpid  pid;
    3:i32       partition_count;
}

struct query_child_state_response
{
    // Possible errors:
    // - ERR_INVALID_STATE: app is not splitting or partition split has been canceled
    1:dsn.error_code                                err;
    2:optional i32                                  partition_count;
    3:optional dsn.layer2.partition_configuration   child_config;
}
