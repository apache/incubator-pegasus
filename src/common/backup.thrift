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

struct policy_info
{
    1:string        policy_name;
    2:string        backup_provider_type;
}

// using configuration_create_app_response to response
struct configuration_restore_request
{
    1:string            cluster_name;
    2:string            policy_name;
    3:i64               time_stamp;   // namely backup_id
    4:string            app_name;
    5:i32               app_id;
    6:string            new_app_name;
    7:string            backup_provider_name;
    8:bool              skip_bad_partition;
    9:optional string   restore_path;
}

struct backup_request
{
    1:dsn.gpid              pid;
    2:policy_info           policy;
    3:string                app_name;
    4:i64                   backup_id;
    // user specified backup_path.
    5:optional string       backup_path;
}

struct backup_response
{
    1:dsn.error_code    err;
    2:dsn.gpid          pid;
    3:i32               progress;  // the progress of the cold_backup
    4:string            policy_name;
    5:i64               backup_id;
    6:i64               checkpoint_total_size;
}

// clear all backup resources (including backup contexts and checkpoint dirs) of this policy.
struct backup_clear_request
{
    1:dsn.gpid          pid;
    2:string            policy_name;
}

struct configuration_modify_backup_policy_request
{
    1:string                    policy_name;
    2:optional list<i32>        add_appids;
    3:optional list<i32>        removal_appids;
    4:optional i64              new_backup_interval_sec;
    5:optional i32              backup_history_count_to_keep;
    6:optional bool             is_disable;
    7:optional string           start_time; // restrict the start time of each backup, hour:minute
}

struct configuration_modify_backup_policy_response
{
    1:dsn.error_code        err;
    2:string                hint_message;
}

struct configuration_add_backup_policy_request
{
    1:string            backup_provider_type;
    2:string            policy_name;
    3:list<i32>         app_ids;
    4:i64               backup_interval_seconds;
    5:i32               backup_history_count_to_keep;
    6:string            start_time;
}

struct configuration_add_backup_policy_response
{
    1:dsn.error_code        err;
    2:string                hint_message;
}

struct policy_entry
{
    1:string        policy_name;
    2:string        backup_provider_type;
    3:string        backup_interval_seconds;
    4:set<i32>      app_ids;
    5:i32           backup_history_count_to_keep;
    6:string        start_time;
    7:bool          is_disable;
}

struct backup_entry
{
    1:i64           backup_id;
    2:i64           start_time_ms;
    3:i64           end_time_ms;
    4:set<i32>      app_ids;
}

struct configuration_query_backup_policy_request
{
    1:list<string>      policy_names;
    2:i32               backup_info_count;
}

struct configuration_query_backup_policy_response
{
    1:dsn.error_code            err;
    2:list<policy_entry>        policys;
    3:list<list<backup_entry>>  backup_infos;
    4:optional string           hint_msg;
}

struct configuration_report_restore_status_request
{
    1:dsn.gpid  pid;
    2:dsn.error_code    restore_status;
    3:i32        progress; //[0~1000]
    4:optional string   reason;
}

struct configuration_report_restore_status_response
{
    1:dsn.error_code    err;
}

struct configuration_query_restore_request
{
    1:i32   restore_app_id;
}

struct configuration_query_restore_response
{
    1:dsn.error_code        err;
    2:list<dsn.error_code>  restore_status;
    3:list<i32>             restore_progress;
}

struct start_backup_app_request
{
    1:string             backup_provider_type;
    2:i32                app_id;
    // user specified backup_path.
    3:optional string    backup_path;
}

struct start_backup_app_response
{
    // Possible error:
    // - ERR_INVALID_STATE: app is not available or is backing up
    // - ERR_INVALID_PARAMETERS: backup provider type is invalid
    // - ERR_SERVICE_NOT_ACTIVE: meta doesn't enable backup service
    1:dsn.error_code    err;
    2:string            hint_message;
    3:optional i64      backup_id;
}

struct backup_item
{
    1:i64           backup_id;
    2:string        app_name;
    3:string        backup_provider_type;
    // user specified backup_path.
    4:string        backup_path;
    5:i64           start_time_ms;
    6:i64           end_time_ms;
    7:bool          is_backup_failed;
}

struct query_backup_status_request
{
    1:i32                 app_id;
    2:optional i64        backup_id;
}

struct query_backup_status_response
{
    // Possible error:
    // - ERR_INVALID_PARAMETERS: no available backup for requested app
    // - ERR_SERVICE_NOT_ACTIVE: meta doesn't enable backup service
    1:dsn.error_code                 err;
    2:string                         hint_message;
    3:optional list<backup_item>     backup_items;
}
