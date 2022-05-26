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

include "base.thrift"

namespace cpp dsn.replication
namespace java dsn.replication

struct partition_configuration
{
    1:base.gpid pid;
    2:i64 ballot;
    3:i32 max_replica_count;
    4:base.rpc_address primary;
    5:list<base.rpc_address> secondaries;
    6:list<base.rpc_address> last_drops;
    7:i64 last_committed_decree;
}

struct query_cfg_request
{
    1:string app_name;
    2:list<i32> partition_indices;
}

struct query_cfg_response
{
    1:base.error_code err;
    2:i32 app_id;
    3:i32 partition_count;
    4:bool is_stateful;
    5:list<partition_configuration> partitions;
}

enum app_status
{
    AS_INVALID,
    AS_AVAILABLE,
    AS_CREATING,
    AS_CREATE_FAILED, // depricated
    AS_DROPPING,
    AS_DROP_FAILED, // depricated
    AS_DROPPED,
    AS_RECALLING
}

struct app_info
{
    1:app_status status = app_status.AS_INVALID;
    2:string app_type;
    3:string app_name;
    4:i32 app_id;
    5:i32 partition_count;
    6:map<string, string> envs;
    7:bool is_stateful;
    8:i32 max_replica_count;
    9:i64 expire_second;
}

struct create_app_options
{
    1:i32 partition_count;
    2:i32 replica_count;
    3:bool success_if_exist;
    4:string app_type;
    5:bool is_stateful;
    6:map<string, string> envs;
}

struct configuration_create_app_request
{
    1:string app_name;
    2:create_app_options options;
}

struct configuration_create_app_response
{
    1:base.error_code err;
    2:i32 appid;
}

struct drop_app_options
{
    1:bool success_if_not_exist;
    2:optional i64 reserve_seconds;
}

struct configuration_drop_app_request
{
    1:string app_name;
    2:drop_app_options options;
}

struct configuration_drop_app_response
{
    1:base.error_code err;
}

struct configuration_recall_app_request
{
    1:i32 app_id;
    2:string new_app_name;
}

struct configuration_recall_app_response
{
    1:base.error_code err;
    2:app_info info;
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
    1:base.error_code        err;
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
    1:base.error_code        err;
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
    1:base.error_code            err;
    2:list<policy_entry>        policys;
    3:list<list<backup_entry>>  backup_infos;
    4:optional string           hint_msg;
}

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
}

struct configuration_query_restore_request
{
    1:i32   restore_app_id;
}

struct configuration_query_restore_response
{
    1:base.error_code        err;
    2:list<base.error_code>  restore_status;
    3:list<i32>              restore_progress;
}

struct configuration_list_apps_request
{
    1:app_status    status = app_status.AS_INVALID;
}

struct configuration_list_apps_response
{
    1:base.error_code  err;
    2:list<app_info>   infos;
}