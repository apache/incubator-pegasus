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

namespace go admin

/////////////////// Tables Management ////////////////////

struct create_app_options
{
    1:i32              partition_count;
    2:i32              replica_count;
    3:bool             success_if_exist;
    4:string           app_type;
    5:bool             is_stateful;
    6:map<string, string>  envs;
}

struct create_app_request
{
    1:string                   app_name;
    2:create_app_options       options;
}

struct create_app_response
{
    1:base.error_code err;
    2:i32 appid;
}

struct drop_app_options
{
    1:bool             success_if_not_exist;
    2:optional i64     reserve_seconds;
}

struct drop_app_request
{
    1:string                   app_name;
    2:drop_app_options         options;
}

struct drop_app_response
{
    1:base.error_code err;
}

struct recall_app_request
{
    1:i32 app_id;
    2:string new_app_name;
}

struct recall_app_response
{
    1:base.error_code err;
    2:app_info info;
}

enum app_status
{
    AS_INVALID,
    AS_AVAILABLE,
    AS_CREATING,
    AS_CREATE_FAILED, // deprecated
    AS_DROPPING,
    AS_DROP_FAILED, // deprecated
    AS_DROPPED,
    AS_RECALLING
}

struct app_info
{
    1:app_status    status = app_status.AS_INVALID;
    2:string        app_type;
    3:string        app_name;
    4:i32           app_id;
    5:i32           partition_count;
    6:map<string, string> envs;
    7:bool          is_stateful;
    8:i32           max_replica_count;
    9:i64           expire_second;

    // new fields added from v1.11.0
    10:i64          create_second;
    11:i64          drop_second;

    // New fields added from v1.12.0
    // Whether this app is duplicating.
    // If true it should prevent its unconfirmed WAL from being compacted.
    12:optional bool duplicating;

    // New fields for partition split
    // If meta server failed during partition split,
    // child partition is not existed on remote stroage, but partition count changed.
    // We use init_partition_count to handle those child partitions while sync_apps_from_remote_stroage
    13:i32          init_partition_count = -1;

    // New fields for bulk load
    // Whether this app is executing bulk load
    14:optional bool    is_bulk_loading = false;
}

struct list_apps_request
{
    1:app_status status = app_status.AS_INVALID;
}

struct list_apps_response
{
    1:base.error_code err
    2:list<app_info> infos;
}

struct query_app_info_request
{
    1:base.rpc_address meta_server;
}

struct query_app_info_response
{
    1:base.error_code err;
    2:list<app_info> apps;
}

enum app_env_operation
{
    APP_ENV_OP_INVALID,
    APP_ENV_OP_SET,
    APP_ENV_OP_DEL,
    APP_ENV_OP_CLEAR
}

struct update_app_env_request
{
    1:string app_name;
    2:app_env_operation op = app_env_operation.APP_ENV_OP_INVALID;
    3:optional list<string> keys;           // used for set and del
    4:optional list<string> values;         // only used for set
    5:optional string clear_prefix;         // only used for clear
                                            // if clear_prefix is empty then we clear all envs
                                            // else clear the env that key = "clear_prefix.xxx"
}

struct update_app_env_response
{
    1:base.error_code err;
    2:string hint_message;
}

struct start_app_manual_compact_request
{
    1:string        app_name;
    2:optional i64  trigger_time;
    3:optional i32  target_level;
    4:optional bool bottommost;
    5:optional i32  max_running_count;
}

struct start_app_manual_compact_response
{
    // Possible error:
    // - ERR_APP_NOT_EXIST: app not exist
    // - ERR_APP_DROPPED: app has been dropped
    // - ERR_OPERATION_DISABLED: app disable manual compaction
    // - ERR_INVALID_PARAMETERS: invalid manual compaction parameters
    1:base.error_code    err;
    2:string            hint_msg;
}

struct query_app_manual_compact_request
{
    1:string app_name;
}

struct query_app_manual_compact_response
{
    // Possible error:
    // - ERR_APP_NOT_EXIST: app not exist
    // - ERR_APP_DROPPED: app has been dropped
    // - ERR_INVALID_STATE: app is not executing manual compaction
    1:base.error_code    err;
    2:string            hint_msg;
    3:optional i32      progress;
}

/////////////////// Nodes Management ////////////////////

enum node_status
{
    NS_INVALID,
    NS_ALIVE,
    NS_UNALIVE,
}

struct node_info
{
    1:node_status status = node_status.NS_INVALID;
    2:base.rpc_address address;
}

struct list_nodes_request
{
    1:node_status status = node_status.NS_INVALID;
}

struct list_nodes_response
{
    1:base.error_code   err;
    2:list<node_info>  infos;
}

struct cluster_info_request
{
}

struct cluster_info_response
{
    1:base.error_code err;
    2:list<string> keys;
    3:list<string> values;
}

enum meta_function_level
{
    // there are 4 ways to modify the meta-server's status:
    // 0. DDL operation: create/drop/recall table
    // 1. downgrade primary when dectect it is not alive
    // 2. accept primary's update-request to kickoff some secondaries
    // 3. make balancer proposal, which further trigger 2
    // according to these ways, we give meta several active level.

    fl_stopped = 100, //we don't take any action to modify the meta's status, even the DDL operations are not responsed
    fl_blind = 200, //only DDL operations are responsed, 1 2 3 are just ignored
    fl_freezed = 300, //0 1 are responsed, 2 3 ignored
    fl_steady = 400, //0 1 2 are responsed, don't do any balancer
    fl_lively = 500, //full functional
    fl_invalid = 10000
}

// if the level is invalid, we just response the old level of meta without updating it
struct meta_control_request
{
    1:meta_function_level level;
}

struct meta_control_response
{
    1:base.error_code err;
    2:meta_function_level old_level;
}

/////////////////// duplication-related structs ////////////////////

//  - INIT  -> START
//  - START -> PAUSE
//  - START -> REMOVED
//  - PAUSE -> START
//  - PAUSE -> REMOVED
enum duplication_status
{
    DS_INIT = 0,
    DS_START,
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

    // True means to initialize the duplication in DS_PAUSE.
    3:bool    freezed;
}

struct duplication_add_response
{
    // Possible errors:
    // - ERR_INVALID_PARAMETERS:
    //   the address of remote cluster is not well configured in meta sever.
    1:base.error_code  err;
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
    1:base.error_code  err;
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
    1:base.error_code            err;
    3:i32                        appid;
    4:list<duplication_entry>    entry_list;
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

struct query_backup_policy_request
{
    1:list<string>      policy_names;
    2:i32               backup_info_count;
}

struct query_backup_policy_response
{
    1:base.error_code           err;
    2:list<policy_entry>        policys;
    3:list<list<backup_entry>>  backup_infos;
    4:optional string           hint_msg;
}

/////////////////// rebalance-related structs ////////////////////

enum config_type
{
    CT_INVALID,
    CT_ASSIGN_PRIMARY,
    CT_UPGRADE_TO_PRIMARY,
    CT_ADD_SECONDARY,
    CT_UPGRADE_TO_SECONDARY, // not used by meta server
    CT_DOWNGRADE_TO_SECONDARY,
    CT_DOWNGRADE_TO_INACTIVE,
    CT_REMOVE,
    CT_ADD_SECONDARY_FOR_LB,
    CT_PRIMARY_FORCE_UPDATE_BALLOT,
    CT_DROP_PARTITION,
    CT_REGISTER_CHILD
}

struct configuration_proposal_action
{
    1:base.rpc_address target;
    2:base.rpc_address node;
    3:config_type type;
}

enum balancer_request_type
{
    move_primary,
    copy_primary,
    copy_secondary,
}

struct balance_request
{
    1:base.gpid gpid;
    2:list<configuration_proposal_action> action_list;
    3:optional bool force = false;
    4:optional balancer_request_type balance_type;
}

struct balance_response
{
    1:base.error_code err;
}

/////////////////// backup&restore-related structs ////////////////////

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
    1:base.error_code    err;
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
    1:base.error_code                 err;
    2:string                         hint_message;
    3:optional list<backup_item>     backup_items;
}

// using create_app_response to response
struct restore_app_request
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

/////////////////// split-related structs ////////////////////

enum split_status
{
    // idle state
    NOT_SPLIT,
    // A replica is splitting into two replicas, original one called parent, new one called child
    SPLITTING,
    PAUSING,
    PAUSED,
    // After split is successfully cancelled, the state turns into NOT_SPLIT
    CANCELING
}

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
    1:base.error_code    err;
    2:string            hint_msg;
}

enum split_control_type
{
    PAUSE,
    RESTART,
    CANCEL
}

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
    1:base.error_code    err;
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
    1:base.error_code            err;
    2:i32                       new_partition_count;
    3:map<i32,split_status>     status;
    4:optional string           hint_msg;
}

/////////////////// bulk-load-related structs ////////////////////

struct start_bulk_load_request
{
    1:string    app_name;
    2:string    cluster_name;
    3:string    file_provider_type;
    4:string    remote_root_path;
}

struct start_bulk_load_response
{
    // Possible error:
    // - ERR_OK: start bulk load succeed
    // - ERR_APP_NOT_EXIST: app not exist
    // - ERR_APP_DROPPED: app has been dropped
    // - ERR_BUSY: app is already executing bulk load
    // - ERR_INVALID_PARAMETERS: wrong file_provider type
    // - ERR_FILE_OPERATION_FAILED: remote file_provider error
    // - ERR_OBJECT_NOT_FOUND: bulk_load_info not exist on file_provider
    // - ERR_CORRUPTION: bulk_load_info is damaged on file_provider
    // - ERR_INCONSISTENT_STATE: app_id or partition_count inconsistent
    1:base.error_code   err;
    2:string            hint_msg;
}

enum bulk_load_control_type
{
    BLC_PAUSE,
    BLC_RESTART,
    BLC_CANCEL,
    BLC_FORCE_CANCEL
}

struct control_bulk_load_request
{
    1:string                    app_name;
    2:bulk_load_control_type    type;
}

struct control_bulk_load_response
{
    // Possible error:
    // - ERR_APP_NOT_EXIST: app not exist
    // - ERR_APP_DROPPED: app has been dropped
    // - ERR_INACTIVE_STATE: app is not executing bulk load
    // - ERR_INVALID_STATE: current bulk load process can not be paused/restarted/canceled
    1:base.error_code   err;
    2:optional string   hint_msg;
}

enum bulk_load_status
{
    BLS_INVALID,
    BLS_DOWNLOADING,
    BLS_DOWNLOADED,
    BLS_INGESTING,
    BLS_SUCCEED,
    BLS_FAILED,
    BLS_PAUSING,
    BLS_PAUSED,
    BLS_CANCELED
}

enum ingestion_status
{
    IS_INVALID,
    IS_RUNNING,
    IS_SUCCEED,
    IS_FAILED
}

struct partition_bulk_load_state
{
    1:optional i32              download_progress = 0;
    2:optional base.error_code  download_status;
    3:optional ingestion_status ingest_status = ingestion_status.IS_INVALID;
    4:optional bool             is_cleaned_up = false;
    5:optional bool             is_paused = false;
}

struct query_bulk_load_request
{
    1:string   app_name;
}

struct query_bulk_load_response
{
    // Possible error:
    // - ERR_APP_NOT_EXIST: app not exist
    // - ERR_APP_DROPPED: app has been dropped
    // - ERR_INVALID_STATE: app is not executing bulk load
    1:base.error_code                                           err;
    2:string                                                    app_name;
    3:bulk_load_status                                          app_status;
    4:list<bulk_load_status>                                    partitions_status;
    5:i32                                                       max_replica_count;
    // detailed bulk load state for each replica
    6:list<map<base.rpc_address, partition_bulk_load_state>>    bulk_load_states;
    7:optional string                                           hint_msg;
}

struct clear_bulk_load_state_request
{
    1:string                    app_name;
}

struct clear_bulk_load_state_response
{
    // Possible error:
    // - ERR_APP_NOT_EXIST: app not exist
    // - ERR_APP_DROPPED: app has been dropped
    // - ERR_INVALID_STATE: app is executing bulk load
    1:base.error_code    err;
    2:string            hint_msg;
}

// A client to MetaServer's administration API.
service admin_client 
{
    create_app_response create_app(1:create_app_request req);
    
    drop_app_response drop_app(1:drop_app_request req);

    recall_app_response recall_app(1:recall_app_request req);
    
    list_apps_response list_apps(1:list_apps_request req);

    duplication_add_response add_duplication(1: duplication_add_request req);

    duplication_query_response query_duplication(1: duplication_query_request req);

    duplication_modify_response modify_duplication(1: duplication_modify_request req);

    query_app_info_response query_app_info(1: query_app_info_request req);

    update_app_env_response update_app_env(1: update_app_env_request req);

    list_nodes_response list_nodes(1: list_nodes_request req);

    cluster_info_response query_cluster_info(1: cluster_info_request req);

    meta_control_response meta_control(1: meta_control_request req);

    query_backup_policy_response query_backup_policy(1: query_backup_policy_request req);

    balance_response balance(1: balance_request req);

    start_backup_app_response start_backup_app(1: start_backup_app_request req);

    query_backup_status_response query_backup_status(1: query_backup_status_request req);

    create_app_response restore_app(1: restore_app_request req);

    start_partition_split_response start_partition_split(1: start_partition_split_request req);

    query_split_response query_split_status(1: query_split_request req);

    control_split_response control_partition_split(1: control_split_request req);

    start_bulk_load_response start_bulk_load(1: start_bulk_load_request req);

    query_bulk_load_response query_bulk_load_status(1: query_bulk_load_request req);

    control_bulk_load_response control_bulk_load(1: control_bulk_load_request req);

    clear_bulk_load_state_response clear_bulk_load(1: clear_bulk_load_state_request req);

    start_app_manual_compact_response start_manual_compact(1: start_app_manual_compact_request req);

    query_app_manual_compact_response query_manual_compact(1: query_app_manual_compact_request req);
}
