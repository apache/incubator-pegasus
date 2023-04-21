/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015 Microsoft Corporation
 *
 * -=- Robust Distributed System Nucleus (rDSN) -=-
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

include "backup.thrift"
include "bulk_load.thrift"
include "dsn.thrift"
include "dsn.layer2.thrift"
include "duplication.thrift"
include "metadata.thrift"
include "partition_split.thrift"

namespace cpp dsn.replication
namespace go admin
namespace java org.apache.pegasus.replication

// This file contains the administration RPCs from client to MetaServer.

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

enum node_status
{
    NS_INVALID,
    NS_ALIVE,
    NS_UNALIVE,
}

// primary | secondary(upgrading) (w/ new config) => meta server
// also served as proposals from meta server to replica servers
struct configuration_update_request
{
    1:dsn.layer2.app_info                 info;
    2:dsn.layer2.partition_configuration  config;
    3:config_type              type = config_type.CT_INVALID;
    4:dsn.rpc_address          node;
    5:dsn.rpc_address          host_node; // deprecated, only used by stateless apps

    // Used for partition split
    // if replica is splitting (whose split_status is not NOT_SPLIT)
    // the `meta_split_status` will be set
    // only used when on_config_sync
    6:optional metadata.split_status    meta_split_status;
}

// meta server (config mgr) => primary | secondary (downgrade) (w/ new config)
struct configuration_update_response
{
    1:dsn.error_code           err;
    2:dsn.layer2.partition_configuration  config;
}

// client => meta server
struct replica_server_info
{
    // replica server can report its geo position
    // possible tags may be:
    // geo_tags["host"] = hostid;
    // geo_tags["rack"] = rackid
    // geo_tags["datacenter"] = datacenterid
    // geo_tags["city"] = cityid
    1:map<string, string> geo_tags;
    2:i64 total_capacity_mb;
}

struct configuration_query_by_node_request
{
    1:dsn.rpc_address  node;
    2:optional list<metadata.replica_info> stored_replicas;
    3:optional replica_server_info info;
}

struct configuration_query_by_node_response
{
    1:dsn.error_code err;
    2:list<configuration_update_request> partitions;
    3:optional list<metadata.replica_info> gc_replicas;
}

struct configuration_recovery_request
{
    1:list<dsn.rpc_address> recovery_set;
    2:bool skip_bad_nodes;
    3:bool skip_lost_partitions;
}

struct configuration_recovery_response
{
    1:dsn.error_code err;
    2:string hint_message;
}

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

struct configuration_create_app_request
{
    1:string                   app_name;
    2:create_app_options       options;
}

// meta server => client
struct configuration_create_app_response
{
    1:dsn.error_code   err;
    2:i32              appid;
}

struct drop_app_options
{
    1:bool             success_if_not_exist;
    2:optional i64     reserve_seconds;
}

struct configuration_drop_app_request
{
    1:string                   app_name;
    2:drop_app_options         options;
}

struct configuration_drop_app_response
{
    1:dsn.error_code   err;
}

struct configuration_rename_app_request
{
    1:string old_app_name;
    2:string new_app_name;
}

struct configuration_rename_app_response
{
    1:dsn.error_code err;
    2:string hint_message;
}

struct configuration_recall_app_request
{
    1:i32 app_id;
    2:string new_app_name;
}

struct configuration_recall_app_response
{
    1:dsn.error_code err;
    2:dsn.layer2.app_info info;
}

struct configuration_list_apps_request
{
    1:dsn.layer2.app_status    status = app_status.AS_INVALID;
}

struct configuration_list_apps_response
{
    1:dsn.error_code              err;
    2:list<dsn.layer2.app_info>   infos;
}

struct query_app_info_request
{
    1:dsn.rpc_address meta_server;
}

struct query_app_info_response
{
    1:dsn.error_code err;
    2:list<dsn.layer2.app_info> apps;
}

enum app_env_operation
{
    APP_ENV_OP_INVALID,
    APP_ENV_OP_SET,
    APP_ENV_OP_DEL,
    APP_ENV_OP_CLEAR
}

struct configuration_update_app_env_request
{
    1:string app_name;
    2:app_env_operation op = app_env_operation.APP_ENV_OP_INVALID;
    3:optional list<string> keys;           // used for set and del
    4:optional list<string> values;         // only used for set
    5:optional string clear_prefix;         // only used for clear
                                            // if clear_prefix is empty then we clear all envs
                                            // else clear the env that key = "clear_prefix.xxx"
}

struct configuration_update_app_env_response
{
    1:dsn.error_code err;
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
    1:dsn.error_code    err;
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
    1:dsn.error_code    err;
    2:string            hint_msg;
    3:optional i32      progress;
}

/////////////////// Nodes Management ////////////////////

struct node_info
{
    1:node_status      status = node_status.NS_INVALID;
    2:dsn.rpc_address  address;
}

struct configuration_list_nodes_request
{
    1:node_status              status = node_status.NS_INVALID;
}

struct configuration_list_nodes_response
{
    1:dsn.error_code   err;
    2:list<node_info>  infos;
}

struct configuration_cluster_info_request
{
}

struct configuration_cluster_info_response
{
    1:dsn.error_code   err;
    2:list<string>     keys;
    3:list<string>     values;
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
struct configuration_meta_control_request
{
    1:meta_function_level level;
}

struct configuration_meta_control_response
{
    1:dsn.error_code err;
    2:meta_function_level old_level;
}

enum balancer_request_type
{
    move_primary,
    copy_primary,
    copy_secondary,
}

struct configuration_proposal_action
{
    1:dsn.rpc_address target;
    2:dsn.rpc_address node;
    3:config_type type;

    // depricated now
    // new fields of this struct should start with 5
    // 4:i64 period_ts;
}

struct configuration_balancer_request
{
    1:dsn.gpid gpid;
    2:list<configuration_proposal_action> action_list;
    3:optional bool force = false;
    4:optional balancer_request_type balance_type;
}

struct configuration_balancer_response
{
    1:dsn.error_code err;
}

struct ddd_diagnose_request
{
    // app_id == -1 means return all partitions of all apps
    // app_id != -1 && partition_id == -1 means return all partitions of specified app
    // app_id != -1 && partition_id != -1 means return specified partition
    1:dsn.gpid pid;
}

struct ddd_node_info
{
    1:dsn.rpc_address node;
    2:i64             drop_time_ms;
    3:bool            is_alive; // if the node is alive now
    4:bool            is_collected; // if replicas has been collected from this node
    5:i64             ballot; // collected && ballot == -1 means replica not exist on this node
    6:i64             last_committed_decree;
    7:i64             last_prepared_decree;
}

struct ddd_partition_info
{
    1:dsn.layer2.partition_configuration config;
    2:list<ddd_node_info>                dropped;
    3:string                             reason;
}

struct ddd_diagnose_response
{
    1:dsn.error_code           err;
    2:list<ddd_partition_info> partitions;
}

struct configuration_get_max_replica_count_request
{
    1:string                    app_name;
}

struct configuration_get_max_replica_count_response
{
    1:dsn.error_code            err;
    2:i32                       max_replica_count;
    3:string                    hint_message;
}

struct configuration_set_max_replica_count_request
{
    1:string                    app_name;
    2:i32                       max_replica_count;
}

struct configuration_set_max_replica_count_response
{
    1:dsn.error_code            err;
    2:i32                       old_max_replica_count;
    3:string                    hint_message;
}

// ONLY FOR GO
// A client to MetaServer's administration API.
service admin_client
{
    configuration_create_app_response create_app(1:configuration_create_app_request req);

    configuration_drop_app_response drop_app(1:configuration_drop_app_request req);

    configuration_recall_app_response recall_app(1:configuration_recall_app_request req);

    configuration_list_apps_response list_apps(1:configuration_list_apps_request req);

    duplication.duplication_add_response add_duplication(1: duplication.duplication_add_request req);

    duplication.duplication_query_response query_duplication(1: duplication.duplication_query_request req);

    duplication.duplication_modify_response modify_duplication(1: duplication.duplication_modify_request req);

    query_app_info_response query_app_info(1: query_app_info_request req);

    configuration_update_app_env_response update_app_env(1: configuration_update_app_env_request req);

    configuration_list_nodes_response list_nodes(1: configuration_list_nodes_request req);

    configuration_cluster_info_response query_cluster_info(1: configuration_cluster_info_request req);

    configuration_meta_control_response meta_control(1: configuration_meta_control_request req);

    backup.configuration_query_backup_policy_response query_backup_policy(1: backup.configuration_query_backup_policy_request req);

    configuration_balancer_response balance(1: configuration_balancer_request req);

    backup.start_backup_app_response start_backup_app(1: backup.start_backup_app_request req);

    backup.query_backup_status_response query_backup_status(1: backup.query_backup_status_request req);

    configuration_create_app_response restore_app(1: backup.configuration_restore_request req);

    partition_split.start_partition_split_response start_partition_split(1: partition_split.start_partition_split_request req);

    partition_split.query_split_response query_split_status(1: partition_split.query_split_request req);

    partition_split.control_split_response control_partition_split(1: partition_split.control_split_request req);

    bulk_load.start_bulk_load_response start_bulk_load(1: bulk_load.start_bulk_load_request req);

    bulk_load.query_bulk_load_response query_bulk_load_status(1: bulk_load.query_bulk_load_request req);

    bulk_load.control_bulk_load_response control_bulk_load(1: bulk_load.control_bulk_load_request req);

    bulk_load.clear_bulk_load_state_response clear_bulk_load(1: bulk_load.clear_bulk_load_state_request req);

    start_app_manual_compact_response start_manual_compact(1: start_app_manual_compact_request req);

    query_app_manual_compact_response query_manual_compact(1: query_app_manual_compact_request req);
}
