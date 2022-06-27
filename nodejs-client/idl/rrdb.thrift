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
include "replication.thrift"

namespace cpp dsn.apps
namespace java dsn.apps

struct update_request
{
    1:base.blob     key;
    2:base.blob     value;
    3:i32           expire_ts_seconds;
}

struct update_response
{
    1:i32           error;
    2:i32           app_id;
    3:i32           partition_index;
    4:i64           decree;
    5:string        server;
}

struct read_response
{
    1:i32           error;
    2:base.blob     value;
    3:i32           app_id;
    4:i32           partition_index;
    6:string        server;
}

struct ttl_response
{
    1:i32           error;
    2:i32           ttl_seconds;
    3:i32           app_id;
    4:i32           partition_index;
    6:string        server;
}

struct count_response
{
    1:i32           error;
    2:i64           count;
    3:i32           app_id;
    4:i32           partition_index;
    6:string        server;
}

struct key_value
{
    1:base.blob     key;
    2:base.blob     value;
}

struct multi_put_request
{
    1:base.blob     hash_key;
    2:list<key_value> kvs; // sort_key => value
    3:i32           expire_ts_seconds;
}

struct multi_remove_request
{
    1:base.blob     hash_key;
    2:list<base.blob> sork_keys; // empty means remove all sortkeys
    3:i64           max_count; // deprecated
}

struct multi_remove_response
{
    1:i32           error;
    2:i64           count; // deleted count
    3:i32           app_id;
    4:i32           partition_index;
    5:i64           decree;
    6:string        server;
}

struct multi_get_request
{
    1:base.blob     hash_key;
    2:list<base.blob> sork_keys; // empty means fetch all sortkeys
    3:i32           max_kv_count; // <= 0 means no limit
    4:i32           max_kv_size; // <= 0 means no limit
    5:bool          no_value; // not return value, only return sortkeys
}

struct multi_get_response
{
    1:i32           error;
    2:list<key_value> kvs; // sort_key => value
    3:i32           app_id;
    4:i32           partition_index;
    6:string        server;
}

struct get_scanner_request
{
    1:base.blob start_key;
    2:base.blob stop_key;
    3:bool      start_inclusive;
    4:bool      stop_inclusive;
    5:i32       batch_size;
}

struct scan_request
{
    1:i64           context_id;
}

struct scan_response
{
    1:i32           error;
    2:list<key_value> kvs;
    3:i64           context_id;
    4:i32           app_id;
    5:i32           partition_index;
    6:string        server;
}

service rrdb
{
    update_response put(1:update_request update);
    update_response multi_put(1:multi_put_request request);
    update_response remove(1:base.blob key);
    multi_remove_response multi_remove(1:multi_remove_request request);
    read_response get(1:base.blob key);
    multi_get_response multi_get(1:multi_get_request request);
    count_response sortkey_count(1:base.blob hash_key);
    ttl_response ttl(1:base.blob key);
    scan_response get_scanner(1:get_scanner_request request);
    scan_response scan(1:scan_request request);
    oneway void clear_scanner(1:i64 context_id);
}

service meta
{
    replication.query_cfg_response query_cfg(1:replication.query_cfg_request query);
    replication.configuration_create_app_response create_app(1:replication.configuration_create_app_request req);
    replication.configuration_list_apps_response list_apps(1:replication.configuration_list_apps_request req);
    replication.configuration_drop_app_response drop_app(1:replication.configuration_drop_app_request req);
    replication.configuration_recall_app_response recall_app(1:replication.configuration_recall_app_request req);

    replication.configuration_query_backup_policy_response query_backup_policy(1:replication.configuration_query_backup_policy_request req);
    replication.configuration_add_backup_policy_response add_backup_policy(1:replication.configuration_add_backup_policy_request req);
    replication.configuration_modify_backup_policy_response modify_backup_policy(1:replication.configuration_modify_backup_policy_request req);

    replication.configuration_create_app_response start_restore(1:replication.configuration_restore_request req);
    replication.configuration_query_restore_response query_restore(1:replication.configuration_query_restore_request req);
}
