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

namespace go radmin

struct disk_info
{
    1:string tag;
    2:string full_dir;
    3:i64 disk_capacity_mb;
    4:i64 disk_available_mb;
    // app_id=>set<gpid>
    5:map<i32,set<base.gpid>> holding_primary_replicas;
    6:map<i32,set<base.gpid>> holding_secondary_replicas;
}

// This request is sent from client to replica_server.
struct query_disk_info_request
{
    1:base.rpc_address node;
    2:string          app_name;
}

// This response is from replica_server to client.
struct query_disk_info_response
{
    // app not existed will return "ERR_OBJECT_NOT_FOUND", otherwise "ERR_OK"
    1:base.error_code err;
    2:i64 total_capacity_mb;
    3:i64 total_available_mb;
    4:list<disk_info> disk_infos;
}

// This request is sent from client to replica_server.
struct replica_disk_migrate_request
{
    1:base.gpid pid
    // disk tag, for example `ssd1`. `origin_disk` and `target_disk` must be specified in the config of [replication] data_dirs.
    2:string origin_disk;
    3:string target_disk;
}

// This response is from replica_server to client.
struct replica_disk_migrate_response
{
   // Possible error:
   // -ERR_OK: start do replica disk migrate
   // -ERR_BUSY: current replica migration is running
   // -ERR_INVALID_STATE: current replica partition status isn't secondary
   // -ERR_INVALID_PARAMETERS: origin disk is equal with target disk
   // -ERR_OBJECT_NOT_FOUND: replica not found, origin or target disk isn't existed, origin disk doesn't exist current replica
   // -ERR_PATH_ALREADY_EXIST: target disk has existed current replica
   1:base.error_code err;
   2:optional string hint;
}

struct add_new_disk_request {
    // format is "disk_tag:disk_dir,tag2:dir2"
    // for example: "ssd1:/home/work/ssd1"
    1: string disk_str;
}

struct add_new_disk_response {
    // Possible error:
    // - ERR_INVALID_PARAMETERS: invalid disk_str in request
    // - ERR_NODE_ALREADY_EXIST: data_dir is already available
    // - ERR_DIR_NOT_EMPTY: data_dir is not empty
    // - ERR_FILE_OPERATION_FAILED: can't create data_dir or directory can't read/write
    1: base.error_code err;
    2: optional string err_hint;
}

// A client to ReplicaServer's administration API.
service replica_client 
{
    query_disk_info_response query_disk_info(1:query_disk_info_request req);
    
    replica_disk_migrate_response disk_migrate(1:replica_disk_migrate_request req);

    add_new_disk_response add_disk(1:add_new_disk_request req);
}
