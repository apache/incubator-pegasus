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

include "dsn.thrift"
include "dsn.layer2.thrift"
include "metadata.thrift"

namespace cpp dsn.replication
namespace go radmin

struct query_replica_decree_request
{
    1:dsn.gpid               pid;
    2:dsn.rpc_address        node1;
    3:optional dsn.host_port hp_node1;
}

struct query_replica_decree_response
{
    1:dsn.error_code      err;
    2:i64                 last_decree;
}

struct query_replica_info_request
{
    1:dsn.rpc_address        node1;
    2:optional dsn.host_port hp_node1;
}

struct query_replica_info_response
{
    1:dsn.error_code      err;
    2:list<metadata.replica_info>  replicas;
}

struct disk_info
{
    1:string tag;
    2:string full_dir;
    3:i64 disk_capacity_mb;
    4:i64 disk_available_mb;
    // app_id=>set<gpid>
    5:map<i32,set<dsn.gpid>> holding_primary_replicas;
    6:map<i32,set<dsn.gpid>> holding_secondary_replicas;
}

// This request is sent from client to replica_server.
struct query_disk_info_request
{
    1:dsn.rpc_address        node1;
    2:string                 app_name;
    3:optional dsn.host_port hp_node1;
}

// This response is from replica_server to client.
struct query_disk_info_response
{
    // app not existed will return "ERR_OBJECT_NOT_FOUND", otherwise "ERR_OK"
    1:dsn.error_code err;
    2:i64 total_capacity_mb;
    3:i64 total_available_mb;
    4:list<disk_info> disk_infos;
}

// This request is sent from client to replica_server.
struct replica_disk_migrate_request
{
    1:dsn.gpid pid
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
   1:dsn.error_code err;
   2:optional string hint;
}

enum disk_migration_status
{
    IDLE,
    MOVING,
    MOVED,
    CLOSED
}

enum hotkey_type
{
    READ,
    WRITE
}

enum detect_action
{
    START,
    STOP,
    QUERY
}

struct detect_hotkey_request {
    1: hotkey_type type
    2: detect_action action
    3: dsn.gpid pid;
}

struct detect_hotkey_response {
    // Possible error:
    // - ERR_OK: start/stop hotkey detect succeed
    // - ERR_OBJECT_NOT_FOUND: replica not found
    // - ERR_SERVICE_ALREADY_EXIST: hotkey detection is running now
    1: dsn.error_code err;
    2: optional string err_hint;
    3: optional string hotkey_result;
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
    1: dsn.error_code err;
    2: optional string err_hint;
}

// ONLY FOR GO
// A client to ReplicaServer's administration API.
service replica_client
{
    query_disk_info_response query_disk_info(1:query_disk_info_request req);

    replica_disk_migrate_response disk_migrate(1:replica_disk_migrate_request req);

    add_new_disk_response add_disk(1:add_new_disk_request req);
}
