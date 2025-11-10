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

namespace cpp dsn
namespace go replication
namespace java org.apache.pegasus.replication
namespace py pypegasus.replication

struct partition_configuration
{
    1:dsn.gpid                      pid;
    2:i64                           ballot;
    3:i32                           max_replica_count;
    4:dsn.rpc_address               primary;
    5:list<dsn.rpc_address>         secondaries;
    6:list<dsn.rpc_address>         last_drops;
    7:i64                           last_committed_decree;
    8:i32                           partition_flags;
    9:optional dsn.host_port        hp_primary;
    10:optional list<dsn.host_port> hp_secondaries;
    11:optional list<dsn.host_port> hp_last_drops;
}

struct query_cfg_request
{
    1:string           app_name;
    2:list<i32>        partition_indices;
}

// for server version > 1.11.2, if err == ERR_FORWARD_TO_OTHERS,
// then the forward address will be put in partitions[0].primary if exist.
struct query_cfg_response
{
    1:dsn.error_code                err;
    2:i32                           app_id;
    3:i32                           partition_count;
    4:bool                          is_stateful;
    5:list<partition_configuration> partitions;
}

struct request_meta {
    1:i32 app_id;
    2:i32 partition_index;
    3:i32 client_timeout;
    4:i64 partition_hash;
    5:bool is_backup_request;
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
    12:optional bool duplicating = false;

    // New fields for partition split
    // If meta server failed during partition split,
    // child partition is not existed on remote stroage, but partition count changed.
    // We use init_partition_count to handle those child partitions while sync_apps_from_remote_stroage
    13:i32          init_partition_count = -1;

    // New fields for bulk load
    // Whether this app is executing bulk load
    14:optional bool    is_bulk_loading = false;

    // Whether all atomic writes to this table are made idempotent:
    // - true: made idempotent.
    // - false: kept non-idempotent as their respective client requests. 
    15:optional bool    atomic_idempotent = false;
}
