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
namespace java org.apache.pegasus.replication
namespace py pypegasus.replication

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

// for server version > 1.11.2, if err == ERR_FORWARD_TO_OTHERS,
// then the forward address will be put in partitions[0].primary if exist.
struct query_cfg_response
{
    1:base.error_code err;
    2:i32 app_id;
    3:i32 partition_count;
    4:bool is_stateful;
    5:list<partition_configuration> partitions;
}

struct request_meta {
    1:i32 app_id;
    2:i32 partition_index;
    3:i32 client_timeout;
    4:i64 partition_hash;
    5:bool is_backup_request;
}
