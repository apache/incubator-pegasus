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

include "dsn.thrift"
include "dsn.layer2.thrift"
include "metadata.thrift"

namespace cpp dsn.replication
namespace go admin
namespace java org.apache.pegasus.replication

// app partition bulk load status
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

struct bulk_load_metadata
{
    1:list<metadata.file_meta>   files;
    2:i64               file_total_size;
}

// client -> meta, start bulk load
struct start_bulk_load_request
{
    1:string    app_name;
    2:string    cluster_name;
    3:string    file_provider_type;
    4:string    remote_root_path;
    5:bool      ingest_behind = false;
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
    1:dsn.error_code    err;
    2:string            hint_msg;
}

struct partition_bulk_load_state
{
    1:optional i32              download_progress = 0;
    2:optional dsn.error_code   download_status;
    3:optional ingestion_status ingest_status = ingestion_status.IS_INVALID;
    4:optional bool             is_cleaned_up = false;
    5:optional bool             is_paused = false;
}

// meta server -> replica server
struct bulk_load_request
{
    1:dsn.gpid          pid;
    2:string            app_name;
    3:dsn.rpc_address   primary_addr;
    4:string            remote_provider_name;
    5:string            cluster_name;
    6:i64               ballot;
    7:bulk_load_status  meta_bulk_load_status;
    8:bool              query_bulk_load_metadata;
    9:string            remote_root_path;
}

struct bulk_load_response
{
    // Possible error:
    // - ERR_OBJECT_NOT_FOUND: replica not found
    // - ERR_INVALID_STATE: replica has invalid state
    // - ERR_BUSY: node has enough replica executing bulk load downloading
    // - ERR_FILE_OPERATION_FAILED: local file system error during bulk load downloading
    // - ERR_FS_INTERNAL: remote file provider error during bulk load downloading
    // - ERR_CORRUPTION: metadata corruption during bulk load downloading
    1:dsn.error_code                                    err;
    2:dsn.gpid                                          pid;
    3:string                                            app_name;
    4:bulk_load_status                                  primary_bulk_load_status;
    5:map<dsn.rpc_address, partition_bulk_load_state>   group_bulk_load_state;
    6:optional bulk_load_metadata                       metadata;
    7:optional i32                                      total_download_progress;
    8:optional bool                                     is_group_ingestion_finished;
    9:optional bool                                     is_group_bulk_load_context_cleaned_up;
    10:optional bool                                    is_group_bulk_load_paused;
}

// primary -> secondary
struct group_bulk_load_request
{
    1:string                        app_name;
    2:dsn.rpc_address               target_address;
    3:metadata.replica_configuration         config;
    4:string                        provider_name;
    5:string                        cluster_name;
    6:bulk_load_status              meta_bulk_load_status;
    7:string                        remote_root_path;
}

struct group_bulk_load_response
{
    // Possible error:
    // - ERR_OBJECT_NOT_FOUND: replica not found
    // - ERR_VERSION_OUTDATED: request out-dated
    // - ERR_INVALID_STATE: replica has invalid state
    // - ERR_BUSY: node has enough replica executing bulk load downloading
    // - ERR_FILE_OPERATION_FAILED: local file system error during bulk load downloading
    // - ERR_FS_INTERNAL: remote file provider error during bulk load downloading
    // - ERR_CORRUPTION: metadata corruption during bulk load downloading
    1:dsn.error_code            err;
    2:bulk_load_status          status;
    3:partition_bulk_load_state bulk_load_state;
}

// meta server -> replica server
struct ingestion_request
{
    1:string                app_name;
    2:bulk_load_metadata    metadata;
    3:bool                  ingest_behind;
    4:i64                   ballot;
    5:bool                  verify_before_ingest;
}

struct ingestion_response
{
    // Possible errors:
    // - ERR_TRY_AGAIN: retry ingestion
    1:dsn.error_code    err;
    // rocksdb ingestion error code
    2:i32               rocksdb_error;
}

enum bulk_load_control_type
{
    BLC_PAUSE,
    BLC_RESTART,
    BLC_CANCEL,
    BLC_FORCE_CANCEL
}

// client -> meta server, pause/restart/cancel/force_cancel bulk load
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
    1:dsn.error_code    err;
    2:optional string   hint_msg;
}

struct query_bulk_load_request
{
    1:string   app_name;
}

struct query_bulk_load_response
{
    // Possible error:
    // - ERR_OK
    // - ERR_APP_NOT_EXIST: app not exist
    // - ERR_APP_DROPPED: app has been dropped
    // - ERR_FILE_OPERATION_FAILED: local file system error
    // - ERR_FS_INTERNAL: remote file system error
    // - ERR_CORRUPTION: file not exist or damaged
    // - ERR_INGESTION_FAILED: ingest failed
    // - ERR_RETRY_EXHAUSTED: retry too many times
    1:dsn.error_code                                        err;
    2:string                                                app_name;
    3:bulk_load_status                                      app_status;
    4:list<bulk_load_status>                                partitions_status;
    5:i32                                                   max_replica_count;
    // detailed bulk load state for each replica
    6:list<map<dsn.rpc_address, partition_bulk_load_state>> bulk_load_states;
    7:optional string                                       hint_msg;
    8:optional bool                                         is_bulk_loading;
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
    1:dsn.error_code    err;
    2:string            hint_msg;
}
