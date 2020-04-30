// Copyright (c) 2017-present, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include "dist/replication/meta_server/meta_service.h"
#include "dist/replication/meta_server/server_state.h"

namespace dsn {
namespace replication {

///
/// bulk load path on remote storage:
/// <cluster_root>/bulk_load/<app_id> -> app_bulk_load_info
/// <cluster_root>/bulk_load/<app_id>/<pidx> -> partition_bulk_load_info
///
struct app_bulk_load_info
{
    int32_t app_id;
    int32_t partition_count;
    std::string app_name;
    std::string cluster_name;
    std::string file_provider_type;
    bulk_load_status::type status;
    DEFINE_JSON_SERIALIZATION(
        app_id, partition_count, app_name, cluster_name, file_provider_type, status)
};

struct partition_bulk_load_info
{
    bulk_load_status::type status;
    bulk_load_metadata metadata;
    DEFINE_JSON_SERIALIZATION(status, metadata)
};

// Used for remote file provider
struct bulk_load_info
{
    int32_t app_id;
    std::string app_name;
    int32_t partition_count;
    DEFINE_JSON_SERIALIZATION(app_id, app_name, partition_count)
};

///
/// Bulk load process:
/// when client sent `start_bulk_load_rpc` to meta server to start bulk load,
/// meta server create bulk load structures on remote storage, and send `RPC_BULK_LOAD` rpc to
/// each primary replica periodically until bulk load succeed or failed. whole process below:
///
///           start bulk load
///                  |
///                  v
///          is_bulk_loading = true
///                  |
///                  v
///     create bulk load info on remote storage
///                  |
///         Err      v
///     ---------Downloading <---------|
///     |            |                 |
///     |            v         Err     |
///     |        Downloaded  --------->|
///     |            |                 |
///     | IngestErr  v         Err     |
///     |<------- Ingesting  --------->|
///     |            |                 |
///     v            v         Err     |
///   Failed       Succeed   --------->|
///     |            |
///     v            v
///    remove bulk load info on remote storage
///                  |
///                  v
///         is_bulk_loading = false
///                  |
///                  v
///            bulk load end

class bulk_load_service
{
public:
    explicit bulk_load_service(meta_service *meta_svc, const std::string &bulk_load_dir);

    void initialize_bulk_load_service();

private:
    void create_bulk_load_root_dir(error_code &err, task_tracker &tracker);

    ///
    /// sync bulk load states from remote storage
    /// called when service initialized or meta server leader switch
    ///
    void sync_apps_bulk_load_from_remote_stroage(error_code &err, task_tracker &tracker);

    ///
    /// try to continue bulk load according to states from remote stroage
    /// called when service initialized or meta server leader switch
    ///
    void try_to_continue_bulk_load();

private:
    friend class bulk_load_service_test;

    meta_service *_meta_svc;
    server_state *_state;

    zrwlock_nr &app_lock() const { return _state->_lock; }
    zrwlock_nr _lock; // bulk load states lock

    const std::string _bulk_load_root; // <cluster_root>/bulk_load

    /// bulk load states
    std::unordered_set<int32_t> _bulk_load_app_id;
    std::unordered_map<app_id, app_bulk_load_info> _app_bulk_load_info;

    std::unordered_map<app_id, int32_t> _apps_in_progress_count;
    std::unordered_map<app_id, bool> _apps_pending_sync_flag;

    std::unordered_map<gpid, partition_bulk_load_info> _partition_bulk_load_info;
    std::unordered_map<gpid, bool> _partitions_pending_sync_flag;
};

} // namespace replication
} // namespace dsn
