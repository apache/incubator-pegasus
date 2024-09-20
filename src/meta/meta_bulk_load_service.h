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

#pragma once

#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <ostream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "bulk_load_types.h"
#include "common/bulk_load_common.h"
#include "common/gpid.h"
#include "common/json_helper.h"
#include "common/replication_other_types.h"
#include "meta/meta_state_service_utils.h"
#include "meta_bulk_load_ingestion_context.h"
#include "rpc/rpc_host_port.h"
#include "server_state.h"
#include "task/task_tracker.h"
#include "utils/error_code.h"
#include "utils/flags.h"
#include "utils/zlocks.h"

DSN_DECLARE_uint32(bulk_load_max_rollback_times);
DSN_DECLARE_bool(enable_concurrent_bulk_load);

namespace dsn {
class partition_configuration;

namespace replication {
class app_state;
class config_context;
class meta_service;

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
    std::string remote_root_path;
    bool ingest_behind;
    bool is_ever_ingesting;
    error_code bulk_load_err;
    DEFINE_JSON_SERIALIZATION(app_id,
                              partition_count,
                              app_name,
                              cluster_name,
                              file_provider_type,
                              status,
                              remote_root_path,
                              ingest_behind,
                              is_ever_ingesting,
                              bulk_load_err)
};

struct partition_bulk_load_info
{
    bulk_load_status::type status;
    bulk_load_metadata metadata;
    bool ever_ingest_succeed;
    std::vector<host_port> host_ports;
    DEFINE_JSON_SERIALIZATION(status, metadata, ever_ingest_succeed, host_ports)
};

// Used for remote file provider
struct bulk_load_info
{
    int32_t app_id;
    std::string app_name;
    int32_t partition_count;
    bulk_load_info(int32_t id = 0, const std::string &name = "", int32_t pcount = 0)
        : app_id(id), app_name(name), partition_count(pcount)
    {
    }
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
/// remove previous bulk load info on remote storage
///                  |
///                  v
///          is_bulk_loading = true
///                  |
///                  v
///     create bulk load info on remote storage
///                  |
///         Err      v
///     ---------Downloading <---------|
///     |  Too many  |                 |
///     |  rollback  |                 |
///     |            v         Err     |
///     |        Downloaded  --------->|
///     |            |                 |
///     | IngestErr  v         Err     |
///     |<------- Ingesting  --------->|
///     |            |
///     v            v
///   Failed       Succeed
///     |            |
///     |            v
///     |---> is_bulk_loading = false
///                  |
///                  v
///            bulk load end

class bulk_load_service
{
public:
    explicit bulk_load_service(meta_service *meta_svc, const std::string &bulk_load_dir);

    void initialize_bulk_load_service();

    // client -> meta server to start bulk load
    void on_start_bulk_load(start_bulk_load_rpc rpc);
    // client -> meta server to pause/restart/cancel/force_cancel bulk load
    void on_control_bulk_load(control_bulk_load_rpc rpc);
    // client -> meta server to query bulk load status
    void on_query_bulk_load_status(query_bulk_load_rpc rpc);
    // client -> meta server to clear bulk load state
    void on_clear_bulk_load(clear_bulk_load_rpc rpc);

    // Called by `sync_apps_from_remote_storage`, check bulk load state consistency
    // Handle inconsistent conditions below:
    // - app is_bulk_loading = true, app_bulk_load_info not existed, set is_bulk_loading=false
    // - app is_bulk_loading = false, app_bulk_load_info existed, remove useless app bulk load on
    // remote storage
    void check_app_bulk_load_states(std::shared_ptr<app_state> app, bool is_app_bulk_loading);

private:
    // Called by `on_start_bulk_load`, check request params
    // - ERR_OK: pass params check
    // - ERR_INVALID_PARAMETERS: wrong file_provider type
    // - ERR_FILE_OPERATION_FAILED: file_provider error
    // - ERR_OBJECT_NOT_FOUND: bulk_load_info not exist, may wrong cluster_name or app_name
    // - ERR_CORRUPTION: bulk_load_info is damaged on file_provider
    // - ERR_INCONSISTENT_STATE: app_id or partition_count inconsistent
    error_code check_bulk_load_request_params(const start_bulk_load_request &request,
                                              const int32_t app_id,
                                              const int32_t partition_count,
                                              const std::map<std::string, std::string> &envs,
                                              std::string &hint_msg);

    void do_start_app_bulk_load(std::shared_ptr<app_state> app, start_bulk_load_rpc rpc);

    void do_clear_app_bulk_load_result(int32_t app_id, clear_bulk_load_rpc rpc);

    // Called by `partition_bulk_load` and `partition_ingestion`
    // check partition status before sending partition_bulk_load_request and
    // partition_ingestion_request
    bool check_partition_status(
        const std::string &app_name,
        const gpid &pid,
        bool always_unhealthy_check,
        const std::function<void(const std::string &, const gpid &)> &retry_function,
        /*out*/ partition_configuration &pc);

    void partition_bulk_load(const std::string &app_name, const gpid &pid);

    void on_partition_bulk_load_reply(error_code err,
                                      const bulk_load_request &request,
                                      const bulk_load_response &response);

    // if app is still in bulk load, resend bulk_load_request to primary after interval seconds
    void try_resend_bulk_load_request(const std::string &app_name, const gpid &pid);

    void handle_app_downloading(const bulk_load_response &response, const host_port &primary);

    void handle_app_ingestion(const bulk_load_response &response, const host_port &primary);

    // when app status is `succeed, `failed`, `canceled`, meta and replica should cleanup bulk load
    // states
    void handle_bulk_load_finish(const bulk_load_response &response, const host_port &primary);

    void handle_app_pausing(const bulk_load_response &response, const host_port &primary);

    // app not existed or not available during bulk load
    void handle_app_unavailable(int32_t app_id, const std::string &app_name);

    void try_rollback_to_downloading(const std::string &app_name, const gpid &pid);

    void handle_bulk_load_failed(int32_t app_id, error_code err);

    // Called when app bulk load status update to ingesting
    // create ingestion_request and send it to primary
    void partition_ingestion(const std::string &app_name, const gpid &pid);

    void send_ingestion_request(const std::string &app_name,
                                const gpid &pid,
                                const host_port &primary,
                                const ballot &meta_ballot);

    void on_partition_ingestion_reply(error_code err,
                                      const ingestion_response &&resp,
                                      const std::string &app_name,
                                      const gpid &pid,
                                      const host_port &primary);

    // Called by `partition_ingestion`
    // - true : this partition has ever executed ingestion succeed, no need to send ingestion
    // request
    // - false: this partition has not executed ingestion or executed ingestion failed
    bool check_ever_ingestion_succeed(const partition_configuration &pc,
                                      const std::string &app_name,
                                      const gpid &pid);

    // is_reset_all
    // - true  : reset all states in memory
    // - false : keep the bulk load results in memory, reset others
    void reset_local_bulk_load_states_unlocked(int32_t app_id,
                                               const std::string &app_name,
                                               bool is_reset_all);
    void
    reset_local_bulk_load_states(int32_t app_id, const std::string &app_name, bool is_reset_all);

    ///
    /// ingestion_context functions
    ///
    bool try_partition_ingestion(const partition_configuration &pc, const config_context &cc)
    {
        return _ingestion_context->try_partition_ingestion(pc, cc);
    }

    void finish_ingestion(const gpid &pid) { _ingestion_context->remove_partition(pid); }

    const int32_t get_app_ingesting_count(const int32_t app_id) const
    {
        return _ingestion_context->get_app_ingesting_count(app_id);
    }

    void reset_app_ingestion(const int32_t app_id) { _ingestion_context->reset_app(app_id); }

    ///
    /// update bulk load states to remote storage functions
    ///

    void create_app_bulk_load_dir(const std::string &app_name,
                                  int32_t app_id,
                                  int32_t partition_count,
                                  start_bulk_load_rpc rpc);

    void create_partition_bulk_load_dir(const std::string &app_name,
                                        const gpid &pid,
                                        int32_t partition_count,
                                        start_bulk_load_rpc rpc);

    // Called by `handle_app_downloading`
    // update partition bulk load metadata reported by replica server on remote storage
    void update_partition_metadata_on_remote_storage(const std::string &app_name,
                                                     const gpid &pid,
                                                     const bulk_load_metadata &metadata);

    // update partition bulk load info on remote storage
    // if should_send_request = true, will send bulk load request after update local partition
    // status, this parameter will be true when restarting bulk load, status will turn from paused
    // to downloading
    void update_partition_info_on_remote_storage(const std::string &app_name,
                                                 const gpid &pid,
                                                 bulk_load_status::type new_status,
                                                 bool should_send_request = false);

    void update_partition_info_unlock(const gpid &pid,
                                      bulk_load_status::type new_status,
                                      /*out*/ partition_bulk_load_info &pinfo);

    void update_partition_info_on_remote_storage_reply(const std::string &app_name,
                                                       const gpid &pid,
                                                       const partition_bulk_load_info &new_info,
                                                       bool should_send_request);

    // update app bulk load status on remote storage
    void update_app_status_on_remote_storage_unlocked(int32_t app_id,
                                                      bulk_load_status::type new_status,
                                                      error_code err = ERR_OK,
                                                      bool should_send_request = false);

    void update_app_status_on_remote_storage_reply(const app_bulk_load_info &ainfo,
                                                   bulk_load_status::type old_status,
                                                   bulk_load_status::type new_status,
                                                   bool should_send_request);

    // called when app is not available or dropped during bulk load, remove bulk load directory on
    // remote storage
    void remove_bulk_load_dir_on_remote_storage(int32_t app_id, const std::string &app_name);

    // called when app is available, remove bulk load directory on remote storage
    // if `set_app_not_bulk_loading` = true: call function
    // `update_app_not_bulk_loading_on_remote_storage` to set app not bulk_loading after removing
    void remove_bulk_load_dir_on_remote_storage(std::shared_ptr<app_state> app,
                                                bool set_app_not_bulk_loading);

    // update app's is_bulk_loading to false on remote_storage
    void update_app_not_bulk_loading_on_remote_storage(std::shared_ptr<app_state> app);

    ///
    /// sync bulk load states from remote storage
    /// called when service initialized or meta server leader switch
    ///
    void create_bulk_load_root_dir();

    void sync_apps_from_remote_storage();

    void do_sync_app(int32_t app_id);

    void sync_partitions_from_remote_storage(int32_t app_id, const std::string &app_name);

    void do_sync_partition(const gpid &pid, std::string &partition_path);

    ///
    /// try to continue bulk load according to states from remote storage
    /// called when service initialized or meta server leader switch
    ///
    void try_to_continue_bulk_load();

    void try_to_continue_app_bulk_load(
        const app_bulk_load_info &ainfo,
        const std::unordered_map<int32_t, partition_bulk_load_info> &partition_map);

    static bool validate_ingest_behind(const std::map<std::string, std::string> &envs,
                                       bool ingest_behind);

    static bool validate_app(int32_t app_id,
                             int32_t partition_count,
                             const std::map<std::string, std::string> &envs,
                             const app_bulk_load_info &ainfo,
                             int32_t pinfo_count);

    static bool
    validate_partition(const app_bulk_load_info &ainfo,
                       const std::unordered_map<int32_t, partition_bulk_load_info> &pinfo_map,
                       const int32_t different_status_count);

    void do_continue_app_bulk_load(
        const app_bulk_load_info &ainfo,
        const std::unordered_map<int32_t, partition_bulk_load_info> &pinfo_map,
        const std::unordered_set<int32_t> &different_status_pidx_set);

    // called by `do_continue_app_bulk_load`
    // only used when app status is downloading and some partition bulk load info not existed on
    // remote storage
    void create_missing_partition_dir(const std::string &app_name,
                                      const gpid &pid,
                                      int32_t partition_count);

    ///
    /// helper functions
    ///
    inline std::shared_ptr<app_state> get_app(const std::string &name)
    {
        zauto_read_lock l(app_lock());
        return _state->get_app(name);
    }

    inline std::shared_ptr<app_state> get_app(int32_t app_id)
    {
        zauto_read_lock l(app_lock());
        return _state->get_app(app_id);
    }

    // get bulk_load_info path on file provider
    // <remote_root_path>/<cluster_name>/<app_name>/bulk_load_info
    inline std::string get_bulk_load_info_path(const std::string &app_name,
                                               const std::string &cluster_name,
                                               const std::string &remote_root_path) const
    {
        std::ostringstream oss;
        oss << remote_root_path << "/" << cluster_name << "/" << app_name << "/"
            << bulk_load_constant::BULK_LOAD_INFO;
        return oss.str();
    }

    // get app_bulk_load_info path on remote storage
    // <_bulk_load_root>/<app_id>
    inline std::string get_app_bulk_load_path(int32_t app_id) const
    {
        std::stringstream oss;
        oss << _bulk_load_root << "/" << app_id;
        return oss.str();
    }

    // get partition_bulk_load_info path on remote storage
    // <_bulk_load_root>/<app_id>/<partition_id>
    inline std::string get_partition_bulk_load_path(const std::string &app_bulk_load_path,
                                                    int partition_id) const
    {
        std::stringstream oss;
        oss << app_bulk_load_path << "/" << partition_id;
        return oss.str();
    }

    inline std::string get_partition_bulk_load_path(const gpid &pid) const
    {
        std::stringstream oss;
        oss << get_app_bulk_load_path(pid.get_app_id()) << "/" << pid.get_partition_index();
        return oss.str();
    }

    inline bool is_partition_metadata_not_updated(gpid pid)
    {
        zauto_read_lock l(_lock);
        return is_partition_metadata_not_updated_unlocked(pid);
    }

    inline bool is_partition_metadata_not_updated_unlocked(gpid pid) const
    {
        const auto &iter = _partition_bulk_load_info.find(pid);
        if (iter == _partition_bulk_load_info.end()) {
            return false;
        }
        const auto &metadata = iter->second.metadata;
        return (metadata.files.size() == 0 && metadata.file_total_size == 0);
    }

    inline bulk_load_status::type get_partition_bulk_load_status_unlocked(gpid pid) const
    {
        const auto &iter = _partition_bulk_load_info.find(pid);
        if (iter != _partition_bulk_load_info.end()) {
            return iter->second.status;
        } else {
            return bulk_load_status::BLS_INVALID;
        }
    }

    inline bulk_load_status::type get_app_bulk_load_status(int32_t app_id)
    {
        zauto_read_lock l(_lock);
        return get_app_bulk_load_status_unlocked(app_id);
    }

    inline bulk_load_status::type get_app_bulk_load_status_unlocked(int32_t app_id) const
    {
        const auto &iter = _app_bulk_load_info.find(app_id);
        if (iter != _app_bulk_load_info.end()) {
            return iter->second.status;
        } else {
            return bulk_load_status::BLS_INVALID;
        }
    }

    inline error_code get_app_bulk_load_err_unlocked(int32_t app_id) const
    {
        const auto &iter = _app_bulk_load_info.find(app_id);
        if (iter != _app_bulk_load_info.end()) {
            return iter->second.bulk_load_err;
        } else {
            return ERR_OK;
        }
    }

    inline bool is_app_bulk_loading_unlocked(int32_t app_id) const
    {
        return (_bulk_load_app_id.find(app_id) != _bulk_load_app_id.end());
    }

private:
    friend class bulk_load_service_test;
    friend class meta_bulk_load_http_test;

    meta_service *_meta_svc;
    server_state *_state;

    std::unique_ptr<mss::meta_storage> _sync_bulk_load_storage;
    std::unique_ptr<ingestion_context> _ingestion_context;
    task_tracker _sync_tracker;

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

    // partition_index -> group total download progress
    std::unordered_map<gpid, int32_t> _partitions_total_download_progress;
    // partition_index -> group bulk load states(node address -> state)
    std::unordered_map<gpid, std::map<host_port, partition_bulk_load_state>>
        _partitions_bulk_load_state;

    std::unordered_map<gpid, bool> _partitions_cleaned_up;
    // Used for bulk load failed and app unavailable to avoid duplicated clean up
    std::unordered_map<app_id, bool> _apps_cleaning_up;
    // Used for bulk load rolling back to downloading
    std::unordered_map<app_id, bool> _apps_rolling_back;
    // Used for restrict bulk load rollback count
    std::unordered_map<app_id, int32_t> _apps_rollback_count;
};

} // namespace replication
} // namespace dsn
