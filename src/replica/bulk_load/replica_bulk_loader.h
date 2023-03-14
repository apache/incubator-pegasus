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

#include <stdint.h>
#include <atomic>
#include <map>
#include <ostream>
#include <string>

#include "bulk_load_types.h"
#include "common/replication_other_types.h"
#include "metadata_types.h"
#include "replica/replica.h"
#include "replica/replica_base.h"
#include "runtime/api_layer1.h"
#include "runtime/task/task.h"
#include "utils/error_code.h"
#include "utils/zlocks.h"

namespace dsn {
class task_tracker;
namespace dist {
namespace block_service {
class block_filesystem;
} // namespace block_service
} // namespace dist

namespace replication {
class replica_stub;

class replica_bulk_loader : replica_base
{
public:
    explicit replica_bulk_loader(replica *r);
    ~replica_bulk_loader();

    void on_bulk_load(const bulk_load_request &request, /*out*/ bulk_load_response &response);

    void on_group_bulk_load(const group_bulk_load_request &request,
                            /*out*/ group_bulk_load_response &response);

private:
    void broadcast_group_bulk_load(const bulk_load_request &meta_req);
    void on_group_bulk_load_reply(error_code err,
                                  const group_bulk_load_request &req,
                                  const group_bulk_load_response &resp);

    error_code do_bulk_load(const std::string &app_name,
                            bulk_load_status::type meta_status,
                            const std::string &cluster_name,
                            const std::string &provider_name,
                            const std::string &remote_root_path);

    // compare meta bulk load status and local bulk load status
    // \return ERR_INVALID_STATE if local status is invalid
    // for example, if meta status is ingestion, replica local status can only be downloaded or
    // ingestion, if local status is other status, will return ERR_INVALID_STATE
    static error_code validate_status(const bulk_load_status::type meta_status,
                                      const bulk_load_status::type local_status);

    // replica start or restart download sst files from remote provider
    // \return ERR_BUSY if node has already had enough replica executing downloading
    // \return ERR_FILE_OPERATION_FAILED: create local bulk load dir failed
    error_code start_download(const std::string &remote_dir, const std::string &provider_name);

    // download metadata file and create sst download tasks
    // metadata and sst files will be downloaded in {_dir}/.bulk_load directory
    void download_files(const std::string &provider_name,
                        const std::string &remote_dir,
                        const std::string &local_dir);

    // download sst files from remote provider
    void download_sst_file(const std::string &remote_dir,
                           const std::string &local_dir,
                           int32_t file_index,
                           dist::block_service::block_filesystem *fs);

    // \return ERR_FILE_OPERATION_FAILED: file not exist, get size failed, open file failed
    // \return ERR_CORRUPTION: parse failed
    // need to acquire write lock while calling it
    error_code parse_bulk_load_metadata(const std::string &fname);

    // update download progress after downloading sst files succeed
    void update_bulk_load_download_progress(uint64_t file_size, const std::string &file_name);

    // need to acquire write lock while calling it
    void try_decrease_bulk_load_download_count();
    void check_download_finish();
    void start_ingestion();
    void check_ingestion_finish();
    void handle_bulk_load_succeed();
    // called when bulk load succeed or failed or canceled
    void handle_bulk_load_finish(bulk_load_status::type new_status);
    void pause_bulk_load();

    void remove_local_bulk_load_dir(const std::string &bulk_load_dir);
    // need to acquire write lock while calling it
    void cleanup_download_tasks();
    bool cleanup_download_task(task_ptr task_);
    void clear_bulk_load_states();
    bool is_cleaned_up();

    void report_bulk_load_states_to_meta(bulk_load_status::type remote_status,
                                         bool report_metadata,
                                         /*out*/ bulk_load_response &response);
    void report_group_download_progress(/*out*/ bulk_load_response &response);
    void report_group_ingestion_status(/*out*/ bulk_load_response &response);
    void report_group_cleaned_up(/*out*/ bulk_load_response &response);
    void report_group_is_paused(/*out*/ bulk_load_response &response);

    void report_bulk_load_states_to_primary(bulk_load_status::type remote_status,
                                            /*out*/ group_bulk_load_response &response);

    // called by `update_local_configuration` to do possible states cleaning up
    void clear_bulk_load_states_if_needed(partition_status::type old_status,
                                          partition_status::type new_status);

    ///
    /// bulk load path on remote file provider:
    /// <remote_root_path>/<cluster_name>/<app_name>/{bulk_load_info}
    /// <remote_root_path>/<cluster_name>/<app_name>/<partition_index>/<file_name>
    /// <remote_root_path>/<cluster_name>/<app_name>/<partition_index>/bulk_load_metadata
    ///
    // get partition's file dir on remote file provider
    inline std::string get_remote_bulk_load_dir(const std::string &app_name,
                                                const std::string &cluster_name,
                                                const std::string &remote_root_path,
                                                uint32_t pidx) const
    {
        std::ostringstream oss;
        oss << remote_root_path << "/" << cluster_name << "/" << app_name << "/" << pidx;
        return oss.str();
    }

    inline bulk_load_status::type get_bulk_load_status() const { return _status; }

    inline void set_bulk_load_status(bulk_load_status::type status) { _status = status; }

    inline uint64_t duration_ms() const
    {
        return _bulk_load_start_time_ms > 0 ? (dsn_now_ms() - _bulk_load_start_time_ms) : 0;
    }

    inline uint64_t ingestion_duration_ms() const
    {
        return _replica->_bulk_load_ingestion_start_time_ms > 0
                   ? (dsn_now_ms() - _replica->_bulk_load_ingestion_start_time_ms)
                   : 0;
    }

    //
    // helper functions
    //
    partition_status::type status() const { return _replica->status(); }
    ballot get_ballot() const { return _replica->get_ballot(); }
    task_tracker *tracker() { return _replica->tracker(); }

private:
    replica *_replica;
    replica_stub *_stub;

    friend class replica;
    friend class replica_stub;
    friend class replica_bulk_loader_test;

    // bulk load states lock
    zrwlock_nr _lock; // {
    bulk_load_status::type _status{bulk_load_status::BLS_INVALID};
    bulk_load_metadata _metadata;
    std::atomic<bool> _is_downloading{false};
    std::atomic<uint64_t> _cur_downloaded_size{0};
    std::atomic<int32_t> _download_progress{0};
    std::atomic<error_code> _download_status{ERR_OK};
    // }
    // file_name -> downloading task
    std::map<std::string, task_ptr> _download_files_task;
    // download metadata and create download file tasks
    task_ptr _download_task;
    // Used for perf-counter
    uint64_t _bulk_load_start_time_ms{0};
};

} // namespace replication
} // namespace dsn
