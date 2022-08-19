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

#include <dsn/cpp/json_helper.h>
#include <dsn/tool-api/zlocks.h>

#include "common/backup_restore_common.h"
#include "meta_service.h"
#include "server_state.h"
#include "meta_backup_service.h"

namespace dsn {
namespace replication {

// backup_info file written into block service
struct app_backup_info
{
    int64_t backup_id;
    int64_t start_time_ms;
    int64_t end_time_ms;
    int32_t app_id;
    std::string app_name;
    app_backup_info() : backup_id(0), start_time_ms(0), end_time_ms(0) {}
    DEFINE_JSON_SERIALIZATION(backup_id, start_time_ms, end_time_ms, app_id, app_name)
};

///
/// backup path on remote storage
///
/// Onetime backup:
/// <cluster_root>/backup/<app_id>/once/<backup_id>/<backup_item>
///
/// Periodic backup:
/// <cluster_root>/backup/<app_id>/periodic/<periodic_backup_policy>
/// <cluster_root>/backup/<app_id>/periodic/<backup_id>/<backup_item>
///
static const std::string ONETIME_PATH = "once";
static const std::string PERIODIC_PATH = "periodic";

///
///           Meta backup status
///
///              start backup
///                  |
///                  v       Error/Cancel
///            Checkpointing ------------->|
///                  |                     |
///                  v       Error/Cancel  |
///              Uploading  -------------->|
///                  |                     |
///                  v                     v
///               Succeed          Failed/Canceled
///
class meta_backup_engine
{
public:
    explicit meta_backup_engine(meta_service *meta_svc, bool is_periodic);
    ~meta_backup_engine();

    int64_t get_current_backup_id() const { return _cur_backup.backup_id; }
    int32_t get_backup_app_id() const { return _cur_backup.app_id; }

    backup_status::type get_backup_status() const
    {
        zauto_read_lock l(_lock);
        return _cur_backup.status;
    }

    backup_item get_backup_item() const
    {
        zauto_read_lock l(_lock);
        backup_item item = _cur_backup;
        return item;
    }

    bool is_in_progress() const
    {
        zauto_read_lock l(_lock);
        return _cur_backup.end_time_ms == 0 && !_is_backup_failed && !_is_backup_canceled;
    }

private:
    void init_backup(int32_t app_id,
                     int32_t partition_count,
                     const std::string &app_name,
                     const std::string &provider,
                     const std::string &backup_root_path);
    void start();

    void backup_app_partition(const gpid &pid);
    void on_backup_reply(error_code err,
                         const backup_response &response,
                         gpid pid,
                         const rpc_address &primary);
    void retry_backup(const dsn::gpid pid);
    void handle_replica_backup_failed(const backup_response &response, const gpid pid);

    error_code write_backup_file(const std::string &remote_dir,
                                 const std::string &file_name,
                                 const blob &write_buffer);
    error_code write_app_info();
    void write_backup_info();

    void update_backup_item_on_remote_storage(backup_status::type new_status, int64_t end_time = 0);

    std::string get_remote_storage_root() const
    {
        return meta_options::concat_path_unix_style(_meta_svc->get_cluster_root(), "backup");
    }

    std::string get_remote_backup_path() const
    {
        auto type_path = _is_periodic_backup ? PERIODIC_PATH : ONETIME_PATH;
        return fmt::format("{}/{}/{}/{}",
                           get_remote_storage_root(),
                           _cur_backup.app_id,
                           type_path,
                           _cur_backup.backup_id);
    }

private:
    friend class meta_backup_engine_test;

    meta_service *_meta_svc;
    task_tracker _tracker;

    mutable zrwlock_nr _lock; // {
    bool _is_periodic_backup;
    bool _is_backup_failed{false};
    bool _is_backup_canceled{false};
    backup_item _cur_backup;
    std::vector<backup_status::type> _backup_status;
    // }

    // TODO(heyuchen): remove following functions and vars
private:
    void complete_current_backup();

    backup_service *_backup_service;
    std::string _backup_path;
};

} // namespace replication
} // namespace dsn
