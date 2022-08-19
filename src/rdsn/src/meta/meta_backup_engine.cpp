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

#include <dsn/dist/fmt_logging.h>
#include <dsn/utility/fail_point.h>
#include <dsn/utility/filesystem.h>

#include "meta_backup_engine.h"

namespace dsn {
namespace replication {

meta_backup_engine::meta_backup_engine(meta_service *meta_svc, bool is_periodic)
    : _meta_svc(meta_svc), _is_periodic_backup(is_periodic)
{
}

meta_backup_engine::~meta_backup_engine() { _tracker.cancel_outstanding_tasks(); }

// ThreadPool: THREAD_POOL_DEFAULT
void meta_backup_engine::init_backup(int32_t app_id,
                                     int32_t partition_count,
                                     const std::string &app_name,
                                     const std::string &provider,
                                     const std::string &backup_root_path)
{
    zauto_write_lock l(_lock);
    _backup_status.clear();
    for (int i = 0; i < partition_count; ++i) {
        _backup_status.emplace_back(backup_status::UNINITIALIZED);
    }
    _cur_backup.app_id = app_id;
    _cur_backup.app_name = app_name;
    _cur_backup.backup_id = static_cast<int64_t>(dsn_now_ms());
    _cur_backup.start_time_ms = _cur_backup.backup_id;
    _cur_backup.backup_provider_type = provider;
    _cur_backup.backup_path = backup_root_path;
    _cur_backup.status = backup_status::UNINITIALIZED;
    _is_backup_failed = false;
    _is_backup_canceled = false;
}

// ThreadPool: THREAD_POOL_DEFAULT
void meta_backup_engine::start()
{
    ddebug_f("App[{}] start {} backup[{}] on {}, root_path = {}",
             _cur_backup.app_name,
             _is_periodic_backup ? "periodic" : "onetime",
             _cur_backup.backup_id,
             _cur_backup.backup_provider_type,
             _cur_backup.backup_path);
    error_code err = write_app_info();
    if (err != ERR_OK) {
        derror_f("backup_id({}): backup meta data for app {} failed, error {}",
                 _cur_backup.backup_id,
                 _cur_backup.app_id,
                 err);
        update_backup_item_on_remote_storage(backup_status::FAILED, dsn_now_ms());
        return;
    }
    update_backup_item_on_remote_storage(backup_status::CHECKPOINTING);
    FAIL_POINT_INJECT_F("meta_backup_engine_start", [](dsn::string_view) {});
    for (auto i = 0; i < _backup_status.size(); ++i) {
        zauto_write_lock l(_lock);
        _backup_status[i] = backup_status::CHECKPOINTING;
        tasking::enqueue(LPC_DEFAULT_CALLBACK, &_tracker, [this, i]() {
            backup_app_partition(gpid(_cur_backup.app_id, i));
        });
    }
}

// ThreadPool: THREAD_POOL_DEFAULT
error_code meta_backup_engine::write_app_info()
{
    blob app_info_buffer;
    {
        zauto_read_lock l;
        _meta_svc->get_server_state()->lock_read(l);
        std::shared_ptr<app_state> app = _meta_svc->get_server_state()->get_app(_cur_backup.app_id);
        if (app == nullptr || app->status != app_status::AS_AVAILABLE) {
            derror_f("app {} is not available, couldn't do backup now.", _cur_backup.app_id);
            return ERR_INVALID_STATE;
        }
        app_state tmp = *app;
        app_info_buffer = json::json_forwarder<app_info>::encode(tmp);
    }

    std::string remote_meta_dir =
        get_backup_meta_path(get_backup_root(FLAGS_cold_backup_root, _cur_backup.backup_path),
                             _cur_backup.app_name,
                             _cur_backup.app_id,
                             _cur_backup.backup_id);
    return write_backup_file(remote_meta_dir, backup_constant::APP_METADATA, app_info_buffer);
}

// ThreadPool: THREAD_POOL_DEFAULT
error_code meta_backup_engine::write_backup_file(const std::string &remote_dir,
                                                 const std::string &file_name,
                                                 const blob &write_buffer)
{
    FAIL_POINT_INJECT_F("meta_write_backup_file", [](string_view str) {
        if (str.find("error") != string_view::npos) {
            return ERR_FS_INTERNAL;
        }
        return ERR_OK;
    });

    dist::block_service::block_service_manager &block_manager =
        _meta_svc->get_block_service_manager();
    dist::block_service::block_filesystem *fs =
        block_manager.get_or_create_block_filesystem(_cur_backup.backup_provider_type);
    return block_manager.write_file(remote_dir, file_name, write_buffer, fs);
}

// ThreadPool: THREAD_POOL_DEFAULT
void meta_backup_engine::backup_app_partition(const gpid &pid)
{
    rpc_address partition_primary;
    {
        zauto_read_lock l;
        _meta_svc->get_server_state()->lock_read(l);
        std::shared_ptr<app_state> app = _meta_svc->get_server_state()->get_app(pid.get_app_id());
        if (app == nullptr || app->status != app_status::AS_AVAILABLE) {
            derror_f("app {} is not available, couldn't do backup now.", pid.get_app_id());
            zauto_write_lock l(_lock);
            _is_backup_failed = true;
            return;
        }
        partition_primary = app->partitions[pid.get_partition_index()].primary;
    }

    if (partition_primary.is_invalid()) {
        dwarn_f("backup_id({}): partition {} doesn't have a primary now, retry to backup it later.",
                _cur_backup.backup_id,
                pid);
        tasking::enqueue(LPC_DEFAULT_CALLBACK,
                         &_tracker,
                         [this, pid]() { backup_app_partition(pid); },
                         0,
                         std::chrono::seconds(10));
        return;
    }

    auto req = std::make_unique<backup_request>();
    req->pid = pid;
    req->app_name = _cur_backup.app_name;
    req->backup_id = _cur_backup.backup_id;
    req->backup_provider_type = _cur_backup.backup_provider_type;
    if (!_cur_backup.backup_path.empty()) {
        req->__set_backup_root_path(_cur_backup.backup_path);
    }
    {
        zauto_read_lock l(_lock);
        req->status = _backup_status[pid.get_partition_index()];
    }
    ddebug_f("backup_id({}): send backup request to partition {}, target_addr = {}",
             _cur_backup.backup_id,
             pid.to_string(),
             partition_primary.to_string());

    backup_rpc rpc(std::move(req), RPC_COLD_BACKUP, 10000_ms, 0, pid.thread_hash());
    rpc.call(
        partition_primary, &_tracker, [this, rpc, pid, partition_primary](error_code err) mutable {
            on_backup_reply(err, rpc.response(), pid, partition_primary);
        });
}

// ThreadPool: THREAD_POOL_DEFAULT
void meta_backup_engine::on_backup_reply(const error_code err,
                                         const backup_response &response,
                                         const gpid &pid,
                                         const rpc_address &primary)
{
    {
        zauto_read_lock l(_lock);
        if (_is_backup_failed) {
            derror_f("partition[{}] handle backup failed", pid);
            return;
        }
    }

    auto rep_error = err == ERR_OK ? response.err : err;
    if (rep_error != ERR_OK) {
        derror_f(
            "backup_id({}): receive backup response for partition {} from server {}, error = {}",
            _cur_backup.backup_id,
            pid.to_string(),
            primary.to_string(),
            rep_error);
        handle_replica_backup_failed(pid.get_app_id());
        return;
    }

    if (response.backup_id != _cur_backup.backup_id) {
        dwarn_f("backup_id({}): receive outdated backup response(backup_id={}) for partition {} "
                "from server {}, ignore it",
                _cur_backup.backup_id,
                response.backup_id,
                pid.to_string(),
                primary.to_string());
        retry_backup(pid);
        return;
    }

    if (response.__isset.checkpoint_upload_err) {
        auto type = response.status == backup_status::UPLOADING ? "upload" : "checkpoint";
        derror_f("backup_id({}): receive backup response for partition {} from server {}, meet {} "
                 "error = {}",
                 _cur_backup.backup_id,
                 pid.to_string(),
                 primary.to_string(),
                 type,
                 response.checkpoint_upload_err);
        handle_replica_backup_failed(pid.get_app_id());
        retry_backup(pid);
        return;
    }

    if (response.status == backup_status::CHECKPOINTED) {
        ddebug_f("backup_id({}): backup for partition {} from server {} finish checkpoint",
                 _cur_backup.backup_id,
                 pid.to_string(),
                 primary.to_string());
        {
            zauto_write_lock l(_lock);
            _backup_status[pid.get_partition_index()] = backup_status::UPLOADING;
        }
        if (check_partition_backup_status(backup_status::UPLOADING)) {
            update_backup_item_on_remote_storage(backup_status::UPLOADING);
        }
    }

    // TODO(heyuchen): handle other status

    // backup is not finished, meta polling to send request
    ddebug_f("backup_id({}): receive backup response for partition {} from server {}, "
             "backup_status = {}, retry to send backup request.",
             _cur_backup.backup_id,
             pid.to_string(),
             primary.to_string(),
             dsn::enum_to_string(response.status));

    retry_backup(pid);
}

// ThreadPool: THREAD_POOL_DEFAULT
void meta_backup_engine::update_backup_item_on_remote_storage(backup_status::type new_status,
                                                              int64_t end_time)
{
    FAIL_POINT_INJECT_F("meta_update_backup_item", [=](dsn::string_view) {
        _cur_backup.status = new_status;
        if (end_time > 0) {
            _cur_backup.end_time_ms = end_time;
        }
    });

    auto item = _cur_backup;
    item.status = new_status;
    if (end_time > 0) {
        item.end_time_ms = end_time;
    }

    blob value = json::json_forwarder<backup_item>::encode(item);
    std::string path = get_remote_backup_path();
    _meta_svc->get_meta_storage()->set_data(std::move(path), std::move(value), [this, item]() {
        zauto_write_lock l(_lock);
        _cur_backup = item;
    });
}

// ThreadPool: THREAD_POOL_DEFAULT
void meta_backup_engine::handle_replica_backup_failed(int32_t app_id)
{
    zauto_write_lock l(_lock);
    // if one partition fail, the whole backup process fail.
    _is_backup_failed = true;
    for (auto i = 0; i < _backup_status.size(); i++) {
        _backup_status[i] = backup_status::FAILED;
        retry_backup(gpid(app_id, i));
    }
    update_backup_item_on_remote_storage(backup_status::FAILED, dsn_now_ms());
}

// ThreadPool: THREAD_POOL_DEFAULT
inline void meta_backup_engine::retry_backup(const gpid &pid)
{
    FAIL_POINT_INJECT_F("meta_retry_backup", [](dsn::string_view) {});
    tasking::enqueue(LPC_DEFAULT_CALLBACK,
                     &_tracker,
                     [this, pid]() { backup_app_partition(pid); },
                     0,
                     std::chrono::seconds(1));
}

// TODO(heyuchen): update following functions

void meta_backup_engine::write_backup_info()
{
    std::string backup_root =
        dsn::utils::filesystem::path_combine(_backup_path, _backup_service->backup_root());
    // TODO(heyuchen): refactor and update it in future
    std::string remote_dir = "todo";
    std::string file_name = "todo";
    blob buf = dsn::json::json_forwarder<backup_item>::encode(_cur_backup);
    error_code err = write_backup_file(remote_dir, file_name, buf);
    if (err == ERR_FS_INTERNAL) {
        derror_f(
            "backup_id({}): write backup info failed, error {}, do not try again for this error.",
            _cur_backup.backup_id,
            err.to_string());
        zauto_write_lock l(_lock);
        _is_backup_failed = true;
        return;
    }
    if (err != ERR_OK) {
        dwarn_f("backup_id({}): write backup info failed, retry it later.", _cur_backup.backup_id);
        tasking::enqueue(LPC_DEFAULT_CALLBACK,
                         &_tracker,
                         [this]() { write_backup_info(); },
                         0,
                         std::chrono::seconds(1));
        return;
    }
    ddebug_f("backup_id({}): successfully wrote backup info, backup for app {} completed.",
             _cur_backup.backup_id,
             _cur_backup.app_id);
    zauto_write_lock l(_lock);
    _cur_backup.end_time_ms = dsn_now_ms();
}

void meta_backup_engine::complete_current_backup()
{
    {
        zauto_read_lock l(_lock);
        for (const auto &status : _backup_status) {
            if (status != backup_status::SUCCEED) {
                // backup for some partition was not finished.
                return;
            }
        }
    }
    // complete backup for all partitions.
    write_backup_info();
}

} // namespace replication
} // namespace dsn
