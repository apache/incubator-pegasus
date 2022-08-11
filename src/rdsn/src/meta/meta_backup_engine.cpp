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
    // TODO(heyuchen): TBD
    return ERR_OK;
}

// ThreadPool: THREAD_POOL_DEFAULT
void meta_backup_engine::update_backup_item_on_remote_storage(backup_status::type new_status,
                                                              int64_t end_time)
{
    // TODO(heyuchen): TBD
}

// TODO(heyuchen): update following functions

error_code meta_backup_engine::write_backup_file(const std::string &file_name,
                                                 const dsn::blob &write_buffer)
{
    dist::block_service::create_file_request create_file_req;
    create_file_req.ignore_metadata = true;
    create_file_req.file_name = file_name;

    dsn::error_code err;
    dist::block_service::block_file_ptr remote_file;
    _block_service
        ->create_file(create_file_req,
                      TASK_CODE_EXEC_INLINED,
                      [&err, &remote_file](const dist::block_service::create_file_response &resp) {
                          err = resp.err;
                          remote_file = resp.file_handle;
                      })
        ->wait();
    if (err != dsn::ERR_OK) {
        ddebug_f("create file {} failed", file_name);
        return err;
    }
    dassert_f(remote_file != nullptr,
              "create file {} succeed, but can't get handle",
              create_file_req.file_name);
    remote_file
        ->write(dist::block_service::write_request{write_buffer},
                TASK_CODE_EXEC_INLINED,
                [&err](const dist::block_service::write_response &resp) { err = resp.err; })
        ->wait();
    return err;
}

error_code meta_backup_engine::backup_app_meta()
{
    dsn::blob app_info_buffer;
    {
        zauto_read_lock l;
        _backup_service->get_state()->lock_read(l);
        std::shared_ptr<app_state> app = _backup_service->get_state()->get_app(_cur_backup.app_id);
        if (app == nullptr || app->status != app_status::AS_AVAILABLE) {
            derror_f("app {} is not available, couldn't do backup now.", _cur_backup.app_id);
            return ERR_INVALID_STATE;
        }
        app_state tmp = *app;
        // Because we don't restore app envs, so no need to write app envs to backup file.
        // TODO(zhangyifan): backup and restore app envs when needed.
        tmp.envs.clear();
        app_info_buffer = dsn::json::json_forwarder<app_info>::encode(tmp);
    }

    std::string backup_root =
        dsn::utils::filesystem::path_combine(_backup_path, _backup_service->backup_root());
    // TODO(heyuchen): refactor and update it in future
    std::string file_name = "todo";
    return write_backup_file(file_name, app_info_buffer);
}

void meta_backup_engine::backup_app_partition(const gpid &pid)
{
    dsn::rpc_address partition_primary;
    {
        zauto_read_lock l;
        _backup_service->get_state()->lock_read(l);
        std::shared_ptr<app_state> app = _backup_service->get_state()->get_app(pid.get_app_id());
        if (app == nullptr || app->status != app_status::AS_AVAILABLE) {
            derror_f("app {} is not available, couldn't do backup now.", pid.get_app_id());

            zauto_write_lock lock(_lock);
            _is_backup_failed = true;
            return;
        }
        partition_primary = app->partitions[pid.get_partition_index()].primary;
    }

    if (partition_primary.is_invalid()) {
        dwarn_f("backup_id({}): partition {} doesn't have a primary now, retry to backup it later.",
                _cur_backup.backup_id,
                pid.to_string());
        tasking::enqueue(LPC_DEFAULT_CALLBACK,
                         &_tracker,
                         [this, pid]() { backup_app_partition(pid); },
                         0,
                         std::chrono::seconds(10));
        return;
    }

    auto req = std::make_unique<backup_request>();
    req->pid = pid;
    policy_info backup_policy_info;
    backup_policy_info.__set_backup_provider_type(_provider_type);
    backup_policy_info.__set_policy_name(get_policy_name());
    req->policy = backup_policy_info;
    req->backup_id = _cur_backup.backup_id;
    req->app_name = _cur_backup.app_name;
    if (!_backup_path.empty()) {
        req->__set_backup_path(_backup_path);
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

    zauto_write_lock l(_lock);
    _backup_status[pid.get_partition_index()] = backup_status::CHECKPOINTING;
}

inline void meta_backup_engine::handle_replica_backup_failed(const backup_response &response,
                                                             const gpid pid)
{
    dcheck_eq(response.pid, pid);
    dcheck_eq(response.backup_id, _cur_backup.backup_id);

    derror_f("backup_id({}): backup for partition {} failed, response.err: {}",
             _cur_backup.backup_id,
             pid.to_string(),
             response.err.to_string());
    zauto_write_lock l(_lock);
    // if one partition fail, the whole backup plan fail.
    _is_backup_failed = true;
    _backup_status[pid.get_partition_index()] = backup_status::FAILED;
}

inline void meta_backup_engine::retry_backup(const dsn::gpid pid)
{
    tasking::enqueue(LPC_DEFAULT_CALLBACK,
                     &_tracker,
                     [this, pid]() { backup_app_partition(pid); },
                     0,
                     std::chrono::seconds(1));
}

void meta_backup_engine::on_backup_reply(const error_code err,
                                         const backup_response &response,
                                         const gpid pid,
                                         const rpc_address &primary)
{
    {
        zauto_read_lock l(_lock);
        // if backup of some partition failed, we would not handle response from other partitions.
        if (_is_backup_failed) {
            return;
        }
    }

    // if backup completed, receive ERR_OK and
    // resp.progress=cold_backup_constant::PROGRESS_FINISHED;
    // if backup failed, receive ERR_LOCAL_APP_FAILURE;
    // backup not completed in other cases.
    // see replica::on_cold_backup() for details.

    auto rep_error = err == ERR_OK ? response.err : err;

    if (rep_error == ERR_LOCAL_APP_FAILURE) {
        handle_replica_backup_failed(response, pid);
        return;
    }

    if (rep_error != ERR_OK) {
        derror_f("backup_id({}): backup request to server {} failed, error: {}, retry to "
                 "send backup request.",
                 _cur_backup.backup_id,
                 primary.to_string(),
                 rep_error.to_string());
        retry_backup(pid);
        return;
    };

    if (response.progress == cold_backup_constant::PROGRESS_FINISHED) {
        dcheck_eq(response.pid, pid);
        dcheck_eq(response.backup_id, _cur_backup.backup_id);
        ddebug_f("backup_id({}): backup for partition {} completed.",
                 _cur_backup.backup_id,
                 pid.to_string());
        {
            zauto_write_lock l(_lock);
            _backup_status[pid.get_partition_index()] = backup_status::SUCCEED;
        }
        complete_current_backup();
        return;
    }

    // backup is not finished, meta polling to send request
    ddebug_f("backup_id({}): receive backup response for partition {} from server {}, now "
             "progress {}, retry to send backup request.",
             _cur_backup.backup_id,
             pid.to_string(),
             primary.to_string(),
             response.progress);

    retry_backup(pid);
}

void meta_backup_engine::write_backup_info()
{
    std::string backup_root =
        dsn::utils::filesystem::path_combine(_backup_path, _backup_service->backup_root());
    // TODO(heyuchen): refactor and update it in future
    std::string file_name = "todo";
    blob buf = dsn::json::json_forwarder<backup_item>::encode(_cur_backup);
    error_code err = write_backup_file(file_name, buf);
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
