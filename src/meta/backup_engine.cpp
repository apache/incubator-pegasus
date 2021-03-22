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

#include "common/backup_utils.h"
#include "common/replication_common.h"
#include "server_state.h"

namespace dsn {
namespace replication {

backup_engine::backup_engine(backup_service *service)
    : _backup_service(service), _block_service(nullptr), _is_backup_failed(false)
{
}

backup_engine::~backup_engine() { _tracker.cancel_outstanding_tasks(); }

error_code backup_engine::init_backup(int32_t app_id)
{
    std::string app_name;
    int partition_count;
    {
        zauto_read_lock l;
        _backup_service->get_state()->lock_read(l);
        std::shared_ptr<app_state> app = _backup_service->get_state()->get_app(app_id);
        if (app == nullptr || app->status != app_status::AS_AVAILABLE) {
            derror_f("app {} is not available, couldn't do backup now.", app_id);
            return ERR_INVALID_STATE;
        }
        app_name = app->app_name;
        partition_count = app->partition_count;
    }

    zauto_lock lock(_lock);
    _backup_status.clear();
    for (int i = 0; i < partition_count; ++i) {
        _backup_status.emplace(i, backup_status::UNALIVE);
    }
    _cur_backup.app_id = app_id;
    _cur_backup.app_name = app_name;
    _cur_backup.backup_id = static_cast<int64_t>(dsn_now_ms());
    _cur_backup.start_time_ms = _cur_backup.backup_id;
    return ERR_OK;
}

error_code backup_engine::set_block_service(const std::string &provider)
{
    _provider_type = provider;
    _block_service = _backup_service->get_meta_service()
                         ->get_block_service_manager()
                         .get_or_create_block_filesystem(provider);
    if (_block_service == nullptr) {
        return ERR_INVALID_PARAMETERS;
    }
    return ERR_OK;
}

error_code backup_engine::write_backup_file(const std::string &file_name,
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

error_code backup_engine::backup_app_meta()
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

    std::string file_name = cold_backup::get_app_metadata_file(_backup_service->backup_root(),
                                                               _cur_backup.app_name,
                                                               _cur_backup.app_id,
                                                               _cur_backup.backup_id);
    return write_backup_file(file_name, app_info_buffer);
}

void backup_engine::backup_app_partition(const gpid &pid)
{
    dsn::rpc_address partition_primary;
    {
        zauto_read_lock l;
        _backup_service->get_state()->lock_read(l);
        std::shared_ptr<app_state> app = _backup_service->get_state()->get_app(pid.get_app_id());
        if (app == nullptr || app->status != app_status::AS_AVAILABLE) {
            derror_f("app {} is not available, couldn't do backup now.", pid.get_app_id());

            zauto_lock lock(_lock);
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

    ddebug_f("backup_id({}): send backup request to partition {}, target_addr = {}",
             _cur_backup.backup_id,
             pid.to_string(),
             partition_primary.to_string());
    backup_rpc rpc(std::move(req), RPC_COLD_BACKUP, 10000_ms, 0, pid.thread_hash());
    rpc.call(
        partition_primary, &_tracker, [this, rpc, pid, partition_primary](error_code err) mutable {
            on_backup_reply(err, rpc.response(), pid, partition_primary);
        });

    zauto_lock l(_lock);
    _backup_status[pid.get_partition_index()] = backup_status::ALIVE;
}

void backup_engine::on_backup_reply(error_code err,
                                    const backup_response &response,
                                    gpid pid,
                                    const rpc_address &primary)
{
    dcheck_eq(response.pid, pid);
    dcheck_eq(response.backup_id, _cur_backup.backup_id);

    {
        zauto_lock l(_lock);
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
    int32_t partition = pid.get_partition_index();
    if (err == dsn::ERR_OK && response.err == dsn::ERR_OK &&
        response.progress == cold_backup_constant::PROGRESS_FINISHED) {
        ddebug_f("backup_id({}): backup for partition {} completed.",
                 _cur_backup.backup_id,
                 pid.to_string());
        {
            zauto_lock l(_lock);
            _backup_status[partition] = backup_status::COMPLETED;
        }
        complete_current_backup();
        return;
    }

    if (response.err == ERR_LOCAL_APP_FAILURE) {
        derror_f("backup_id({}): backup for partition {} failed.",
                 _cur_backup.backup_id,
                 pid.to_string());
        zauto_lock l(_lock);
        _is_backup_failed = true;
        _backup_status[partition] = backup_status::FAILED;
        return;
    }

    if (err != ERR_OK) {
        dwarn_f("backup_id({}): send backup request to server {} failed, rpc error: {}, retry to "
                "send backup request.",
                _cur_backup.backup_id,
                primary.to_string(),
                err.to_string());
    } else {
        ddebug_f(
            "backup_id({}): receive backup response for partition {} from server {}, rpc error "
            "{}, response error {}, retry to send backup request.",
            _cur_backup.backup_id,
            pid.to_string(),
            primary.to_string(),
            err.to_string(),
            response.err.to_string());
    }
    tasking::enqueue(LPC_DEFAULT_CALLBACK,
                     &_tracker,
                     [this, pid]() { backup_app_partition(pid); },
                     0,
                     std::chrono::seconds(1));
}

void backup_engine::write_backup_info()
{
    std::string file_name =
        cold_backup::get_backup_info_file(_backup_service->backup_root(), _cur_backup.backup_id);
    blob buf = dsn::json::json_forwarder<app_backup_info>::encode(_cur_backup);
    error_code err = write_backup_file(file_name, buf);
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
}

void backup_engine::complete_current_backup()
{
    {
        zauto_lock l(_lock);
        for (const auto &status : _backup_status) {
            if (status.second != backup_status::COMPLETED) {
                return;
            }
        }
        // complete backup for all partitions.
        _cur_backup.end_time_ms = dsn_now_ms();
    }
    write_backup_info();
}

error_code backup_engine::start()
{
    error_code err = backup_app_meta();
    if (err != ERR_OK) {
        derror_f("backup_id({}): backup meta data for app {} failed, error {}",
                 _cur_backup.backup_id,
                 _cur_backup.app_id,
                 err.to_string());
        return err;
    }
    for (int i = 0; i < _backup_status.size(); ++i) {
        tasking::enqueue(LPC_DEFAULT_CALLBACK, &_tracker, [this, i]() {
            backup_app_partition(gpid(_cur_backup.app_id, i));
        });
    }
    return ERR_OK;
}

bool backup_engine::is_in_progress() const
{
    zauto_lock l(_lock);
    return _cur_backup.end_time_ms == 0 && !_is_backup_failed;
}

backup_item backup_engine::get_backup_item() const
{
    zauto_lock l(_lock);
    backup_item item;
    item.backup_id = _cur_backup.backup_id;
    item.app_name = _cur_backup.app_name;
    item.backup_provider_type = _provider_type;
    item.start_time_ms = _cur_backup.start_time_ms;
    item.end_time_ms = _cur_backup.end_time_ms;
    item.is_backup_failed = _is_backup_failed;
    return item;
}

} // namespace replication
} // namespace dsn
