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

#include <chrono>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "backup_types.h"
#include "block_service/block_service.h"
#include "block_service/block_service_manager.h"
#include "common/backup_common.h"
#include "common/gpid.h"
#include "common/json_helper.h"
#include "common/replication.codes.h"
#include "dsn.layer2_types.h"
#include "meta/backup_engine.h"
#include "meta/meta_backup_service.h"
#include "meta/meta_data.h"
#include "meta/meta_service.h"
#include "rpc/dns_resolver.h"
#include "rpc/rpc_holder.h"
#include "rpc/rpc_host_port.h"
#include "runtime/api_layer1.h"
#include "server_state.h"
#include "task/async_calls.h"
#include "task/task.h"
#include "task/task_code.h"
#include "task/task_tracker.h"
#include "utils/autoref_ptr.h"
#include "utils/blob.h"
#include "utils/chrono_literals.h"
#include "utils/error_code.h"
#include "utils/filesystem.h"
#include "utils/fmt_logging.h"
#include "utils/zlocks.h"

namespace dsn {
namespace replication {

backup_engine::backup_engine(backup_service *service)
    : _backup_service(service), _block_service(nullptr), _backup_path(""), _is_backup_failed(false)
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
            LOG_ERROR("app {} is not available, couldn't do backup now.", app_id);
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

error_code backup_engine::set_backup_path(const std::string &path)
{
    if (_block_service && _block_service->is_root_path_set()) {
        return ERR_INVALID_PARAMETERS;
    }
    LOG_INFO("backup path is set to {}.", path);
    _backup_path = path;
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
        LOG_INFO("create file {} failed", file_name);
        return err;
    }
    CHECK_NOTNULL(
        remote_file, "create file {} succeed, but can't get handle", create_file_req.file_name);
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
            LOG_ERROR("app {} is not available, couldn't do backup now.", _cur_backup.app_id);
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
    std::string file_name = cold_backup::get_app_metadata_file(
        backup_root, _cur_backup.app_name, _cur_backup.app_id, _cur_backup.backup_id);
    return write_backup_file(file_name, app_info_buffer);
}

void backup_engine::backup_app_partition(const gpid &pid)
{
    dsn::host_port partition_primary;
    {
        zauto_read_lock l;
        _backup_service->get_state()->lock_read(l);
        std::shared_ptr<app_state> app = _backup_service->get_state()->get_app(pid.get_app_id());
        if (app == nullptr || app->status != app_status::AS_AVAILABLE) {
            LOG_ERROR("app {} is not available, couldn't do backup now.", pid.get_app_id());

            zauto_lock lock(_lock);
            _is_backup_failed = true;
            return;
        }
        partition_primary = app->pcs[pid.get_partition_index()].hp_primary;
    }

    if (!partition_primary) {
        LOG_WARNING(
            "backup_id({}): partition {} doesn't have a primary now, retry to backup it later.",
            _cur_backup.backup_id,
            pid);
        tasking::enqueue(
            LPC_DEFAULT_CALLBACK,
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

    LOG_INFO("backup_id({}): send backup request to partition {}, target_addr = {}",
             _cur_backup.backup_id,
             pid,
             partition_primary);
    backup_rpc rpc(std::move(req), RPC_COLD_BACKUP, 10000_ms, 0, pid.thread_hash());
    rpc.call(dsn::dns_resolver::instance().resolve_address(partition_primary),
             &_tracker,
             [this, rpc, pid, partition_primary](error_code err) mutable {
                 on_backup_reply(err, rpc.response(), pid, partition_primary);
             });

    zauto_lock l(_lock);
    _backup_status[pid.get_partition_index()] = backup_status::ALIVE;
}

inline void backup_engine::handle_replica_backup_failed(const backup_response &response,
                                                        const gpid pid)
{
    CHECK_EQ(response.pid, pid);
    CHECK_EQ(response.backup_id, _cur_backup.backup_id);

    LOG_ERROR("backup_id({}): backup for partition {} failed, response.err: {}",
              _cur_backup.backup_id,
              pid,
              response.err);
    zauto_lock l(_lock);
    // if one partition fail, the whole backup plan fail.
    _is_backup_failed = true;
    _backup_status[pid.get_partition_index()] = backup_status::FAILED;
}

inline void backup_engine::retry_backup(const dsn::gpid pid)
{
    tasking::enqueue(
        LPC_DEFAULT_CALLBACK,
        &_tracker,
        [this, pid]() { backup_app_partition(pid); },
        0,
        std::chrono::seconds(1));
}

void backup_engine::on_backup_reply(const error_code err,
                                    const backup_response &response,
                                    const gpid pid,
                                    const host_port &primary)
{
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

    auto rep_error = err == ERR_OK ? response.err : err;

    if (rep_error == ERR_LOCAL_APP_FAILURE) {
        handle_replica_backup_failed(response, pid);
        return;
    }

    if (rep_error != ERR_OK) {
        LOG_ERROR("backup_id({}): backup request to server {} failed, error: {}, retry to "
                  "send backup request.",
                  _cur_backup.backup_id,
                  primary,
                  rep_error);
        retry_backup(pid);
        return;
    };

    if (response.progress == cold_backup_constant::PROGRESS_FINISHED) {
        CHECK_EQ(response.pid, pid);
        CHECK_EQ(response.backup_id, _cur_backup.backup_id);
        LOG_INFO("backup_id({}): backup for partition {} completed.", _cur_backup.backup_id, pid);
        {
            zauto_lock l(_lock);
            _backup_status[pid.get_partition_index()] = backup_status::COMPLETED;
        }
        complete_current_backup();
        return;
    }

    // backup is not finished, meta polling to send request
    LOG_INFO("backup_id({}): receive backup response for partition {} from server {}, now "
             "progress {}, retry to send backup request.",
             _cur_backup.backup_id,
             pid,
             primary,
             response.progress);

    retry_backup(pid);
}

void backup_engine::write_backup_info()
{
    std::string backup_root =
        dsn::utils::filesystem::path_combine(_backup_path, _backup_service->backup_root());
    std::string file_name = cold_backup::get_backup_info_file(backup_root, _cur_backup.backup_id);
    blob buf = dsn::json::json_forwarder<app_backup_info>::encode(_cur_backup);
    error_code err = write_backup_file(file_name, buf);
    if (err == ERR_FS_INTERNAL) {
        LOG_ERROR(
            "backup_id({}): write backup info failed, error {}, do not try again for this error.",
            _cur_backup.backup_id,
            err);
        zauto_lock l(_lock);
        _is_backup_failed = true;
        return;
    }
    if (err != ERR_OK) {
        LOG_WARNING("backup_id({}): write backup info failed, retry it later.",
                    _cur_backup.backup_id);
        tasking::enqueue(
            LPC_DEFAULT_CALLBACK,
            &_tracker,
            [this]() { write_backup_info(); },
            0,
            std::chrono::seconds(1));
        return;
    }
    LOG_INFO("backup_id({}): successfully wrote backup info, backup for app {} completed.",
             _cur_backup.backup_id,
             _cur_backup.app_id);
    zauto_lock l(_lock);
    _cur_backup.end_time_ms = dsn_now_ms();
}

void backup_engine::complete_current_backup()
{
    {
        zauto_lock l(_lock);
        for (const auto &status : _backup_status) {
            if (status.second != backup_status::COMPLETED) {
                // backup for some partition was not finished.
                return;
            }
        }
    }
    // complete backup for all partitions.
    write_backup_info();
}

error_code backup_engine::start()
{
    error_code err = backup_app_meta();
    if (err != ERR_OK) {
        LOG_ERROR("backup_id({}): backup meta data for app {} failed, error {}",
                  _cur_backup.backup_id,
                  _cur_backup.app_id,
                  err);
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
    item.backup_path = _backup_path;
    item.backup_provider_type = _provider_type;
    item.start_time_ms = _cur_backup.start_time_ms;
    item.end_time_ms = _cur_backup.end_time_ms;
    item.is_backup_failed = _is_backup_failed;
    return item;
}

} // namespace replication
} // namespace dsn
