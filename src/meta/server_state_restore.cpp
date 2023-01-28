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

#include <boost/lexical_cast.hpp>
#include "block_service/block_service.h"
#include "utils/fmt_logging.h"
#include "utils/filesystem.h"

#include "block_service/block_service_manager.h"
#include "common/backup_common.h"
#include "meta_service.h"
#include "server_state.h"

using namespace dsn::dist::block_service;

namespace dsn {
namespace replication {

void server_state::sync_app_from_backup_media(
    const configuration_restore_request &request,
    std::function<void(error_code, const blob &)> &&callback)
{
    dsn::ref_ptr<dsn::future_task<dsn::error_code, dsn::blob>> callback_tsk(
        new dsn::future_task<dsn::error_code, dsn::blob>(
            LPC_RESTORE_BACKGROUND, std::move(callback), 0));

    block_filesystem *blk_fs =
        _meta_svc->get_block_service_manager().get_or_create_block_filesystem(
            request.backup_provider_name);
    if (blk_fs == nullptr) {
        LOG_ERROR("acquire block_filesystem({}) failed", request.backup_provider_name);
        callback_tsk->enqueue_with(ERR_INVALID_PARAMETERS, dsn::blob());
        return;
    }

    std::string backup_root = request.cluster_name;
    if (request.__isset.restore_path) {
        backup_root = dsn::utils::filesystem::path_combine(request.restore_path, backup_root);
    }
    if (!request.policy_name.empty()) {
        backup_root = dsn::utils::filesystem::path_combine(backup_root, request.policy_name);
    }
    std::string app_metadata = cold_backup::get_app_metadata_file(
        backup_root, request.app_name, request.app_id, request.time_stamp);

    error_code err = ERR_OK;
    block_file_ptr file_handle = nullptr;
    LOG_INFO("start to create metadata file {}", app_metadata);
    blk_fs
        ->create_file(create_file_request{app_metadata, true},
                      TASK_CODE_EXEC_INLINED,
                      [&err, &file_handle](const create_file_response &resp) {
                          err = resp.err;
                          file_handle = resp.file_handle;
                      })
        ->wait();

    if (err != ERR_OK) {
        LOG_ERROR("create metadata file {} failed.", app_metadata);
        callback_tsk->enqueue_with(err, dsn::blob());
        return;
    }
    CHECK_NOTNULL(file_handle, "create file from backup media ecounter error");
    file_handle->read(
        read_request{0, -1}, TASK_CODE_EXEC_INLINED, [callback_tsk](const read_response &resp) {
            callback_tsk->enqueue_with(resp.err, resp.buffer);
        });
}

std::pair<dsn::error_code, std::shared_ptr<app_state>> server_state::restore_app_info(
    dsn::message_ex *msg, const configuration_restore_request &req, const dsn::blob &app_info)
{
    std::pair<dsn::error_code, std::shared_ptr<app_state>> res = std::make_pair(ERR_OK, nullptr);

    dsn::app_info info;
    if (!::dsn::json::json_forwarder<dsn::app_info>::decode(app_info, info)) {
        std::string b_str(app_info.data(), app_info.length());
        LOG_ERROR("decode app_info '{}' failed", b_str);
        // NOTICE : maybe find a better error_code to replace err_corruption
        res.first = ERR_CORRUPTION;
        return res;
    }
    int32_t old_app_id = info.app_id;
    std::string old_app_name = info.app_name;
    CHECK_EQ_MSG(old_app_id, req.app_id, "invalid app id");
    CHECK_EQ_MSG(old_app_name, req.app_name, "invalid app name");
    std::shared_ptr<app_state> app = nullptr;

    if (!req.new_app_name.empty()) {
        info.app_name = req.new_app_name;
    }

    {
        // check whether appid and app_name/new_app_name is valid
        zauto_write_lock l(_lock);
        app = get_app(info.app_name);
        if (app != nullptr) {
            res.first = ERR_INVALID_PARAMETERS;
            return res;
        } else {
            info.app_id = next_app_id();
            app = app_state::create(info);
            app->status = app_status::AS_CREATING;
            app->helpers->pending_response = msg;
            app->helpers->partitions_in_progress.store(info.partition_count);

            _all_apps.emplace(app->app_id, app);
            _exist_apps.emplace(info.app_name, app);
        }
    }
    // TODO: using one single env to replace
    app->envs[backup_restore_constant::BLOCK_SERVICE_PROVIDER] = req.backup_provider_name;
    app->envs[backup_restore_constant::CLUSTER_NAME] = req.cluster_name;
    app->envs[backup_restore_constant::POLICY_NAME] = req.policy_name;
    app->envs[backup_restore_constant::APP_NAME] = old_app_name;
    app->envs[backup_restore_constant::APP_ID] = std::to_string(old_app_id);
    app->envs[backup_restore_constant::BACKUP_ID] = std::to_string(req.time_stamp);
    if (req.skip_bad_partition) {
        app->envs[backup_restore_constant::SKIP_BAD_PARTITION] = std::string("true");
    }
    if (req.__isset.restore_path) {
        app->envs[backup_restore_constant::RESTORE_PATH] = req.restore_path;
    }
    res.second.swap(app);
    return res;
}

void server_state::restore_app(dsn::message_ex *msg)
{
    configuration_restore_request request;
    dsn::unmarshall(msg, request);
    sync_app_from_backup_media(
        request, [this, msg, request](dsn::error_code err, const dsn::blob &app_info_data) {
            dsn::error_code ec = ERR_OK;
            // if err != ERR_OK, then sync_app_from_backup_media ecounter some error
            if (err != ERR_OK) {
                LOG_ERROR("sync app_info_data from backup media failed with err({})", err);
                ec = err;
            } else {
                auto pair = restore_app_info(msg, request, app_info_data);
                if (pair.first != ERR_OK) {
                    ec = pair.first;
                } else {
                    CHECK_NOTNULL(pair.second, "app info shouldn't be empty");
                    // the same with create_app
                    do_app_create(pair.second);
                    return;
                }
            }
            if (ec != ERR_OK) {
                configuration_create_app_response response;
                response.err = ec;
                response.appid = -1;
                _meta_svc->reply_data(msg, response);
                msg->release_ref();
            }
        });
}

void server_state::on_recv_restore_report(configuration_report_restore_status_rpc rpc)
{
    zauto_write_lock l(_lock);

    const configuration_report_restore_status_request &request = rpc.request();
    configuration_report_restore_status_response &response = rpc.response();
    response.err = ERR_OK;

    std::shared_ptr<app_state> app = get_app(request.pid.get_app_id());
    if (app == nullptr) {
        response.err = ERR_OBJECT_NOT_FOUND;
    } else {
        restore_state &r_state = app->helpers->restore_states[request.pid.get_partition_index()];
        if (r_state.restore_status != request.restore_status) {
            r_state.restore_status = request.restore_status;
        }
        // TODO: for simply we don't allow progress to rollback;
        // when restore-app, if meta crash, meta may assign primary to different server, so
        // progress-rollback will happen, wait to process this situation
        if (r_state.progress < request.progress) {
            r_state.progress = request.progress;
        }
        if (request.__isset.reason) {
            r_state.reason = request.reason;
        }
        LOG_INFO("{} restore report: restore_status({}), progress({})",
                 request.pid,
                 request.restore_status,
                 request.progress);
    }
}

void server_state::on_query_restore_status(configuration_query_restore_rpc rpc)
{
    zauto_read_lock l(_lock);

    const configuration_query_restore_request &request = rpc.request();
    configuration_query_restore_response &response = rpc.response();
    response.err = ERR_OK;

    std::shared_ptr<app_state> app = get_app(request.restore_app_id);
    if (app == nullptr) {
        response.err = ERR_APP_NOT_EXIST;
    } else {
        if (app->status == app_status::AS_DROPPED) {
            response.err = ERR_APP_DROPPED;
        } else {
            response.restore_progress.resize(app->partition_count,
                                             cold_backup_constant::PROGRESS_FINISHED);
            response.restore_status.resize(app->partition_count, ERR_OK);
            for (int32_t i = 0; i < app->partition_count; i++) {
                const auto &r_state = app->helpers->restore_states[i];
                const auto &p = app->partitions[i];
                if (!p.primary.is_invalid() || !p.secondaries.empty()) {
                    // already have primary, restore succeed
                    continue;
                } else {
                    if (r_state.progress < response.restore_progress[i]) {
                        response.restore_progress[i] = r_state.progress;
                    }
                }
                response.restore_status[i] = r_state.restore_status;
            }
        }
    }
}
}
}
