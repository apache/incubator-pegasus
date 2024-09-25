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

#include <boost/cstdint.hpp>
#include <boost/lexical_cast.hpp>
#include <fmt/core.h>
#include <prometheus/check_names.h>
#include <prometheus/metric_type.h>
#include <algorithm>
#include <iterator>
#include <string_view>
#include <type_traits>
#include <utility>

#include "block_service/block_service.h"
#include "block_service/block_service_manager.h"
#include "common/backup_common.h"
#include "common/replication.codes.h"
#include "common/replication_enums.h"
#include "dsn.layer2_types.h"
#include "meta/backup_engine.h"
#include "meta/meta_data.h"
#include "meta/meta_rpc_types.h"
#include "meta/meta_state_service.h"
#include "meta_backup_service.h"
#include "meta_service.h"
#include "rpc/rpc_holder.h"
#include "rpc/rpc_host_port.h"
#include "rpc/rpc_message.h"
#include "rpc/serialization.h"
#include "runtime/api_layer1.h"
#include "security/access_controller.h"
#include "server_state.h"
#include "task/async_calls.h"
#include "task/task_code.h"
#include "utils/autoref_ptr.h"
#include "utils/blob.h"
#include "utils/chrono_literals.h"
#include "utils/defer.h"
#include "utils/flags.h"
#include "utils/fmt_logging.h"
#include "utils/time_utils.h"

DSN_DECLARE_int32(cold_backup_checkpoint_reserve_minutes);
DSN_DECLARE_int32(fd_lease_seconds);

METRIC_DEFINE_entity(backup_policy);
METRIC_DEFINE_gauge_int64(backup_policy,
                          backup_recent_duration_ms,
                          dsn::metric_unit::kMilliSeconds,
                          "The duration of recent backup");

namespace dsn {
namespace replication {

namespace {

metric_entity_ptr instantiate_backup_policy_metric_entity(const std::string &policy_name)
{
    auto entity_id = fmt::format("backup_policy@{}", policy_name);

    return METRIC_ENTITY_backup_policy.instantiate(entity_id, {{"policy_name", policy_name}});
}

bool validate_backup_interval(int64_t backup_interval_seconds, std::string &hint_message)
{
    // The backup interval must be larger than checkpoint reserve time.
    // Or the next cold backup checkpoint may be cleared by the clear operation.
    if (backup_interval_seconds <= FLAGS_cold_backup_checkpoint_reserve_minutes * 60) {
        hint_message = fmt::format(
            "backup interval must be larger than cold_backup_checkpoint_reserve_minutes={}",
            FLAGS_cold_backup_checkpoint_reserve_minutes);
        return false;
    }

    // There is a bug occurred in backup if the backup interval is less than 1 day, this is a
    // temporary resolution, the long term plan is to remove periodic backup.
    // See details https://github.com/apache/incubator-pegasus/issues/1081.
    if (backup_interval_seconds < 86400) {
        hint_message = fmt::format("backup interval must be >= 86400 (1 day)");
        return false;
    }

    return true;
}

} // anonymous namespace

backup_policy_metrics::backup_policy_metrics(const std::string &policy_name)
    : _policy_name(policy_name),
      _backup_policy_metric_entity(instantiate_backup_policy_metric_entity(policy_name)),
      METRIC_VAR_INIT_backup_policy(backup_recent_duration_ms)
{
}

const metric_entity_ptr &backup_policy_metrics::backup_policy_metric_entity() const
{
    CHECK_NOTNULL(_backup_policy_metric_entity,
                  "backup_policy metric entity (policy_name={}) should has been instantiated: "
                  "uninitialized entity cannot be used to instantiate metric",
                  _policy_name);
    return _backup_policy_metric_entity;
}

// TODO: backup_service and policy_context should need two locks, its own _lock and server_state's
// _lock this maybe lead to deadlock, should refactor this

void policy_context::start_backup_app_meta_unlocked(int32_t app_id)
{
    server_state *state = _backup_service->get_state();
    dsn::blob buffer;
    bool app_available = false;
    {
        zauto_read_lock l;
        state->lock_read(l);
        const std::shared_ptr<app_state> &app = state->get_app(app_id);
        if (app != nullptr && app->status == app_status::AS_AVAILABLE) {
            app_available = true;
            // do not persistent envs to backup file
            if (app->envs.empty()) {
                buffer = dsn::json::json_forwarder<app_info>::encode(*app);
            } else {
                app_state tmp = *app;
                tmp.envs.clear();
                buffer = dsn::json::json_forwarder<app_info>::encode(tmp);
            }
        }
    }

    // if app is dropped when app is under backuping, we just skip backup this app this time, and
    // also we will not write backup-finish-flag on fds
    if (!app_available) {
        LOG_WARNING(
            "{}: can't encode app_info for app({}), perhaps removed, treat it as backup finished",
            _backup_sig,
            app_id);
        auto iter = _progress.unfinished_partitions_per_app.find(app_id);
        CHECK(iter != _progress.unfinished_partitions_per_app.end(),
              "{}: can't find app({}) in unfished_map",
              _backup_sig,
              app_id);
        _progress.is_app_skipped[app_id] = true;
        int total_partitions = iter->second;
        for (int32_t pidx = 0; pidx < total_partitions; ++pidx) {
            update_partition_progress_unlocked(
                gpid(app_id, pidx), cold_backup_constant::PROGRESS_FINISHED, dsn::host_port());
        }
        return;
    }

    dist::block_service::create_file_request create_file_req;
    create_file_req.ignore_metadata = true;
    create_file_req.file_name = cold_backup::get_app_metadata_file(_backup_service->backup_root(),
                                                                   _policy.app_names.at(app_id),
                                                                   app_id,
                                                                   _cur_backup.backup_id);

    dsn::error_code err;
    dist::block_service::block_file_ptr remote_file;
    // here we can use synchronous way coz create_file with ignored metadata is very fast
    _block_service
        ->create_file(create_file_req,
                      TASK_CODE_EXEC_INLINED,
                      [&err, &remote_file](const dist::block_service::create_file_response &resp) {
                          err = resp.err;
                          remote_file = resp.file_handle;
                      })
        ->wait();
    if (err != dsn::ERR_OK) {
        LOG_ERROR("{}: create file {} failed, restart this backup later",
                  _backup_sig,
                  create_file_req.file_name);
        tasking::enqueue(
            LPC_DEFAULT_CALLBACK,
            &_tracker,
            [this, app_id]() {
                zauto_lock l(_lock);
                start_backup_app_meta_unlocked(app_id);
            },
            0,
            _backup_service->backup_option().block_retry_delay_ms);
        return;
    }
    CHECK_NOTNULL(remote_file,
                  "{}: create file({}) succeed, but can't get handle",
                  _backup_sig,
                  create_file_req.file_name);

    remote_file->write(
        dist::block_service::write_request{buffer},
        LPC_DEFAULT_CALLBACK,
        [this, remote_file, buffer, app_id](const dist::block_service::write_response &resp) {
            if (resp.err == dsn::ERR_OK) {
                CHECK_EQ(resp.written_size, buffer.length());
                {
                    zauto_lock l(_lock);
                    LOG_INFO("{}: successfully backup app metadata to {}",
                             _policy.policy_name,
                             remote_file->file_name());
                    start_backup_app_partitions_unlocked(app_id);
                }
            } else if (resp.err == ERR_FS_INTERNAL) {
                zauto_lock l(_lock);
                _is_backup_failed = true;
                LOG_ERROR("write {} failed, err = {}, don't try again when got this error.",
                          remote_file->file_name(),
                          resp.err);
                return;
            } else {
                LOG_WARNING("write {} failed, reason({}), try it later",
                            remote_file->file_name(),
                            resp.err);
                tasking::enqueue(
                    LPC_DEFAULT_CALLBACK,
                    &_tracker,
                    [this, app_id]() {
                        zauto_lock l(_lock);
                        start_backup_app_meta_unlocked(app_id);
                    },
                    0,
                    _backup_service->backup_option().block_retry_delay_ms);
            }
        },
        &_tracker);
}

void policy_context::start_backup_app_partitions_unlocked(int32_t app_id)
{
    auto iter = _progress.unfinished_partitions_per_app.find(app_id);
    CHECK(iter != _progress.unfinished_partitions_per_app.end(),
          "{}: can't find app({}) in unfinished apps",
          _backup_sig,
          app_id);
    for (int32_t i = 0; i < iter->second; ++i) {
        start_backup_partition_unlocked(gpid(app_id, i));
    }
}

void policy_context::write_backup_app_finish_flag_unlocked(int32_t app_id,
                                                           dsn::task_ptr write_callback)
{
    if (_progress.is_app_skipped[app_id]) {
        LOG_WARNING("app is unavaliable, skip write finish flag for this app(app_id = {})", app_id);
        if (write_callback != nullptr) {
            write_callback->enqueue();
        }
        return;
    }

    backup_flag flag;
    flag.total_checkpoint_size = 0;

    for (const auto &pair : _progress.app_chkpt_size[app_id]) {
        flag.total_checkpoint_size += pair.second;
    }

    dsn::error_code err;
    dist::block_service::block_file_ptr remote_file;

    dist::block_service::create_file_request create_file_req;
    create_file_req.ignore_metadata = true;
    create_file_req.file_name =
        cold_backup::get_app_backup_status_file(_backup_service->backup_root(),
                                                _policy.app_names.at(app_id),
                                                app_id,
                                                _cur_backup.backup_id);
    // here we can use synchronous way coz create_file with ignored metadata is very fast
    _block_service
        ->create_file(create_file_req,
                      TASK_CODE_EXEC_INLINED,
                      [&err, &remote_file](const dist::block_service::create_file_response &resp) {
                          err = resp.err;
                          remote_file = resp.file_handle;
                      })
        ->wait();

    if (err != ERR_OK) {
        LOG_ERROR("{}: create file {} failed, restart this backup later",
                  _backup_sig,
                  create_file_req.file_name);
        tasking::enqueue(
            LPC_DEFAULT_CALLBACK,
            &_tracker,
            [this, app_id, write_callback]() {
                zauto_lock l(_lock);
                write_backup_app_finish_flag_unlocked(app_id, write_callback);
            },
            0,
            _backup_service->backup_option().block_retry_delay_ms);
        return;
    }

    CHECK_NOTNULL(remote_file,
                  "{}: create file({}) succeed, but can't get handle",
                  _backup_sig,
                  create_file_req.file_name);
    if (remote_file->get_size() > 0) {
        // we only focus whether app_backup_status file is exist, so ignore app_backup_status file's
        // context
        LOG_INFO("app({}) already write finish-flag on block service", app_id);
        if (write_callback != nullptr) {
            write_callback->enqueue();
        }
        return;
    }

    blob buf = ::dsn::json::json_forwarder<backup_flag>::encode(flag);

    remote_file->write(
        dist::block_service::write_request{buf},
        LPC_DEFAULT_CALLBACK,
        [this, app_id, write_callback, remote_file](
            const dist::block_service::write_response &resp) {
            if (resp.err == ERR_OK) {
                LOG_INFO("app({}) finish backup and write finish-flag on block service succeed",
                         app_id);
                if (write_callback != nullptr) {
                    write_callback->enqueue();
                }
            } else if (resp.err == ERR_FS_INTERNAL) {
                zauto_lock l(_lock);
                _is_backup_failed = true;
                LOG_ERROR("write {} failed, err = {}, don't try again when got this error.",
                          remote_file->file_name(),
                          resp.err);
                return;
            } else {
                LOG_WARNING("write {} failed, reason({}), try it later",
                            remote_file->file_name(),
                            resp.err);
                tasking::enqueue(
                    LPC_DEFAULT_CALLBACK,
                    &_tracker,
                    [this, app_id, write_callback]() {
                        zauto_lock l(_lock);
                        write_backup_app_finish_flag_unlocked(app_id, write_callback);
                    },
                    0,
                    _backup_service->backup_option().block_retry_delay_ms);
            }
        });
}

void policy_context::finish_backup_app_unlocked(int32_t app_id)
{
    LOG_INFO("{}: finish backup for app({}), progress({})",
             _backup_sig,
             app_id,
             _progress.unfinished_apps);
    if (--_progress.unfinished_apps == 0) {
        LOG_INFO("{}: finish current backup for all apps", _backup_sig);
        _cur_backup.end_time_ms = dsn_now_ms();

        task_ptr write_backup_info_callback =
            tasking::create_task(LPC_DEFAULT_CALLBACK, &_tracker, [this]() {
                task_ptr start_a_new_backup =
                    tasking::create_task(LPC_DEFAULT_CALLBACK, &_tracker, [this]() {
                        zauto_lock l(_lock);
                        auto iter = _backup_history.emplace(_cur_backup.backup_id, _cur_backup);
                        CHECK(iter.second,
                              "{}: backup_id({}) already in the backup_history",
                              _policy.policy_name,
                              _cur_backup.backup_id);
                        _cur_backup.start_time_ms = 0;
                        _cur_backup.end_time_ms = 0;
                        LOG_INFO("{}: finish an old backup, try to start a new one", _backup_sig);
                        issue_new_backup_unlocked();
                    });
                sync_backup_to_remote_storage_unlocked(_cur_backup, start_a_new_backup, false);
            });
        write_backup_info_unlocked(_cur_backup, write_backup_info_callback);
    }
}

void policy_context::write_backup_info_unlocked(const backup_info &b_info,
                                                dsn::task_ptr write_callback)
{
    dsn::error_code err;
    dist::block_service::block_file_ptr remote_file;

    dist::block_service::create_file_request create_file_req;
    create_file_req.ignore_metadata = true;
    create_file_req.file_name =
        cold_backup::get_backup_info_file(_backup_service->backup_root(), b_info.backup_id);
    // here we can use synchronous way coz create_file with ignored metadata is very fast
    _block_service
        ->create_file(create_file_req,
                      TASK_CODE_EXEC_INLINED,
                      [&err, &remote_file](const dist::block_service::create_file_response &resp) {
                          err = resp.err;
                          remote_file = resp.file_handle;
                      })
        ->wait();

    if (err != ERR_OK) {
        LOG_ERROR("{}: create file {} failed, restart this backup later",
                  _backup_sig,
                  create_file_req.file_name);
        tasking::enqueue(
            LPC_DEFAULT_CALLBACK,
            &_tracker,
            [this, b_info, write_callback]() {
                zauto_lock l(_lock);
                write_backup_info_unlocked(b_info, write_callback);
            },
            0,
            _backup_service->backup_option().block_retry_delay_ms);
        return;
    }

    CHECK_NOTNULL(remote_file,
                  "{}: create file({}) succeed, but can't get handle",
                  _backup_sig,
                  create_file_req.file_name);

    blob buf = dsn::json::json_forwarder<backup_info>::encode(b_info);

    remote_file->write(
        dist::block_service::write_request{buf},
        LPC_DEFAULT_CALLBACK,
        [this, b_info, write_callback, remote_file](
            const dist::block_service::write_response &resp) {
            if (resp.err == ERR_OK) {
                LOG_INFO("policy({}) write backup_info to cold backup media succeed",
                         _policy.policy_name);
                if (write_callback != nullptr) {
                    write_callback->enqueue();
                }
            } else if (resp.err == ERR_FS_INTERNAL) {
                zauto_lock l(_lock);
                _is_backup_failed = true;
                LOG_ERROR("write {} failed, err = {}, don't try again when got this error.",
                          remote_file->file_name(),
                          resp.err);
                return;
            } else {
                LOG_WARNING("write {} failed, reason({}), try it later",
                            remote_file->file_name(),
                            resp.err);
                tasking::enqueue(
                    LPC_DEFAULT_CALLBACK,
                    &_tracker,
                    [this, b_info, write_callback]() {
                        zauto_lock l(_lock);
                        write_backup_info_unlocked(b_info, write_callback);
                    },
                    0,
                    _backup_service->backup_option().block_retry_delay_ms);
            }
        });
}

bool policy_context::update_partition_progress_unlocked(gpid pid,
                                                        int32_t progress,
                                                        const host_port &source)
{
    int32_t &local_progress = _progress.partition_progress[pid];
    if (local_progress == cold_backup_constant::PROGRESS_FINISHED) {
        LOG_WARNING(
            "{}: backup of partition {} has been finished, ignore the backup response from {} ",
            _backup_sig,
            pid,
            source);
        return true;
    }

    if (progress < local_progress) {
        LOG_WARNING("{}: local backup progress {} is larger than progress {} from server {} for "
                    "partition {}, perhaps it's primary has changed",
                    _backup_sig,
                    local_progress,
                    progress,
                    source,
                    pid);
    }

    local_progress = progress;
    LOG_DEBUG("{}: update partition {} backup progress to {}.", _backup_sig, pid, progress);
    if (local_progress == cold_backup_constant::PROGRESS_FINISHED) {
        LOG_INFO("{}: finish backup for partition {}, the app has {} unfinished backup "
                 "partition now.",
                 _backup_sig,
                 pid,
                 _progress.unfinished_partitions_per_app[pid.get_app_id()]);

        // update the progress-chain: partition => app => current_backup_instance
        if (--_progress.unfinished_partitions_per_app[pid.get_app_id()] == 0) {
            dsn::task_ptr task_after_write_finish_flag =
                tasking::create_task(LPC_DEFAULT_CALLBACK, &_tracker, [this, pid]() {
                    zauto_lock l(_lock);
                    finish_backup_app_unlocked(pid.get_app_id());
                });
            write_backup_app_finish_flag_unlocked(pid.get_app_id(), task_after_write_finish_flag);
        }
    }
    return local_progress == cold_backup_constant::PROGRESS_FINISHED;
}

void policy_context::record_partition_checkpoint_size_unlock(const gpid &pid, int64_t size)
{
    _progress.app_chkpt_size[pid.get_app_id()][pid.get_partition_index()] = size;
}

void policy_context::start_backup_partition_unlocked(gpid pid)
{
    dsn::host_port partition_primary;
    {
        // check app and partition status
        zauto_read_lock l;
        _backup_service->get_state()->lock_read(l);
        const app_state *app = _backup_service->get_state()->get_app(pid.get_app_id()).get();

        if (app == nullptr || app->status == app_status::AS_DROPPED) {
            LOG_WARNING(
                "{}: app {} is not available, skip to backup it.", _backup_sig, pid.get_app_id());
            _progress.is_app_skipped[pid.get_app_id()] = true;
            update_partition_progress_unlocked(
                pid, cold_backup_constant::PROGRESS_FINISHED, dsn::host_port());
            return;
        }
        partition_primary = app->pcs[pid.get_partition_index()].hp_primary;
    }
    if (!partition_primary) {
        LOG_WARNING("{}: partition {} doesn't have a primary now, retry to backup it later",
                    _backup_sig,
                    pid);
        tasking::enqueue(
            LPC_DEFAULT_CALLBACK,
            &_tracker,
            [this, pid]() {
                zauto_lock l(_lock);
                start_backup_partition_unlocked(pid);
            },
            0,
            _backup_service->backup_option().reconfiguration_retry_delay_ms);
        return;
    }

    backup_request req;
    req.pid = pid;
    req.policy = *(static_cast<const policy_info *>(&_policy));
    req.backup_id = _cur_backup.backup_id;
    req.app_name = _policy.app_names.at(pid.get_app_id());
    dsn::message_ex *request =
        dsn::message_ex::create_request(RPC_COLD_BACKUP, 0, pid.thread_hash());
    dsn::marshall(request, req);
    dsn::rpc_response_task_ptr rpc_callback = rpc::create_rpc_response_task(
        request,
        &_tracker,
        [this, pid, partition_primary](error_code err, backup_response &&response) {
            on_backup_reply(err, std::move(response), pid, partition_primary);
        });
    LOG_INFO("{}: send backup command to partition {}, target_addr = {}",
             _backup_sig,
             pid,
             partition_primary);
    _backup_service->get_meta_service()->send_request(request, partition_primary, rpc_callback);
}

void policy_context::on_backup_reply(error_code err,
                                     backup_response &&response,
                                     gpid pid,
                                     const host_port &primary)
{
    LOG_INFO(
        "{}: receive backup response for partition {} from server {}.", _backup_sig, pid, primary);
    if (err == dsn::ERR_OK && response.err == dsn::ERR_OK) {
        CHECK_EQ_MSG(response.policy_name,
                     _policy.policy_name,
                     "policy names don't match, pid({}), replica_server({})",
                     pid,
                     primary);
        CHECK_EQ_MSG(response.pid,
                     pid,
                     "{}: backup pids don't match, replica_server({})",
                     _policy.policy_name,
                     primary);
        CHECK_LE_MSG(response.backup_id,
                     _cur_backup.backup_id,
                     "{}: replica server({}) has bigger backup_id({}), gpid({})",
                     _backup_sig,
                     primary,
                     response.backup_id,
                     pid);

        if (response.backup_id < _cur_backup.backup_id) {
            LOG_WARNING("{}: got a backup response of partition {} from server {}, whose backup id "
                        "{} is smaller than current backup id {},  maybe it is a stale message",
                        _backup_sig,
                        pid,
                        primary,
                        response.backup_id,
                        _cur_backup.backup_id);
        } else {
            zauto_lock l(_lock);
            record_partition_checkpoint_size_unlock(pid, response.checkpoint_total_size);
            if (update_partition_progress_unlocked(pid, response.progress, primary)) {
                // partition backup finished
                return;
            }
        }
    } else if (response.err == dsn::ERR_LOCAL_APP_FAILURE) {
        zauto_lock l(_lock);
        _is_backup_failed = true;
        LOG_ERROR("{}: backup got error {} for partition {} from {}, don't try again when got "
                  "this error.",
                  _backup_sig.c_str(),
                  response.err,
                  pid,
                  primary);
        return;
    } else {
        LOG_WARNING(
            "{}: backup got error for partition {} from {}, rpc error {}, response error {}",
            _backup_sig.c_str(),
            pid,
            primary,
            err,
            response.err);
    }

    // retry to backup the partition.
    tasking::enqueue(
        LPC_DEFAULT_CALLBACK,
        &_tracker,
        [this, pid]() {
            zauto_lock l(_lock);
            start_backup_partition_unlocked(pid);
        },
        0,
        _backup_service->backup_option().request_backup_period_ms);
}

void policy_context::initialize_backup_progress_unlocked()
{
    _progress.reset();

    zauto_read_lock l;
    _backup_service->get_state()->lock_read(l);

    // NOTICE: the unfinished_apps is initialized with the app-set's size
    // even if some apps are not available.
    _progress.unfinished_apps = _cur_backup.app_ids.size();
    for (const int32_t &app_id : _cur_backup.app_ids) {
        const std::shared_ptr<app_state> &app = _backup_service->get_state()->get_app(app_id);
        _progress.is_app_skipped[app_id] = true;
        if (app == nullptr) {
            LOG_WARNING("{}: app id({}) is invalid", _policy.policy_name, app_id);
        } else if (app->status != app_status::AS_AVAILABLE) {
            LOG_WARNING("{}: {} is not available, status({})",
                        _policy.policy_name,
                        app->get_logname(),
                        enum_to_string(app->status));
        } else {
            // NOTICE: only available apps have entry in
            // unfinished_partitions_per_app & partition_progress & app_chkpt_size
            _progress.unfinished_partitions_per_app[app_id] = app->partition_count;
            std::map<int, int64_t> partition_chkpt_size;
            for (const auto &pc : app->pcs) {
                _progress.partition_progress[pc.pid] = 0;
                partition_chkpt_size[pc.pid.get_app_id()] = 0;
            }
            _progress.app_chkpt_size[app_id] = std::move(partition_chkpt_size);
            _progress.is_app_skipped[app_id] = false;
        }
    }
}

void policy_context::prepare_current_backup_on_new_unlocked()
{
    // initialize the current backup structure
    _cur_backup.backup_id = _cur_backup.start_time_ms = static_cast<int64_t>(dsn_now_ms());
    _cur_backup.app_ids = _policy.app_ids;
    _cur_backup.app_names = _policy.app_names;
    _is_backup_failed = false;

    initialize_backup_progress_unlocked();
    _backup_sig =
        _policy.policy_name + "@" + boost::lexical_cast<std::string>(_cur_backup.backup_id);
}

void policy_context::sync_backup_to_remote_storage_unlocked(const backup_info &b_info,
                                                            task_ptr sync_callback,
                                                            bool create_new_node)
{
    dsn::blob backup_data = dsn::json::json_forwarder<backup_info>::encode(b_info);
    std::string backup_info_path =
        _backup_service->get_backup_path(_policy.policy_name, b_info.backup_id);

    auto callback = [this, b_info, sync_callback, create_new_node](dsn::error_code err) {
        if (dsn::ERR_OK == err || (create_new_node && ERR_NODE_ALREADY_EXIST == err)) {
            LOG_INFO("{}: synced backup_info({}) to remote storage successfully, "
                     "start real backup work, new_node_create({})",
                     _policy.policy_name,
                     b_info.backup_id,
                     create_new_node ? "true" : "false");
            if (sync_callback != nullptr) {
                sync_callback->enqueue();
            } else {
                LOG_WARNING("{}: empty callback", _policy.policy_name);
            }
        } else if (ERR_TIMEOUT == err) {
            LOG_ERROR("{}: sync backup info({}) to remote storage got timeout, retry it later",
                      _policy.policy_name,
                      b_info.backup_id);
            tasking::enqueue(
                LPC_DEFAULT_CALLBACK,
                &_tracker,
                [this, b_info, sync_callback, create_new_node]() {
                    zauto_lock l(_lock);
                    sync_backup_to_remote_storage_unlocked(
                        std::move(b_info), std::move(sync_callback), create_new_node);
                },
                0,
                _backup_service->backup_option().meta_retry_delay_ms);
        } else {
            CHECK(false, "{}: we can't handle this right now, error({})", _backup_sig, err);
        }
    };

    if (create_new_node) {
        _backup_service->get_meta_service()->get_remote_storage()->create_node(
            backup_info_path, LPC_DEFAULT_CALLBACK, callback, backup_data, nullptr);
    } else {
        _backup_service->get_meta_service()->get_remote_storage()->set_data(
            backup_info_path, backup_data, LPC_DEFAULT_CALLBACK, callback, nullptr);
    }
}

void policy_context::continue_current_backup_unlocked()
{
    if (_policy.is_disable) {
        LOG_INFO("{}: policy is disabled, ignore this backup and try it later",
                 _policy.policy_name);
        tasking::enqueue(
            LPC_DEFAULT_CALLBACK,
            &_tracker,
            [this]() {
                zauto_lock l(_lock);
                issue_new_backup_unlocked();
            },
            0,
            _backup_service->backup_option().issue_backup_interval_ms);
        return;
    }

    for (const int32_t &app : _cur_backup.app_ids) {
        if (_progress.unfinished_partitions_per_app.find(app) !=
            _progress.unfinished_partitions_per_app.end()) {
            start_backup_app_meta_unlocked(app);
        } else {
            dsn::task_ptr task_after_write_finish_flag =
                tasking::create_task(LPC_DEFAULT_CALLBACK, &_tracker, [this, app]() {
                    zauto_lock l(_lock);
                    finish_backup_app_unlocked(app);
                });
            write_backup_app_finish_flag_unlocked(app, task_after_write_finish_flag);
        }
    }
}

bool policy_context::should_start_backup_unlocked()
{
    uint64_t now = dsn_now_ms();
    uint64_t recent_backup_start_time_ms = 0;
    if (!_backup_history.empty()) {
        recent_backup_start_time_ms = _backup_history.rbegin()->second.start_time_ms;
    }

    // the true start time of recent backup have drifted away with the origin start time of
    // policy,
    // so we should take the drift into consideration; if user change the start time of the
    // policy,
    // we just think the change of start time as drift
    int32_t hour = 0, min = 0, sec = 0;
    if (recent_backup_start_time_ms == 0) {
        //  the first time to backup, just consider the start time
        ::dsn::utils::time_ms_to_date_time(now, hour, min, sec);
        return _policy.start_time.should_start_backup(hour, min);
    } else {
        uint64_t next_backup_time_ms =
            recent_backup_start_time_ms + _policy.backup_interval_seconds * 1000;
        if (_policy.start_time.hour != 24) {
            // user have specify the time point to start backup, so we should take the the
            // time-drift into consideration

            // compute the time-drift
            ::dsn::utils::time_ms_to_date_time(recent_backup_start_time_ms, hour, min, sec);
            int64_t time_dirft_ms = _policy.start_time.compute_time_drift_ms(hour, min);

            if (time_dirft_ms >= 0) {
                // hour:min(the true start time) >= policy.start_time :
                //      1, user move up the start time of policy, such as 20:00 to 2:00, we just
                //      think this case as time drift
                //      2, the true start time of backup is delayed, compared the origin start time
                //      of policy, we should process this case
                //      3, the true start time of backup is the same with the origin start time of
                //      policy
                next_backup_time_ms -= time_dirft_ms;
            } else {
                // hour:min(the true start time) < policy.start_time:
                //      1, user delay the start time of policy, such as 2:00 to 23:00
                //
                // these case has already been handled, we do nothing
            }
        }
        if (next_backup_time_ms <= now) {
            ::dsn::utils::time_ms_to_date_time(now, hour, min, sec);
            return _policy.start_time.should_start_backup(hour, min);
        } else {
            return false;
        }
    }
}

void policy_context::issue_new_backup_unlocked()
{
    // before issue new backup, we check whether the policy is dropped
    if (_policy.is_disable) {
        LOG_INFO("{}: policy is disabled, just ignore backup, try it later", _policy.policy_name);
        tasking::enqueue(
            LPC_DEFAULT_CALLBACK,
            &_tracker,
            [this]() {
                zauto_lock l(_lock);
                issue_new_backup_unlocked();
            },
            0,
            _backup_service->backup_option().issue_backup_interval_ms);
        return;
    }

    if (!should_start_backup_unlocked()) {
        tasking::enqueue(
            LPC_DEFAULT_CALLBACK,
            &_tracker,
            [this]() {
                zauto_lock l(_lock);
                issue_new_backup_unlocked();
            },
            0,
            _backup_service->backup_option().issue_backup_interval_ms);
        LOG_INFO("{}: start issue new backup {}ms later",
                 _policy.policy_name,
                 _backup_service->backup_option().issue_backup_interval_ms.count());
        return;
    }

    prepare_current_backup_on_new_unlocked();
    // if all apps are dropped, we don't issue a new backup
    if (_progress.unfinished_partitions_per_app.empty()) {
        // TODO: just ignore this backup and wait next backup
        LOG_WARNING("{}: all apps have been dropped, ignore this backup and retry it later",
                    _backup_sig);
        tasking::enqueue(
            LPC_DEFAULT_CALLBACK,
            &_tracker,
            [this]() {
                zauto_lock l(_lock);
                issue_new_backup_unlocked();
            },
            0,
            _backup_service->backup_option().issue_backup_interval_ms);
    } else {
        task_ptr continue_to_backup =
            tasking::create_task(LPC_DEFAULT_CALLBACK, &_tracker, [this]() {
                zauto_lock l(_lock);
                continue_current_backup_unlocked();
            });
        sync_backup_to_remote_storage_unlocked(_cur_backup, continue_to_backup, true);
    }
}

void policy_context::start()
{
    zauto_lock l(_lock);

    if (_cur_backup.start_time_ms == 0) {
        issue_new_backup_unlocked();
    } else {
        continue_current_backup_unlocked();
    }

    CHECK(!_policy.policy_name.empty(), "policy_name should has been initialized");
    _metrics = std::make_unique<backup_policy_metrics>(_policy.policy_name);

    issue_gc_backup_info_task_unlocked();
    LOG_INFO("{}: start gc backup info task succeed", _policy.policy_name);
}

void policy_context::add_backup_history(const backup_info &info)
{
    zauto_lock l(_lock);
    if (info.end_time_ms <= 0) {
        LOG_INFO("{}: encounter an unfished backup_info({}), start_time({}), continue it later",
                 _policy.policy_name,
                 info.backup_id,
                 info.start_time_ms);

        CHECK_EQ_MSG(_cur_backup.start_time_ms,
                     0,
                     "{}: shouldn't have multiple unfinished backup instance in a policy, {} vs {}",
                     _policy.policy_name,
                     _cur_backup.backup_id,
                     info.backup_id);
        CHECK(_backup_history.empty() || info.backup_id > _backup_history.rbegin()->first,
              "{}: backup_id({}) in history larger than current({})",
              _policy.policy_name,
              _backup_history.rbegin()->first,
              info.backup_id);
        _cur_backup = info;
        initialize_backup_progress_unlocked();
        _backup_sig =
            _policy.policy_name + "@" + boost::lexical_cast<std::string>(_cur_backup.backup_id);
    } else {
        LOG_INFO("{}: add backup history, id({}), start_time({}), endtime({})",
                 _policy.policy_name,
                 info.backup_id,
                 info.start_time_ms,
                 info.end_time_ms);
        CHECK(_cur_backup.end_time_ms == 0 || info.backup_id < _cur_backup.backup_id,
              "{}: backup_id({}) in history larger than current({})",
              _policy.policy_name,
              info.backup_id,
              _cur_backup.backup_id);

        auto result_pair = _backup_history.emplace(info.backup_id, info);
        CHECK(
            result_pair.second, "{}: conflict backup id({})", _policy.policy_name, info.backup_id);
    }
}

std::vector<backup_info> policy_context::get_backup_infos(int cnt)
{
    zauto_lock l(_lock);

    std::vector<backup_info> ret;

    if (cnt > 0 && _cur_backup.start_time_ms > 0) {
        ret.emplace_back(_cur_backup);
        cnt--;
    }

    for (auto it = _backup_history.rbegin(); it != _backup_history.rend() && cnt > 0; it++) {
        cnt--;
        ret.emplace_back(it->second);
    }
    return ret;
}

bool policy_context::is_under_backuping()
{
    zauto_lock l(_lock);
    if (!_is_backup_failed && _cur_backup.start_time_ms > 0 && _cur_backup.end_time_ms <= 0) {
        return true;
    }
    return false;
}

void policy_context::set_policy(const policy &p)
{
    zauto_lock l(_lock);

    const std::string old_backup_provider_type = _policy.backup_provider_type;
    _policy = p;
    if (_policy.backup_provider_type != old_backup_provider_type) {
        _block_service = _backup_service->get_meta_service()
                             ->get_block_service_manager()
                             .get_or_create_block_filesystem(_policy.backup_provider_type);
    }
    CHECK(_block_service,
          "can't initialize block filesystem by provider ({})",
          _policy.backup_provider_type);
}

policy policy_context::get_policy()
{
    zauto_lock l(_lock);
    return _policy;
}

void policy_context::gc_backup_info_unlocked(const backup_info &info_to_gc)
{
    char start_time[30] = {'\0'};
    char end_time[30] = {'\0'};
    ::dsn::utils::time_ms_to_date_time(
        static_cast<uint64_t>(info_to_gc.start_time_ms), start_time, 30);
    ::dsn::utils::time_ms_to_date_time(static_cast<uint64_t>(info_to_gc.end_time_ms), end_time, 30);
    LOG_INFO("{}: start to gc backup info, backup_id({}), start_time({}), end_time({})",
             _policy.policy_name,
             info_to_gc.backup_id,
             start_time,
             end_time);

    dsn::task_ptr sync_callback =
        ::dsn::tasking::create_task(LPC_DEFAULT_CALLBACK, &_tracker, [this, info_to_gc]() {
            dist::block_service::remove_path_request req;
            req.path =
                cold_backup::get_backup_path(_backup_service->backup_root(), info_to_gc.backup_id);
            req.recursive = true;
            _block_service->remove_path(
                req,
                LPC_DEFAULT_CALLBACK,
                [this, info_to_gc](const dist::block_service::remove_path_response &resp) {
                    // remove dir ok or dir is not exist
                    if (resp.err == ERR_OK || resp.err == ERR_OBJECT_NOT_FOUND) {
                        dsn::task_ptr remove_local_backup_info_task = tasking::create_task(
                            LPC_DEFAULT_CALLBACK, &_tracker, [this, info_to_gc]() {
                                zauto_lock l(_lock);
                                _backup_history.erase(info_to_gc.backup_id);
                                issue_gc_backup_info_task_unlocked();
                            });
                        sync_remove_backup_info(info_to_gc, remove_local_backup_info_task);
                    } else { // ERR_FS_INTERNAL, ERR_TIMEOUT, ERR_DIR_NOT_EMPTY
                        LOG_WARNING(
                            "{}: gc backup info, id({}) failed, with err = {}, just try again",
                            _policy.policy_name,
                            info_to_gc.backup_id,
                            resp.err);
                        gc_backup_info_unlocked(info_to_gc);
                    }
                });
        });
    sync_backup_to_remote_storage_unlocked(info_to_gc, sync_callback, false);
}

void policy_context::issue_gc_backup_info_task_unlocked()
{
    if (_backup_history.size() > _policy.backup_history_count_to_keep) {
        backup_info &info = _backup_history.begin()->second;
        info.info_status = backup_info_status::type::DELETING;
        LOG_INFO("{}: start to gc backup info with id({})", _policy.policy_name, info.backup_id);

        tasking::create_task(LPC_DEFAULT_CALLBACK, &_tracker, [this, info]() {
            gc_backup_info_unlocked(info);
        })->enqueue();
    } else {
        // there is no extra backup to gc, we just issue a new task to call
        // issue_gc_backup_info_task_unlocked later
        LOG_DEBUG("{}: no need to gc backup info, start it later", _policy.policy_name);
        tasking::create_task(LPC_DEFAULT_CALLBACK, &_tracker, [this]() {
            zauto_lock l(_lock);
            issue_gc_backup_info_task_unlocked();
        })->enqueue(std::chrono::minutes(3));
    }

    // update recent backup duration time
    uint64_t last_backup_duration_time_ms = 0;
    if (_cur_backup.start_time_ms == 0) {
        if (!_backup_history.empty()) {
            const backup_info &b_info = _backup_history.rbegin()->second;
            last_backup_duration_time_ms = (b_info.end_time_ms - b_info.start_time_ms);
        }
    } else if (_cur_backup.start_time_ms > 0) {
        if (_cur_backup.end_time_ms == 0) {
            last_backup_duration_time_ms = (dsn_now_ms() - _cur_backup.start_time_ms);
        } else if (_cur_backup.end_time_ms > 0) {
            last_backup_duration_time_ms = (_cur_backup.end_time_ms - _cur_backup.start_time_ms);
        }
    }
    METRIC_SET(*_metrics, backup_recent_duration_ms, last_backup_duration_time_ms);
}

void policy_context::sync_remove_backup_info(const backup_info &info, dsn::task_ptr sync_callback)
{
    std::string backup_info_path =
        _backup_service->get_backup_path(_policy.policy_name, info.backup_id);
    auto callback = [this, info, sync_callback](dsn::error_code err) {
        if (err == dsn::ERR_OK || err == dsn::ERR_OBJECT_NOT_FOUND) {
            LOG_INFO("{}: sync remove backup_info on remote storage successfully, backup_id({})",
                     _policy.policy_name,
                     info.backup_id);
            if (sync_callback != nullptr) {
                sync_callback->enqueue();
            }
        } else if (err == ERR_TIMEOUT) {
            LOG_ERROR("{}: sync remove backup info on remote storage got timeout, retry it later",
                      _policy.policy_name);
            tasking::enqueue(
                LPC_DEFAULT_CALLBACK,
                &_tracker,
                [this, info, sync_callback]() { sync_remove_backup_info(info, sync_callback); },
                0,
                _backup_service->backup_option().meta_retry_delay_ms);
        } else {
            CHECK(false, "{}: we can't handle this right now, error({})", _policy.policy_name, err);
        }
    };

    _backup_service->get_meta_service()->get_remote_storage()->delete_node(
        backup_info_path, true, LPC_DEFAULT_CALLBACK, callback, nullptr);
}

backup_service::backup_service(meta_service *meta_svc,
                               const std::string &policy_meta_root,
                               const std::string &backup_root,
                               const policy_factory &factory)
    : _factory(factory),
      _meta_svc(meta_svc),
      _policy_meta_root(policy_meta_root),
      _backup_root(backup_root)
{
    _state = _meta_svc->get_server_state();

    _opt.meta_retry_delay_ms = 10000_ms;
    _opt.block_retry_delay_ms = 60000_ms;
    _opt.app_dropped_retry_delay_ms = 600000_ms;
    _opt.reconfiguration_retry_delay_ms = 15000_ms;
    _opt.request_backup_period_ms = 10000_ms;
    _opt.issue_backup_interval_ms = 300000_ms;

    _in_initialize.store(true);
}

void backup_service::start_create_policy_meta_root(dsn::task_ptr callback)
{
    LOG_DEBUG("create policy meta root({}) on remote_storage", _policy_meta_root);
    _meta_svc->get_remote_storage()->create_node(
        _policy_meta_root, LPC_DEFAULT_CALLBACK, [this, callback](dsn::error_code err) {
            if (err == dsn::ERR_OK || err == ERR_NODE_ALREADY_EXIST) {
                LOG_INFO(
                    "create policy meta root({}) succeed, with err({})", _policy_meta_root, err);
                callback->enqueue();
            } else if (err == dsn::ERR_TIMEOUT) {
                LOG_ERROR("create policy meta root({}) timeout, try it later", _policy_meta_root);
                dsn::tasking::enqueue(
                    LPC_DEFAULT_CALLBACK,
                    &_tracker,
                    std::bind(&backup_service::start_create_policy_meta_root, this, callback),
                    0,
                    _opt.meta_retry_delay_ms);
            } else {
                CHECK(false, "we can't handle this error({}) right now", err);
            }
        });
}

void backup_service::start_sync_policies()
{
    // TODO: make sync_policies_from_remote_storage function to async
    //       sync-api will leader to deadlock when the threadnum = 1 in default threadpool
    LOG_INFO("backup service start to sync policies from remote storage");
    dsn::error_code err = sync_policies_from_remote_storage();
    if (err == dsn::ERR_OK) {
        for (auto &policy_kv : _policy_states) {
            LOG_INFO("policy({}) start to backup", policy_kv.first);
            policy_kv.second->start();
        }
        if (_policy_states.empty()) {
            LOG_WARNING(
                "can't sync policies from remote storage, user should config some policies");
        }
        _in_initialize.store(false);
    } else if (err == dsn::ERR_TIMEOUT) {
        LOG_ERROR("sync policies got timeout, retry it later");
        dsn::tasking::enqueue(LPC_DEFAULT_CALLBACK,
                              &_tracker,
                              std::bind(&backup_service::start_sync_policies, this),
                              0,
                              _opt.meta_retry_delay_ms);
    } else {
        CHECK(false,
              "sync policies from remote storage encounter error({}), we can't handle "
              "this right now");
    }
}

error_code backup_service::sync_policies_from_remote_storage()
{
    // policy on remote storage:
    //      -- <root>/policy_name/backup_id_1
    //      --                   /backup_id_2
    error_code err = ERR_OK;
    dsn::task_tracker tracker;

    auto init_backup_info = [this, &err, &tracker](const std::string &policy_name) {
        auto after_get_backup_info = [this, &err, policy_name](error_code ec, const blob &value) {
            if (ec == ERR_OK) {
                LOG_DEBUG("sync a backup string({}) from remote storage", value.data());
                backup_info tbackup_info;
                dsn::json::json_forwarder<backup_info>::decode(value, tbackup_info);

                policy_context *ptr = nullptr;
                {
                    zauto_lock l(_lock);
                    auto it = _policy_states.find(policy_name);
                    if (it == _policy_states.end()) {
                        CHECK(false,
                              "before initializing the backup_info, initialize the policy first");
                    }
                    ptr = it->second.get();
                }
                ptr->add_backup_history(tbackup_info);
            } else {
                err = ec;
                LOG_INFO("init backup_info from remote storage fail, error_code = {}", ec);
            }
        };
        std::string backup_info_root = get_policy_path(policy_name);

        _meta_svc->get_remote_storage()->get_children(
            backup_info_root,
            LPC_DEFAULT_CALLBACK, // TASK_CODE_EXEC_INLINED,
            [this, &err, &tracker, policy_name, after_get_backup_info](
                error_code ec, const std::vector<std::string> &children) {
                if (ec == ERR_OK) {
                    if (children.size() > 0) {
                        for (const auto &b_id : children) {
                            int64_t backup_id = boost::lexical_cast<int64_t>(b_id);
                            std::string backup_path = get_backup_path(policy_name, backup_id);
                            LOG_INFO("start to acquire backup_info({}) of policy({})",
                                     backup_id,
                                     policy_name);
                            _meta_svc->get_remote_storage()->get_data(
                                backup_path,
                                TASK_CODE_EXEC_INLINED,
                                std::move(after_get_backup_info),
                                &tracker);
                        }
                    } else // have not backup
                    {
                        LOG_INFO("policy has not started a backup process, policy_name = {}",
                                 policy_name);
                    }
                } else {
                    err = ec;
                    LOG_ERROR("get backup info dirs fail from remote storage, backup_dirs_root = "
                              "{}, err = {}",
                              get_policy_path(policy_name),
                              ec);
                }
            },
            &tracker);
    };

    auto init_one_policy =
        [this, &err, &tracker, &init_backup_info](const std::string &policy_name) {
            auto policy_path = get_policy_path(policy_name);
            LOG_INFO("start to acquire the context of policy({})", policy_name);
            _meta_svc->get_remote_storage()->get_data(
                policy_path,
                LPC_DEFAULT_CALLBACK, // TASK_CODE_EXEC_INLINED,
                [this, &err, &init_backup_info, policy_path, policy_name](error_code ec,
                                                                          const blob &value) {
                    if (ec == ERR_OK) {
                        std::shared_ptr<policy_context> policy_ctx = _factory(this);
                        policy tpolicy;
                        dsn::json::json_forwarder<policy>::decode(value, tpolicy);
                        policy_ctx->set_policy(tpolicy);

                        {
                            zauto_lock l(_lock);
                            _policy_states.insert(std::make_pair(policy_name, policy_ctx));
                        }
                        init_backup_info(policy_name);
                    } else {
                        err = ec;
                        LOG_ERROR(
                            "init policy fail, policy_path = {}, error_code = {}", policy_path, ec);
                    }
                },
                &tracker);
        };

    _meta_svc->get_remote_storage()->get_children(
        _policy_meta_root,
        LPC_DEFAULT_CALLBACK, // TASK_CODE_EXEC_INLINED,,
        [&err, &init_one_policy](error_code ec, const std::vector<std::string> &children) {
            if (ec == ERR_OK) {
                // children's name is name of each policy
                for (const auto &policy_name : children) {
                    init_one_policy(policy_name);
                }
            } else {
                err = ec;
                LOG_ERROR("get policy dirs from remote storage fail, error_code = {}", ec);
            }
        },
        &tracker);
    tracker.wait_outstanding_tasks();
    return err;
}

void backup_service::start()
{
    dsn::task_ptr after_create_policy_meta_root =
        tasking::create_task(LPC_DEFAULT_CALLBACK, &_tracker, [this]() { start_sync_policies(); });
    start_create_policy_meta_root(after_create_policy_meta_root);
}

void backup_service::add_backup_policy(dsn::message_ex *msg)
{
    configuration_add_backup_policy_request request;
    configuration_add_backup_policy_response response;
    auto log_on_failed = dsn::defer([&response]() {
        if (!response.hint_message.empty()) {
            LOG_WARNING(response.hint_message);
        }
    });

    dsn::message_ex *copied_msg = message_ex::copy_message_no_reply(*msg);
    ::dsn::unmarshall(msg, request);
    std::set<int32_t> app_ids;
    std::map<int32_t, std::string> app_names;

    std::string hint_message;
    if (!validate_backup_interval(request.backup_interval_seconds, hint_message)) {
        response.err = ERR_INVALID_PARAMETERS;
        response.hint_message = hint_message;
        _meta_svc->reply_data(msg, response);
        msg->release_ref();
        return;
    }

    {
        // check app status
        zauto_read_lock l;
        _state->lock_read(l);
        for (auto &app_id : request.app_ids) {
            const std::shared_ptr<app_state> &app = _state->get_app(app_id);
            if (app == nullptr) {
                LOG_ERROR("app {} doesn't exist, policy {} shouldn't be added.",
                          app_id,
                          request.policy_name);
                response.err = ERR_INVALID_PARAMETERS;
                response.hint_message = "invalid app " + std::to_string(app_id);
                _meta_svc->reply_data(msg, response);
                msg->release_ref();
                return;
            }
            // when the Ranger ACL is enabled, access control will be checked for each table.
            auto access_controller = _meta_svc->get_access_controller();
            // adding multiple judgments here is to adapt to the old ACL and avoid checking again.
            if (access_controller->is_enable_ranger_acl() &&
                !access_controller->allowed(copied_msg, app->app_name)) {
                response.err = ERR_ACL_DENY;
                response.hint_message =
                    fmt::format("not authorized to add backup policy({}) for app id: {}",
                                request.policy_name,
                                app_id);
                _meta_svc->reply_data(msg, response);
                msg->release_ref();
                return;
            }
            app_ids.insert(app_id);
            app_names.insert(std::make_pair(app_id, app->app_name));
        }
    }

    {
        // check policy name
        zauto_lock l(_lock);
        if (!is_valid_policy_name_unlocked(request.policy_name, hint_message)) {
            response.err = ERR_INVALID_PARAMETERS;
            response.hint_message =
                fmt::format("invalid policy name: '{}', {}", request.policy_name, hint_message);
            _meta_svc->reply_data(msg, response);
            msg->release_ref();
            return;
        }
    }

    // check backup provider
    if (_meta_svc->get_block_service_manager().get_or_create_block_filesystem(
            request.backup_provider_type) == nullptr) {
        response.err = ERR_INVALID_PARAMETERS;
        response.hint_message = "invalid backup_provider_type: " + request.backup_provider_type;
        _meta_svc->reply_data(msg, response);
        msg->release_ref();
        return;
    }

    LOG_INFO("start to add backup polciy {}.", request.policy_name);
    std::shared_ptr<policy_context> policy_context_ptr = _factory(this);
    CHECK_NOTNULL(policy_context_ptr, "invalid policy_context");
    policy p;
    p.policy_name = request.policy_name;
    p.backup_provider_type = request.backup_provider_type;
    p.backup_interval_seconds = request.backup_interval_seconds;
    p.backup_history_count_to_keep = request.backup_history_count_to_keep;
    p.start_time.parse_from(request.start_time);
    p.app_ids = app_ids;
    p.app_names = app_names;
    policy_context_ptr->set_policy(p);
    do_add_policy(msg, policy_context_ptr, response.hint_message);
}

void backup_service::do_add_policy(dsn::message_ex *req,
                                   std::shared_ptr<policy_context> p,
                                   const std::string &hint_msg)
{
    policy cur_policy = p->get_policy();

    std::string policy_path = get_policy_path(cur_policy.policy_name);
    blob value = json::json_forwarder<policy>::encode(cur_policy);
    _meta_svc->get_remote_storage()->create_node(
        policy_path,
        LPC_DEFAULT_CALLBACK, // TASK_CODE_EXEC_INLINED,
        [this, req, p, hint_msg, policy_name = cur_policy.policy_name](error_code err) {
            if (err == ERR_OK || err == ERR_NODE_ALREADY_EXIST) {
                configuration_add_backup_policy_response resp;
                resp.hint_message = hint_msg;
                resp.err = ERR_OK;
                LOG_INFO("add backup policy succeed, policy_name = {}", policy_name);

                _meta_svc->reply_data(req, resp);
                req->release_ref();
                {
                    zauto_lock l(_lock);
                    _policy_states.insert(std::make_pair(policy_name, p));
                }
                p->start();
            } else if (err == ERR_TIMEOUT) {
                LOG_ERROR("create backup policy on remote storage timeout, retry after {} ms",
                          _opt.meta_retry_delay_ms.count());
                tasking::enqueue(LPC_DEFAULT_CALLBACK,
                                 &_tracker,
                                 std::bind(&backup_service::do_add_policy, this, req, p, hint_msg),
                                 0,
                                 _opt.meta_retry_delay_ms);
                return;
            } else {
                CHECK(false, "we can't handle this when create backup policy, err({})", err);
            }
        },
        value);
}

void backup_service::do_update_policy_to_remote_storage(
    configuration_modify_backup_policy_rpc rpc,
    const policy &p,
    std::shared_ptr<policy_context> &p_context_ptr)
{
    std::string policy_path = get_policy_path(p.policy_name);
    blob value = json::json_forwarder<policy>::encode(p);
    _meta_svc->get_remote_storage()->set_data(
        policy_path, value, LPC_DEFAULT_CALLBACK, [this, rpc, p, p_context_ptr](error_code err) {
            if (err == ERR_OK) {
                configuration_modify_backup_policy_response resp;
                resp.err = ERR_OK;
                LOG_INFO("update backup policy to remote storage succeed, policy_name = {}",
                         p.policy_name);
                p_context_ptr->set_policy(p);
            } else if (err == ERR_TIMEOUT) {
                LOG_ERROR("update backup policy to remote storage failed, policy_name = {}, "
                          "retry after {} ms",
                          p.policy_name,
                          _opt.meta_retry_delay_ms.count());
                tasking::enqueue(LPC_DEFAULT_CALLBACK,
                                 &_tracker,
                                 std::bind(&backup_service::do_update_policy_to_remote_storage,
                                           this,
                                           rpc,
                                           p,
                                           p_context_ptr),
                                 0,
                                 _opt.meta_retry_delay_ms);
            } else {
                CHECK(false, "we can't handle this when create backup policy, err({})", err);
            }
        });
}

bool backup_service::is_valid_policy_name_unlocked(const std::string &policy_name,
                                                   std::string &hint_message)
{
    // BACKUP_INFO and policy_name should not be the same, because they are in the same level in the
    // output when query the policy details, use different names to distinguish the respective
    // contents.
    if (policy_name.find(cold_backup_constant::BACKUP_INFO) != std::string::npos) {
        hint_message = "policy name is reserved";
        return false;
    }

    // Validate the policy name as a metric name in prometheus.
    if (!prometheus::CheckMetricName(policy_name)) {
        hint_message = "policy name should match regex '[a-zA-Z_:][a-zA-Z0-9_:]*' when act as a "
                       "metric name in prometheus";
        return false;
    }

    // Validate the policy name as a metric label in prometheus.
    if (!prometheus::CheckLabelName(policy_name, prometheus::MetricType::Gauge)) {
        hint_message = "policy name should match regex '[a-zA-Z_][a-zA-Z0-9_]*' when act as a "
                       "metric label in prometheus";
        return false;
    }

    const auto iter = _policy_states.find(policy_name);
    if (iter != _policy_states.end()) {
        hint_message = "policy name is already exist";
        return false;
    }

    hint_message.clear();
    return true;
}

void backup_service::query_backup_policy(query_backup_policy_rpc rpc)
{
    const configuration_query_backup_policy_request &request = rpc.request();
    configuration_query_backup_policy_response &response = rpc.response();
    auto log_on_failed = dsn::defer([&response]() {
        if (!response.hint_msg.empty()) {
            LOG_WARNING(response.hint_msg);
        }
    });

    response.err = ERR_OK;

    std::vector<std::string> policy_names = request.policy_names;
    if (policy_names.empty()) {
        // default all the policy
        zauto_lock l(_lock);
        for (const auto &pair : _policy_states) {
            policy_names.emplace_back(pair.first);
        }
    }
    for (const auto &policy_name : policy_names) {
        std::shared_ptr<policy_context> policy_context_ptr(nullptr);
        {
            zauto_lock l(_lock);
            auto it = _policy_states.find(policy_name);
            if (it != _policy_states.end()) {
                policy_context_ptr = it->second;
            }
        }
        if (policy_context_ptr == nullptr) {
            if (!response.hint_msg.empty()) {
                response.hint_msg += "\n\t";
            }
            response.hint_msg += std::string("invalid policy_name " + policy_name);
            continue;
        }

        policy cur_policy = policy_context_ptr->get_policy();
        policy_entry p_entry;
        p_entry.policy_name = cur_policy.policy_name;
        p_entry.backup_provider_type = cur_policy.backup_provider_type;
        p_entry.backup_interval_seconds = std::to_string(cur_policy.backup_interval_seconds);
        p_entry.app_ids = cur_policy.app_ids;
        p_entry.backup_history_count_to_keep = cur_policy.backup_history_count_to_keep;
        p_entry.start_time = cur_policy.start_time.to_string();
        p_entry.is_disable = cur_policy.is_disable;
        response.policys.emplace_back(p_entry);
        // acquire backup_infos
        std::vector<backup_info> b_infos =
            policy_context_ptr->get_backup_infos(request.backup_info_count);
        std::vector<backup_entry> b_entries;
        for (const auto &b_info : b_infos) {
            backup_entry b_entry;
            b_entry.backup_id = b_info.backup_id;
            b_entry.start_time_ms = b_info.start_time_ms;
            b_entry.end_time_ms = b_info.end_time_ms;
            b_entry.app_ids = b_info.app_ids;
            b_entries.emplace_back(b_entry);
        }
        response.backup_infos.emplace_back(std::move(b_entries));
        // policy_context_ptr.reset();
    }
    if (response.policys.empty()) {
        // have not pass a valid policy_name
        if (!policy_names.empty()) {
            response.err = ERR_INVALID_PARAMETERS;
        }
    }

    if (!response.hint_msg.empty()) {
        response.__isset.hint_msg = true;
    }
}

void backup_service::modify_backup_policy(configuration_modify_backup_policy_rpc rpc)
{
    const configuration_modify_backup_policy_request &request = rpc.request();
    configuration_modify_backup_policy_response &response = rpc.response();
    response.err = ERR_OK;

    auto log_on_failed = dsn::defer([&response]() {
        if (!response.hint_message.empty()) {
            LOG_WARNING(response.hint_message);
        }
    });

    std::shared_ptr<policy_context> context_ptr;
    {
        zauto_lock _(_lock);
        auto iter = _policy_states.find(request.policy_name);
        if (iter == _policy_states.end()) {
            response.err = ERR_INVALID_PARAMETERS;
            context_ptr = nullptr;
        } else {
            context_ptr = iter->second;
        }
    }
    if (context_ptr == nullptr) {
        return;
    }
    policy cur_policy = context_ptr->get_policy();

    bool is_under_backup = context_ptr->is_under_backuping();
    bool have_modify_policy = false;
    std::vector<int32_t> valid_app_ids_to_add;
    std::map<int32_t, std::string> id_to_app_names;
    if (request.__isset.add_appids) {
        // lock the _lock of server_state to acquire verify the apps that added to policy
        zauto_read_lock l;
        _state->lock_read(l);

        for (const auto &appid : request.add_appids) {
            const auto &app = _state->get_app(appid);
            auto access_controller = _meta_svc->get_access_controller();
            // TODO: if app is dropped, how to process
            if (app == nullptr) {
                LOG_WARNING("{}: add app to policy failed, because invalid app({}), ignore it",
                            cur_policy.policy_name,
                            appid);
                continue;
            }
            if (access_controller->is_enable_ranger_acl() &&
                !access_controller->allowed(rpc.dsn_request(), app->app_name)) {
                LOG_WARNING("not authorized to modify backup policy({}) for app id: {}, skip it",
                            cur_policy.policy_name,
                            appid);
                continue;
            }
            valid_app_ids_to_add.emplace_back(appid);
            id_to_app_names.insert(std::make_pair(appid, app->app_name));
            have_modify_policy = true;
        }
    }

    if (request.__isset.is_disable) {
        if (request.is_disable) {
            if (is_under_backup) {
                if (request.__isset.force_disable && request.force_disable) {
                    LOG_INFO("{}: policy is under backuping, force to disable",
                             cur_policy.policy_name);
                    cur_policy.is_disable = true;
                    have_modify_policy = true;
                } else {
                    LOG_INFO("{}: policy is under backuping, not allow to disable",
                             cur_policy.policy_name);
                    response.err = ERR_BUSY;
                }
            } else if (!cur_policy.is_disable) {
                LOG_INFO("{}: policy is marked to disable", cur_policy.policy_name);
                cur_policy.is_disable = true;
                have_modify_policy = true;
            } else { // cur_policy.is_disable = true
                LOG_INFO("{}: policy is already disabled", cur_policy.policy_name);
            }
        } else {
            if (cur_policy.is_disable) {
                cur_policy.is_disable = false;
                LOG_INFO("{}: policy is marked to enable", cur_policy.policy_name);
                have_modify_policy = true;
            } else {
                LOG_INFO("{}: policy is already enabled", cur_policy.policy_name);
                response.err = ERR_OK;
                response.hint_message = std::string("policy is already enabled");
            }
        }
    }

    if (request.__isset.add_appids && !valid_app_ids_to_add.empty()) {
        for (const auto &appid : valid_app_ids_to_add) {
            cur_policy.app_ids.insert(appid);
            cur_policy.app_names.insert(std::make_pair(appid, id_to_app_names.at(appid)));
            have_modify_policy = true;
        }
    }

    if (request.__isset.removal_appids) {
        for (const auto &appid : request.removal_appids) {
            if (appid > 0) {
                cur_policy.app_ids.erase(appid);
                LOG_INFO("{}: remove app({}) to policy", cur_policy.policy_name, appid);
                have_modify_policy = true;
            } else {
                LOG_WARNING("{}: invalid app_id({})", cur_policy.policy_name, appid);
            }
        }
    }

    if (request.__isset.new_backup_interval_sec) {
        std::string hint_message;
        if (validate_backup_interval(request.new_backup_interval_sec, hint_message)) {
            LOG_INFO("{}: policy will change backup interval from {}s to {}s",
                     cur_policy.policy_name,
                     cur_policy.backup_interval_seconds,
                     request.new_backup_interval_sec);
            cur_policy.backup_interval_seconds = request.new_backup_interval_sec;
            have_modify_policy = true;
        } else {
            LOG_WARNING("{}: invalid backup_interval_sec({}), {}",
                        cur_policy.policy_name,
                        request.new_backup_interval_sec,
                        hint_message);
        }
    }

    if (request.__isset.backup_history_count_to_keep) {
        if (request.backup_history_count_to_keep > 0) {
            LOG_INFO("{}: policy will change backup_history_count_to_keep from {} to {}",
                     cur_policy.policy_name,
                     cur_policy.backup_history_count_to_keep,
                     request.backup_history_count_to_keep);
            cur_policy.backup_history_count_to_keep = request.backup_history_count_to_keep;
            have_modify_policy = true;
        }
    }

    if (request.__isset.start_time) {
        backup_start_time t_start_time;
        if (t_start_time.parse_from(request.start_time)) {
            LOG_INFO("{}: policy change start_time from {} to {}",
                     cur_policy.policy_name,
                     cur_policy.start_time,
                     t_start_time);
            cur_policy.start_time = t_start_time;
            have_modify_policy = true;
        }
    }

    if (have_modify_policy) {
        do_update_policy_to_remote_storage(rpc, cur_policy, context_ptr);
    }
}

std::string backup_service::get_policy_path(const std::string &policy_name)
{
    std::stringstream ss;
    ss << _policy_meta_root << "/" << policy_name;
    return ss.str();
}

std::string backup_service::get_backup_path(const std::string &policy_name, int64_t backup_id)
{
    std::stringstream ss;
    ss << _policy_meta_root << "/" << policy_name << "/" << backup_id;
    return ss.str();
}

void backup_service::start_backup_app(start_backup_app_rpc rpc)
{
    const start_backup_app_request &request = rpc.request();
    start_backup_app_response &response = rpc.response();
    auto log_on_failed = dsn::defer([&response]() {
        if (!response.hint_message.empty()) {
            LOG_WARNING(response.hint_message);
        }
    });

    int32_t app_id = request.app_id;
    auto engine = std::make_shared<backup_engine>(this);
    error_code err = engine->init_backup(app_id);
    if (err != ERR_OK) {
        response.err = err;
        response.hint_message = fmt::format("Backup failed: invalid app id {}.", app_id);
        return;
    }

    err = engine->set_block_service(request.backup_provider_type);
    if (err != ERR_OK) {
        response.err = err;
        response.hint_message = fmt::format("Backup failed: invalid backup_provider_type {}.",
                                            request.backup_provider_type);
        return;
    }

    if (request.__isset.backup_path) {
        err = engine->set_backup_path(request.backup_path);
        if (err != ERR_OK) {
            response.err = err;
            response.hint_message = "Backup failed: the default backup path has already configured "
                                    "in `hdfs_service`, please modify the configuration if you "
                                    "want to use a specific backup path.";
            return;
        }
    }

    {
        zauto_lock l(_lock);
        for (const auto &backup : _backup_states) {
            if (app_id == backup->get_backup_app_id() && backup->is_in_progress()) {
                response.err = ERR_INVALID_STATE;
                response.hint_message =
                    fmt::format("Backup failed: app {} is actively being backed up.", app_id);
                return;
            }
        }
    }

    err = engine->start();
    if (err == ERR_OK) {
        int64_t backup_id = engine->get_current_backup_id();
        {
            zauto_lock l(_lock);
            _backup_states.emplace_back(std::move(engine));
        }
        response.__isset.backup_id = true;
        response.backup_id = backup_id;
        response.hint_message =
            fmt::format("Backup succeed: metadata of app {} has been successfully backed up "
                        "and backup request has been sent to replica servers.",
                        app_id);
    } else {
        response.hint_message =
            fmt::format("Backup failed: could not backup metadata for app {}.", app_id);
    }
    response.err = err;
}

void backup_service::query_backup_status(query_backup_status_rpc rpc)
{
    const query_backup_status_request &request = rpc.request();
    query_backup_status_response &response = rpc.response();
    auto log_on_failed = dsn::defer([&response]() {
        if (!response.hint_message.empty()) {
            LOG_WARNING(response.hint_message);
        }
    });

    int32_t app_id = request.app_id;
    {
        zauto_lock l(_lock);
        for (const auto &backup : _backup_states) {
            if (app_id == backup->get_backup_app_id() &&
                (!request.__isset.backup_id ||
                 request.backup_id == backup->get_current_backup_id())) {
                response.backup_items.emplace_back(backup->get_backup_item());
            }
        }
    }

    if (response.backup_items.empty()) {
        response.err = ERR_INVALID_PARAMETERS;
        response.hint_message = "Backup not found, please check app_id or backup_id.";
        return;
    }
    response.__isset.backup_items = true;
    response.hint_message = fmt::format(
        "There are {} available backups for app {}.", response.backup_items.size(), app_id);
    response.err = ERR_OK;
}

} // namespace replication
} // namespace dsn
