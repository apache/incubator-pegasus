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

#include "replica_backup_manager.h"
#include "cold_backup_context.h"
#include "replica/replica.h"

#include <dsn/dist/fmt_logging.h>
#include <dsn/utility/filesystem.h>
#include <dsn/dist/replication/replication_app_base.h>

namespace dsn {
namespace replication {

// returns true if this checkpoint dir belongs to the policy
static bool is_policy_checkpoint(const std::string &chkpt_dirname, const std::string &policy_name)
{
    std::vector<std::string> strs;
    utils::split_args(chkpt_dirname.c_str(), strs, '.');
    // backup_tmp.<policy_name>.* or backup.<policy_name>.*
    return strs.size() >= 2 &&
           (strs[0] == std::string("backup_tmp") || strs[0] == std::string("backup")) &&
           strs[1] == policy_name;
}

// get all backup checkpoint dirs which belong to the policy
static bool get_policy_checkpoint_dirs(const std::string &dir,
                                       const std::string &policy,
                                       /*out*/ std::vector<std::string> &chkpt_dirs)
{
    chkpt_dirs.clear();
    // list sub dirs
    std::vector<std::string> sub_dirs;
    if (!utils::filesystem::get_subdirectories(dir, sub_dirs, false)) {
        derror_f("list sub dirs of dir {} failed", dir.c_str());
        return false;
    }

    for (std::string &d : sub_dirs) {
        std::string dirname = utils::filesystem::get_file_name(d);
        if (is_policy_checkpoint(dirname, policy)) {
            chkpt_dirs.push_back(std::move(dirname));
        }
    }
    return true;
}

replica_backup_manager::replica_backup_manager(replica *r) : replica_base(r), _replica(r) {}

replica_backup_manager::~replica_backup_manager()
{
    if (_collect_info_timer != nullptr) {
        _collect_info_timer->cancel(true);
    }
}

void replica_backup_manager::on_clear_cold_backup(const backup_clear_request &request)
{
    _replica->_checker.only_one_thread_access();

    auto find = _replica->_cold_backup_contexts.find(request.policy_name);
    if (find != _replica->_cold_backup_contexts.end()) {
        cold_backup_context_ptr backup_context = find->second;
        if (backup_context->is_checkpointing()) {
            ddebug_replica(
                "{}: delay clearing obsoleted cold backup context, cause backup_status == "
                "ColdBackupCheckpointing",
                backup_context->name);
            tasking::enqueue(LPC_REPLICATION_COLD_BACKUP,
                             &_replica->_tracker,
                             [this, request]() { on_clear_cold_backup(request); },
                             get_gpid().thread_hash(),
                             std::chrono::seconds(100));
            return;
        }

        _replica->_cold_backup_contexts.erase(request.policy_name);
    }

    background_clear_backup_checkpoint(request.policy_name);
}

void replica_backup_manager::start_collect_backup_info()
{
    if (_collect_info_timer == nullptr) {
        _collect_info_timer =
            tasking::enqueue_timer(LPC_PER_REPLICA_COLLECT_INFO_TIMER,
                                   &_replica->_tracker,
                                   [this]() { collect_backup_info(); },
                                   std::chrono::milliseconds(_replica->options()->gc_interval_ms),
                                   get_gpid().thread_hash());
    }
}

void replica_backup_manager::collect_backup_info()
{
    uint64_t cold_backup_running_count = 0;
    uint64_t cold_backup_max_duration_time_ms = 0;
    uint64_t cold_backup_max_upload_file_size = 0;
    uint64_t now_ms = dsn_now_ms();

    // collect backup info from all of the cold backup contexts
    for (const auto &p : _replica->_cold_backup_contexts) {
        const cold_backup_context_ptr &backup_context = p.second;
        cold_backup_status backup_status = backup_context->status();
        if (_replica->status() == partition_status::type::PS_PRIMARY) {
            if (backup_status > ColdBackupInvalid && backup_status < ColdBackupCanceled) {
                cold_backup_running_count++;
            }
        } else if (_replica->status() == partition_status::type::PS_SECONDARY) {
            // secondary end backup with status ColdBackupCheckpointed
            if (backup_status > ColdBackupInvalid && backup_status < ColdBackupCheckpointed) {
                cold_backup_running_count++;
            }
        }

        if (backup_status == ColdBackupUploading) {
            cold_backup_max_duration_time_ms = std::max(
                cold_backup_max_duration_time_ms, now_ms - backup_context->get_start_time_ms());
            cold_backup_max_upload_file_size =
                std::max(cold_backup_max_upload_file_size, backup_context->get_upload_file_size());
        }
    }

    _replica->_cold_backup_running_count.store(cold_backup_running_count);
    _replica->_cold_backup_max_duration_time_ms.store(cold_backup_max_duration_time_ms);
    _replica->_cold_backup_max_upload_file_size.store(cold_backup_max_upload_file_size);
}

void replica_backup_manager::background_clear_backup_checkpoint(const std::string &policy_name)
{
    ddebug_replica("schedule to clear all checkpoint dirs of policy({}) after {} minutes",
                   policy_name,
                   _replica->options()->cold_backup_checkpoint_reserve_minutes);
    tasking::enqueue(
        LPC_BACKGROUND_COLD_BACKUP,
        &_replica->_tracker,
        [this, policy_name]() { clear_backup_checkpoint(policy_name); },
        get_gpid().thread_hash(),
        std::chrono::minutes(_replica->options()->cold_backup_checkpoint_reserve_minutes));
}

// clear all checkpoint dirs of the policy
void replica_backup_manager::clear_backup_checkpoint(const std::string &policy_name)
{
    ddebug_replica("clear all checkpoint dirs of policy({})", policy_name);
    auto backup_dir = _replica->_app->backup_dir();
    if (!utils::filesystem::directory_exists(backup_dir)) {
        return;
    }

    // Find the corresponding checkpoint dirs with policy name
    std::vector<std::string> chkpt_dirs;
    if (!get_policy_checkpoint_dirs(backup_dir, policy_name, chkpt_dirs)) {
        dwarn_replica("get checkpoint dirs in backup dir({}) failed", backup_dir);
        return;
    }

    // Remove these checkpoint dirs
    for (const std::string &dirname : chkpt_dirs) {
        std::string full_path = utils::filesystem::path_combine(backup_dir, dirname);
        if (utils::filesystem::remove_path(full_path)) {
            ddebug_replica("remove backup checkpoint dir({}) succeed", full_path);
        } else {
            dwarn_replica("remove backup checkpoint dir({}) failed", full_path);
        }
    }
}

void replica_backup_manager::send_clear_request_to_secondaries(const gpid &pid,
                                                               const std::string &policy_name)
{
    backup_clear_request request;
    request.__set_pid(pid);
    request.__set_policy_name(policy_name);

    for (const auto &target_address : _replica->_primary_states.membership.secondaries) {
        rpc::call_one_way_typed(
            target_address, RPC_CLEAR_COLD_BACKUP, request, get_gpid().thread_hash());
    }
}

} // namespace replication
} // namespace dsn
