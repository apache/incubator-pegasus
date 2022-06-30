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

#include <dsn/utility/filesystem.h>
#include <dsn/utils/time_utils.h>
#include <dsn/dist/fmt_logging.h>
#include <dsn/dist/replication/replication_app_base.h>
#include <dsn/utility/flags.h>

#include "block_service/block_service_manager.h"
#include "backup/replica_backup_manager.h"
#include "backup/cold_backup_context.h"

#include "replica.h"
#include "replica_stub.h"

namespace dsn {
namespace replication {

DSN_DEFINE_uint64("replication",
                  max_concurrent_uploading_file_count,
                  10,
                  "concurrent uploading file count to block service");

void replica::on_cold_backup(const backup_request &request, /*out*/ backup_response &response)
{
    _checker.only_one_thread_access();

    const std::string &policy_name = request.policy.policy_name;
    auto backup_id = request.backup_id;
    cold_backup_context_ptr new_context(
        new cold_backup_context(this, request, FLAGS_max_concurrent_uploading_file_count));

    ddebug_replica("{}: received cold backup request, partition_status = {}",
                   new_context->name,
                   enum_to_string(status()));

    if (status() == partition_status::type::PS_PRIMARY ||
        status() == partition_status::type::PS_SECONDARY) {
        cold_backup_context_ptr backup_context = nullptr;
        auto find = _cold_backup_contexts.find(policy_name);
        if (find != _cold_backup_contexts.end()) {
            backup_context = find->second;
        } else {
            /// TODO: policy may change provider
            dist::block_service::block_filesystem *block_service =
                _stub->_block_service_manager.get_or_create_block_filesystem(
                    request.policy.backup_provider_type);
            if (block_service == nullptr) {
                derror("%s: create cold backup block service failed, provider_type = %s, response "
                       "ERR_INVALID_PARAMETERS",
                       new_context->name,
                       request.policy.backup_provider_type.c_str());
                response.err = ERR_INVALID_PARAMETERS;
                return;
            }
            auto r = _cold_backup_contexts.insert(std::make_pair(policy_name, new_context));
            dassert(r.second, "");
            backup_context = r.first->second;
            backup_context->block_service = block_service;
            backup_context->backup_root = request.__isset.backup_path
                                              ? dsn::utils::filesystem::path_combine(
                                                    request.backup_path, _options->cold_backup_root)
                                              : _options->cold_backup_root;
        }

        dcheck_eq_replica(backup_context->request.policy.policy_name, policy_name);
        cold_backup_status backup_status = backup_context->status();

        if (backup_context->request.backup_id < backup_id || backup_status == ColdBackupCanceled) {
            if (backup_status == ColdBackupCheckpointing) {
                ddebug("%s: delay clearing obsoleted cold backup context, cause backup_status == "
                       "ColdBackupCheckpointing",
                       new_context->name);
                tasking::enqueue(LPC_REPLICATION_COLD_BACKUP,
                                 &_tracker,
                                 [this, request]() {
                                     backup_response response;
                                     on_cold_backup(request, response);
                                 },
                                 get_gpid().thread_hash(),
                                 std::chrono::seconds(100));
            } else {
                // TODO(wutao1): deleting cold backup context should be
                //               extracted as a function like try_delete_cold_backup_context;
                // clear obsoleted backup context firstly
                ddebug("%s: clear obsoleted cold backup context, old_backup_id = %" PRId64
                       ", old_backup_status = %s",
                       new_context->name,
                       backup_context->request.backup_id,
                       cold_backup_status_to_string(backup_status));
                backup_context->cancel();
                _cold_backup_contexts.erase(policy_name);
                // go to another round
                on_cold_backup(request, response);
            }
            return;
        }

        if (backup_context->request.backup_id > backup_id) {
            // backup_id is outdated
            derror("%s: request outdated cold backup, current_backup_id = %" PRId64
                   ", response ERR_VERSION_OUTDATED",
                   new_context->name,
                   backup_context->request.backup_id);
            response.err = ERR_VERSION_OUTDATED;
            return;
        }

        // for secondary, request is already filtered by primary, so if
        //      request is repeated, so generate_backup_checkpoint is already running, we do
        //      nothing;
        //      request is new, we should call generate_backup_checkpoint;

        // TODO: if secondary's status have changed, how to process the _cold_backup_state,
        // and how to process the backup_status, cancel/pause
        if (status() == partition_status::PS_SECONDARY) {
            if (backup_status == ColdBackupInvalid) {
                // new backup_request, should set status to ColdBackupChecked to allow secondary
                // can start to checkpoint
                backup_context->start_check();
                backup_context->complete_check(false);
                if (backup_context->start_checkpoint()) {
                    _stub->_counter_cold_backup_recent_start_count->increment();
                    tasking::enqueue(
                        LPC_BACKGROUND_COLD_BACKUP, &_tracker, [this, backup_context]() {
                            generate_backup_checkpoint(backup_context);
                        });
                }
            }
            return;
        }

        send_backup_request_to_secondary(request);

        if (backup_status == ColdBackupChecking || backup_status == ColdBackupCheckpointing ||
            backup_status == ColdBackupUploading) {
            // do nothing
            ddebug("%s: backup is busy, status = %s, progress = %d, response ERR_BUSY",
                   backup_context->name,
                   cold_backup_status_to_string(backup_status),
                   backup_context->progress());
            response.err = ERR_BUSY;
        } else if (backup_status == ColdBackupInvalid && backup_context->start_check()) {
            _stub->_counter_cold_backup_recent_start_count->increment();
            ddebug("%s: start checking backup on remote, response ERR_BUSY", backup_context->name);
            tasking::enqueue(LPC_BACKGROUND_COLD_BACKUP, nullptr, [backup_context]() {
                backup_context->check_backup_on_remote();
            });
            response.err = ERR_BUSY;
        } else if (backup_status == ColdBackupChecked && backup_context->start_checkpoint()) {
            // start generating checkpoint
            ddebug("%s: start generating checkpoint, response ERR_BUSY", backup_context->name);
            tasking::enqueue(LPC_BACKGROUND_COLD_BACKUP, &_tracker, [this, backup_context]() {
                generate_backup_checkpoint(backup_context);
            });
            response.err = ERR_BUSY;
        } else if ((backup_status == ColdBackupCheckpointed || backup_status == ColdBackupPaused) &&
                   backup_context->start_upload()) {
            // start uploading checkpoint
            ddebug("%s: start uploading checkpoint, response ERR_BUSY", backup_context->name);
            tasking::enqueue(LPC_BACKGROUND_COLD_BACKUP, nullptr, [backup_context]() {
                backup_context->upload_checkpoint_to_remote();
            });
            response.err = ERR_BUSY;
        } else if (backup_status == ColdBackupFailed) {
            derror("%s: upload checkpoint failed, reason = %s, response ERR_LOCAL_APP_FAILURE",
                   backup_context->name,
                   backup_context->reason());
            response.err = ERR_LOCAL_APP_FAILURE;
            backup_context->cancel();
            _cold_backup_contexts.erase(policy_name);
        } else if (backup_status == ColdBackupCompleted) {
            ddebug("%s: upload checkpoint completed, response ERR_OK", backup_context->name);
            _backup_mgr->send_clear_request_to_secondaries(backup_context->request.pid,
                                                           policy_name);

            // clear local checkpoint dirs in background thread
            _backup_mgr->background_clear_backup_checkpoint(policy_name);
            response.err = ERR_OK;
        } else {
            dwarn(
                "%s: unhandled case, handle_status = %s, real_time_status = %s, response ERR_BUSY",
                backup_context->name,
                cold_backup_status_to_string(backup_status),
                cold_backup_status_to_string(backup_context->status()));
            response.err = ERR_BUSY;
        }

        response.progress = backup_context->progress();
        response.checkpoint_total_size = backup_context->get_checkpoint_total_size();
        ddebug("%s: backup progress is %d", backup_context->name, response.progress);
    } else {
        derror(
            "%s: invalid state for cold backup, partition_status = %s, response ERR_INVALID_STATE",
            new_context->name,
            enum_to_string(status()));
        response.err = ERR_INVALID_STATE;
    }
}

void replica::send_backup_request_to_secondary(const backup_request &request)
{
    for (const auto &target_address : _primary_states.membership.secondaries) {
        // primary will send backup_request to secondary periodically
        // so, we shouldn't handle the response
        rpc::call_one_way_typed(target_address, RPC_COLD_BACKUP, request, get_gpid().thread_hash());
    }
}

// backup/backup.<policy_name>.<backup_id>.<decree>.<timestamp>
static std::string backup_get_dir_name(const std::string &policy_name,
                                       int64_t backup_id,
                                       int64_t decree,
                                       int64_t timestamp)
{
    char buffer[256];
    sprintf(buffer,
            "backup.%s.%" PRId64 ".%" PRId64 ".%" PRId64 "",
            policy_name.c_str(),
            backup_id,
            decree,
            timestamp);
    return std::string(buffer);
}

// backup/backup_tmp.<policy_name>.<backup_id>.<timestamp>
static std::string
backup_get_tmp_dir_name(const std::string &policy_name, int64_t backup_id, int64_t timestamp)
{
    char buffer[256];
    sprintf(
        buffer, "backup_tmp.%s.%" PRId64 ".%" PRId64 "", policy_name.c_str(), backup_id, timestamp);
    return std::string(buffer);
}

// returns:
//   0 : not related
//   1 : related (belong to this policy but not belong to this backup_context)
//   2 : valid (belong to this policy and belong to this backup_context)
static int is_related_or_valid_checkpoint(const std::string &chkpt_dirname,
                                          const cold_backup_context_ptr &backup_context)
{
    std::vector<std::string> strs;
    ::dsn::utils::split_args(chkpt_dirname.c_str(), strs, '.');
    if (strs.size() == 4 && strs[0] == std::string("backup_tmp") &&
        strs[1] == backup_context->request.policy.policy_name) {
        // backup_tmp.<policy_name>.<backup_id>.<timestamp>
        // refer to backup_get_tmp_dir_name().
        int64_t backup_id = boost::lexical_cast<int64_t>(strs[2]);
        if (backup_id < backup_context->request.backup_id) {
            // it belongs to old backup_context, we can remove it safely.
            return 1;
        }
    } else if (strs.size() == 5 && strs[0] == std::string("backup_tmp") &&
               strs[4] == std::string("tmp") &&
               strs[1] == backup_context->request.policy.policy_name) {
        // backup_tmp.<policy_name>.<backup_id>.<timestamp>.tmp
        // refer to CheckpointImpl::CreateCheckpointQuick().
        int64_t backup_id = boost::lexical_cast<int64_t>(strs[2]);
        if (backup_id < backup_context->request.backup_id) {
            // it belongs to old backup_context, we can remove it safely.
            return 1;
        }
    } else if (strs.size() == 5 && strs[0] == std::string("backup") &&
               strs[1] == backup_context->request.policy.policy_name) {
        // backup.<policy_name>.<backup_id>.<decree>.<timestamp>
        // refer to backup_get_dir_name().
        int64_t backup_id = boost::lexical_cast<int64_t>(strs[2]);
        // here, we only need policy_name and backup_id to verify whether chkpt_dirname belong
        // to this backup_context.
        if (backup_id == backup_context->request.backup_id) {
            // it belongs to this backup_context.
            return 2;
        } else if (backup_id < backup_context->request.backup_id) {
            // it belongs to old backup_context, we can remove it safely.
            return 1;
        }
    } else {
        // unknown dir, ignore it
        dwarn(
            "%s: found a invalid checkpoint dir(%s)", backup_context->name, chkpt_dirname.c_str());
    }
    return 0;
}

// filter backup checkpoint under 'dir'
//  - find the valid backup checkpoint dir if exist
//  - find all the backup checkpoint belong to this policy, mainly obsolete backup checkpoint
static bool filter_checkpoint(const std::string &dir,
                              const cold_backup_context_ptr &backup_context,
                              /*out*/ std::vector<std::string> &related_chkpt_dirs,
                              /*out*/ std::string &valid_chkpt_dir)
{
    valid_chkpt_dir.clear();
    related_chkpt_dirs.clear();
    // list sub dirs
    std::vector<std::string> sub_dirs;
    if (!utils::filesystem::get_subdirectories(dir, sub_dirs, false)) {
        derror("%s: list sub dirs of dir %s failed", backup_context->name, dir.c_str());
        return false;
    }

    for (std::string &d : sub_dirs) {
        std::string dirname = utils::filesystem::get_file_name(d);
        int ret = is_related_or_valid_checkpoint(dirname, backup_context);
        if (ret == 1) {
            related_chkpt_dirs.emplace_back(std::move(dirname));
        } else if (ret == 2) {
            dassert(valid_chkpt_dir.empty(),
                    "%s: there are two valid backup checkpoint dir, %s VS %s",
                    backup_context->name,
                    valid_chkpt_dir.c_str(),
                    dirname.c_str());
            valid_chkpt_dir = dirname;
        }
    }
    return true;
}

static bool
statistic_file_infos_under_dir(const std::string &dir,
                               /*out*/ std::vector<std::pair<std::string, int64_t>> &file_infos,
                               /*out*/ int64_t &total_size)
{
    std::vector<std::string> sub_files;
    if (!utils::filesystem::get_subfiles(dir, sub_files, false)) {
        derror("list sub files of dir %s failed", dir.c_str());
        return false;
    }

    total_size = 0;
    file_infos.clear();

    for (std::string &file : sub_files) {
        std::pair<std::string, int64_t> file_info;

        if (!utils::filesystem::file_size(file, file_info.second)) {
            derror("get file size of %s failed", file.c_str());
            return false;
        }
        file_info.first = utils::filesystem::get_file_name(file);
        total_size += file_info.second;

        file_infos.emplace_back(std::move(file_info));
    }
    return true;
}

static bool backup_parse_dir_name(const char *name,
                                  std::string &policy_name,
                                  int64_t &backup_id,
                                  int64_t &decree,
                                  int64_t &timestamp)
{
    std::vector<std::string> strs;
    ::dsn::utils::split_args(name, strs, '.');
    if (strs.size() < 5) {
        return false;
    } else {
        policy_name = strs[1];
        backup_id = boost::lexical_cast<int64_t>(strs[2]);
        decree = boost::lexical_cast<int64_t>(strs[3]);
        timestamp = boost::lexical_cast<int64_t>(strs[4]);
        return (std::string(name) ==
                backup_get_dir_name(policy_name, backup_id, decree, timestamp));
    }
}

// run in REPLICATION_LONG thread
// Effection:
// - may ignore_checkpoint() if in invalid status
// - may fail_checkpoint() if some error occurs
// - may complete_checkpoint() and schedule on_cold_backup() if backup checkpoint dir is already
// exist
// - may schedule trigger_async_checkpoint_for_backup() if backup checkpoint dir is not exist
void replica::generate_backup_checkpoint(cold_backup_context_ptr backup_context)
{
    if (backup_context->status() != ColdBackupCheckpointing) {
        ddebug("%s: ignore generating backup checkpoint because backup_status = %s",
               backup_context->name,
               cold_backup_status_to_string(backup_context->status()));
        backup_context->ignore_checkpoint();
        return;
    }

    // prepare back dir
    auto backup_dir = _app->backup_dir();
    if (!utils::filesystem::directory_exists(backup_dir) &&
        !utils::filesystem::create_directory(backup_dir)) {
        derror("%s: create backup dir %s failed", backup_context->name, backup_dir.c_str());
        backup_context->fail_checkpoint("create backup dir failed");
        return;
    }

    std::vector<std::string> related_backup_chkpt_dirname;
    std::string valid_backup_chkpt_dirname;
    if (!filter_checkpoint(
            backup_dir, backup_context, related_backup_chkpt_dirname, valid_backup_chkpt_dirname)) {
        // encounter some error, just return
        backup_context->fail_checkpoint("list sub backup dir failed");
        return;
    }
    if (!valid_backup_chkpt_dirname.empty()) {
        std::vector<std::pair<std::string, int64_t>> file_infos;
        int64_t total_size = 0;
        std::string valid_chkpt_full_path =
            utils::filesystem::path_combine(backup_dir, valid_backup_chkpt_dirname);
        // parse checkpoint dirname
        std::string policy_name;
        int64_t backup_id = 0, decree = 0, timestamp = 0;
        dassert(backup_parse_dir_name(
                    valid_backup_chkpt_dirname.c_str(), policy_name, backup_id, decree, timestamp),
                "%s: valid chekpoint dirname %s",
                backup_context->name,
                valid_backup_chkpt_dirname.c_str());

        if (statistic_file_infos_under_dir(valid_chkpt_full_path, file_infos, total_size)) {
            backup_context->checkpoint_decree = decree;
            backup_context->checkpoint_timestamp = timestamp;
            backup_context->checkpoint_dir = valid_chkpt_full_path;
            for (std::pair<std::string, int64_t> &p : file_infos) {
                backup_context->checkpoint_files.emplace_back(std::move(p.first));
                backup_context->checkpoint_file_sizes.emplace_back(std::move(p.second));
            }
            backup_context->checkpoint_file_total_size = total_size;
            backup_context->complete_checkpoint();

            ddebug("%s: backup checkpoint aleady exist, dir = %s, file_count = %d, total_size = "
                   "%" PRId64,
                   backup_context->name,
                   backup_context->checkpoint_dir.c_str(),
                   (int)file_infos.size(),
                   total_size);
            // TODO: in primary, this will make the request send to secondary again
            tasking::enqueue(LPC_REPLICATION_COLD_BACKUP,
                             &_tracker,
                             [this, backup_context]() {
                                 backup_response response;
                                 on_cold_backup(backup_context->request, response);
                             },
                             get_gpid().thread_hash());
        } else {
            backup_context->fail_checkpoint("statistic file info under checkpoint failed");
            return;
        }
    } else {
        ddebug("%s: backup checkpoint not exist, start to trigger async checkpoint",
               backup_context->name);
        tasking::enqueue(
            LPC_REPLICATION_COLD_BACKUP,
            &_tracker,
            [this, backup_context]() { trigger_async_checkpoint_for_backup(backup_context); },
            get_gpid().thread_hash());
    }

    // clear related but not valid checkpoint
    for (const std::string &dirname : related_backup_chkpt_dirname) {
        std::string full_path = utils::filesystem::path_combine(backup_dir, dirname);
        ddebug("%s: found obsolete backup checkpoint dir(%s), remove it",
               backup_context->name,
               full_path.c_str());
        if (!utils::filesystem::remove_path(full_path)) {
            dwarn("%s: remove obsolete backup checkpoint dir(%s) failed",
                  backup_context->name,
                  full_path.c_str());
        }
    }
}

// run in REPLICATION thread
// Effection:
// - may ignore_checkpoint() if in invalid status
// - may fail_checkpoint() if some error occurs
// - may trigger async checkpoint and invoke wait_async_checkpoint_for_backup()
void replica::trigger_async_checkpoint_for_backup(cold_backup_context_ptr backup_context)
{
    _checker.only_one_thread_access();

    if (backup_context->status() != ColdBackupCheckpointing) {
        ddebug("%s: ignore triggering async checkpoint because backup_status = %s",
               backup_context->name,
               cold_backup_status_to_string(backup_context->status()));
        backup_context->ignore_checkpoint();
        return;
    }

    if (status() != partition_status::PS_PRIMARY && status() != partition_status::PS_SECONDARY) {
        ddebug("%s: ignore triggering async checkpoint because partition_status = %s",
               backup_context->name,
               enum_to_string(status()));
        backup_context->ignore_checkpoint();
        return;
    }

    decree durable_decree = last_durable_decree();
    if (backup_context->checkpoint_decree > 0 &&
        durable_decree >= backup_context->checkpoint_decree) {
        // checkpoint done
    } else if (backup_context->checkpoint_decree > 0 &&
               backup_context->durable_decree_when_checkpoint == durable_decree) {
        // already triggered, just wait
        char time_buf[20];
        dsn::utils::time_ms_to_date_time(backup_context->checkpoint_timestamp, time_buf, 20);
        ddebug("%s: do not trigger async checkpoint because it is already triggered, "
               "checkpoint_decree = %" PRId64 ", checkpoint_timestamp = %" PRId64 " (%s), "
               "durable_decree_when_checkpoint = %" PRId64,
               backup_context->name,
               backup_context->checkpoint_decree,
               backup_context->checkpoint_timestamp,
               time_buf,
               backup_context->durable_decree_when_checkpoint);
    } else { // backup_context->checkpoint_decree == 0 ||
             // backup_context->durable_decree_when_checkpoint != durable_decree
        if (backup_context->checkpoint_decree == 0) {
            // first trigger
            backup_context->checkpoint_decree = last_committed_decree();
        } else { // backup_context->durable_decree_when_checkpoint != durable_decree
            // checkpoint generated, but is behind checkpoint_decree, need trigger again
            dassert(backup_context->durable_decree_when_checkpoint < durable_decree,
                    "durable_decree_when_checkpoint(%" PRId64 ") < durable_decree(%" PRId64 ")",
                    backup_context->durable_decree_when_checkpoint,
                    durable_decree);
            ddebug("%s: need trigger async checkpoint again", backup_context->name);
        }
        backup_context->checkpoint_timestamp = dsn_now_ms();
        backup_context->durable_decree_when_checkpoint = durable_decree;
        char time_buf[20];
        dsn::utils::time_ms_to_date_time(backup_context->checkpoint_timestamp, time_buf, 20);
        ddebug("%s: trigger async checkpoint, "
               "checkpoint_decree = %" PRId64 ", checkpoint_timestamp = %" PRId64 " (%s), "
               "durable_decree_when_checkpoint = %" PRId64,
               backup_context->name,
               backup_context->checkpoint_decree,
               backup_context->checkpoint_timestamp,
               time_buf,
               backup_context->durable_decree_when_checkpoint);
        init_checkpoint(true);
    }

    // after triggering init_checkpoint, we just wait until it finish
    wait_async_checkpoint_for_backup(backup_context);
}

// run in REPLICATION thread
// Effection:
// - may ignore_checkpoint() if in invalid status
// - may delay some time and schedule trigger_async_checkpoint_for_backup() if async checkpoint not
// completed
// - may schedule local_create_backup_checkpoint if async checkpoint completed
void replica::wait_async_checkpoint_for_backup(cold_backup_context_ptr backup_context)
{
    _checker.only_one_thread_access();

    if (backup_context->status() != ColdBackupCheckpointing) {
        ddebug("%s: ignore waiting async checkpoint because backup_status = %s",
               backup_context->name,
               cold_backup_status_to_string(backup_context->status()));
        backup_context->ignore_checkpoint();
        return;
    }

    if (status() != partition_status::PS_PRIMARY && status() != partition_status::PS_SECONDARY) {
        ddebug("%s: ignore waiting async checkpoint because partition_status = %s",
               backup_context->name,
               enum_to_string(status()));
        backup_context->ignore_checkpoint();
        return;
    }

    decree du = last_durable_decree();
    if (du < backup_context->checkpoint_decree) {
        ddebug("%s: async checkpoint not done, we just wait it done, "
               "last_durable_decree = %" PRId64 ", backup_checkpoint_decree = %" PRId64,
               backup_context->name,
               du,
               backup_context->checkpoint_decree);
        tasking::enqueue(
            LPC_REPLICATION_COLD_BACKUP,
            &_tracker,
            [this, backup_context]() { trigger_async_checkpoint_for_backup(backup_context); },
            get_gpid().thread_hash(),
            std::chrono::seconds(10));
    } else {
        ddebug("%s: async checkpoint done, last_durable_decree = %" PRId64
               ", backup_context->checkpoint_decree = %" PRId64,
               backup_context->name,
               du,
               backup_context->checkpoint_decree);
        tasking::enqueue(LPC_BACKGROUND_COLD_BACKUP, &_tracker, [this, backup_context]() {
            local_create_backup_checkpoint(backup_context);
        });
    }
}

// run in REPLICATION_LONG thread
// Effection:
// - may ignore_checkpoint() if in invalid status
// - may fail_checkpoint() if some error occurs
// - may complete_checkpoint() and schedule on_cold_backup() if checkpoint dir is successfully
// copied
void replica::local_create_backup_checkpoint(cold_backup_context_ptr backup_context)
{
    if (backup_context->status() != ColdBackupCheckpointing) {
        ddebug("%s: ignore generating backup checkpoint because backup_status = %s",
               backup_context->name,
               cold_backup_status_to_string(backup_context->status()));
        backup_context->ignore_checkpoint();
        return;
    }

    // the real checkpoint decree may be larger than backup_context->checkpoint_decree,
    // so we need copy checkpoint to backup_checkpoint_tmp_dir_path, and then rename it.
    std::string backup_checkpoint_tmp_dir_path = utils::filesystem::path_combine(
        _app->backup_dir(),
        backup_get_tmp_dir_name(backup_context->request.policy.policy_name,
                                backup_context->request.backup_id,
                                backup_context->checkpoint_timestamp));
    int64_t last_decree = 0;
    dsn::error_code err =
        _app->copy_checkpoint_to_dir(backup_checkpoint_tmp_dir_path.c_str(), &last_decree);
    if (err != ERR_OK) {
        // try local_create_backup_checkpoint 10s later
        ddebug("%s: create backup checkpoint failed with err = %s, try call "
               "local_create_backup_checkpoint 10s later",
               backup_context->name,
               err.to_string());
        utils::filesystem::remove_path(backup_checkpoint_tmp_dir_path);
        tasking::enqueue(
            LPC_BACKGROUND_COLD_BACKUP,
            &_tracker,
            [this, backup_context]() { local_create_backup_checkpoint(backup_context); },
            0,
            std::chrono::seconds(10));
    } else {
        dassert(last_decree >= backup_context->checkpoint_decree,
                "%" PRId64 " VS %" PRId64 "",
                last_decree,
                backup_context->checkpoint_decree);
        backup_context->checkpoint_decree = last_decree; // update to real decree
        std::string backup_checkpoint_dir_path = utils::filesystem::path_combine(
            _app->backup_dir(),
            backup_get_dir_name(backup_context->request.policy.policy_name,
                                backup_context->request.backup_id,
                                backup_context->checkpoint_decree,
                                backup_context->checkpoint_timestamp));
        if (!utils::filesystem::rename_path(backup_checkpoint_tmp_dir_path,
                                            backup_checkpoint_dir_path)) {
            derror("%s: rename checkpoint dir(%s) to dir(%s) failed",
                   backup_context->name,
                   backup_checkpoint_tmp_dir_path.c_str(),
                   backup_checkpoint_dir_path.c_str());
            utils::filesystem::remove_path(backup_checkpoint_tmp_dir_path);
            utils::filesystem::remove_path(backup_checkpoint_dir_path);
            backup_context->fail_checkpoint("rename checkpoint dir failed");
            return;
        }

        std::vector<std::pair<std::string, int64_t>> file_infos;
        int64_t total_size = 0;
        if (!statistic_file_infos_under_dir(backup_checkpoint_dir_path, file_infos, total_size)) {
            derror("%s: statistic file info under dir(%s) failed",
                   backup_context->name,
                   backup_checkpoint_dir_path.c_str());
            backup_context->fail_checkpoint("statistic file info under dir failed");
            return;
        }

        ddebug("%s: generate backup checkpoint succeed, dir = %s, file_count = %d, total_size = "
               "%" PRId64,
               backup_context->name,
               backup_checkpoint_dir_path.c_str(),
               (int)file_infos.size(),
               total_size);
        backup_context->checkpoint_dir = backup_checkpoint_dir_path;
        for (std::pair<std::string, int64_t> &pair : file_infos) {
            backup_context->checkpoint_files.emplace_back(std::move(pair.first));
            backup_context->checkpoint_file_sizes.emplace_back(std::move(pair.second));
        }
        backup_context->checkpoint_file_total_size = total_size;
        backup_context->complete_checkpoint();
        tasking::enqueue(LPC_REPLICATION_COLD_BACKUP,
                         &_tracker,
                         [this, backup_context]() {
                             backup_response response;
                             on_cold_backup(backup_context->request, response);
                         },
                         get_gpid().thread_hash());
    }
}

void replica::set_backup_context_cancel()
{
    for (auto &pair : _cold_backup_contexts) {
        pair.second->cancel();
        ddebug("%s: cancel backup progress, backup_request = %s",
               name(),
               boost::lexical_cast<std::string>(pair.second->request).c_str());
    }
}

void replica::clear_cold_backup_state() { _cold_backup_contexts.clear(); }
} // namespace replication
} // namespace dsn
