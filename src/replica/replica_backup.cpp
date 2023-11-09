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
// IWYU pragma: no_include <boost/detail/basic_pointerbuf.hpp>
#include <boost/lexical_cast.hpp>
// IWYU pragma: no_include <ext/alloc_traits.h>
#include <inttypes.h>
#include <stdio.h>
#include <algorithm>
#include <chrono>
#include <cstdint>
#include <ios>
#include <map>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "backup/cold_backup_context.h"
#include "backup/replica_backup_manager.h"
#include "backup_types.h"
#include "block_service/block_service_manager.h"
#include "common/gpid.h"
#include "common/replication.codes.h"
#include "common/replication_enums.h"
#include "common/replication_other_types.h"
#include "dsn.layer2_types.h"
#include "metadata_types.h"
#include "perf_counter/perf_counter.h"
#include "perf_counter/perf_counter_wrapper.h"
#include "replica.h"
#include "replica/replica_context.h"
#include "replica/replication_app_base.h"
#include "replica_stub.h"
#include "runtime/api_layer1.h"
#include "runtime/task/async_calls.h"
#include "utils/autoref_ptr.h"
#include "utils/env.h"
#include "utils/error_code.h"
#include "utils/filesystem.h"
#include "utils/flags.h"
#include "utils/fmt_logging.h"
#include "utils/strings.h"
#include "utils/thread_access_checker.h"
#include "utils/time_utils.h"

namespace dsn {
namespace dist {
namespace block_service {
class block_filesystem;
} // namespace block_service
} // namespace dist

namespace replication {

DSN_DEFINE_uint64(replication,
                  max_concurrent_uploading_file_count,
                  10,
                  "concurrent uploading file count to block service");

DSN_DECLARE_string(cold_backup_root);

void replica::on_cold_backup(const backup_request &request, /*out*/ backup_response &response)
{
    _checker.only_one_thread_access();

    const std::string &policy_name = request.policy.policy_name;
    auto backup_id = request.backup_id;
    cold_backup_context_ptr new_context(
        new cold_backup_context(this, request, FLAGS_max_concurrent_uploading_file_count));

    LOG_INFO_PREFIX("{}: received cold backup request, partition_status = {}",
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
                LOG_ERROR(
                    "{}: create cold backup block service failed, provider_type = {}, response "
                    "ERR_INVALID_PARAMETERS",
                    new_context->name,
                    request.policy.backup_provider_type);
                response.err = ERR_INVALID_PARAMETERS;
                return;
            }
            auto r = _cold_backup_contexts.insert(std::make_pair(policy_name, new_context));
            CHECK(r.second, "");
            backup_context = r.first->second;
            backup_context->block_service = block_service;
            backup_context->backup_root = request.__isset.backup_path
                                              ? dsn::utils::filesystem::path_combine(
                                                    request.backup_path, FLAGS_cold_backup_root)
                                              : FLAGS_cold_backup_root;
        }

        CHECK_EQ_PREFIX(backup_context->request.policy.policy_name, policy_name);
        cold_backup_status backup_status = backup_context->status();

        if (backup_context->request.backup_id < backup_id || backup_status == ColdBackupCanceled) {
            if (backup_status == ColdBackupCheckpointing) {
                LOG_INFO("{}: delay clearing obsoleted cold backup context, cause backup_status == "
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
                LOG_INFO("{}: clear obsoleted cold backup context, old_backup_id = {}, "
                         "old_backup_status = {}",
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
            LOG_ERROR("{}: request outdated cold backup, current_backup_id = {}, response "
                      "ERR_VERSION_OUTDATED",
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
            LOG_INFO("{}: backup is busy, status = {}, progress = {}, response ERR_BUSY",
                     backup_context->name,
                     cold_backup_status_to_string(backup_status),
                     backup_context->progress());
            response.err = ERR_BUSY;
        } else if (backup_status == ColdBackupInvalid && backup_context->start_check()) {
            _stub->_counter_cold_backup_recent_start_count->increment();
            LOG_INFO("{}: start checking backup on remote, response ERR_BUSY",
                     backup_context->name);
            tasking::enqueue(LPC_BACKGROUND_COLD_BACKUP, nullptr, [backup_context]() {
                backup_context->check_backup_on_remote();
            });
            response.err = ERR_BUSY;
        } else if (backup_status == ColdBackupChecked && backup_context->start_checkpoint()) {
            // start generating checkpoint
            LOG_INFO("{}: start generating checkpoint, response ERR_BUSY", backup_context->name);
            tasking::enqueue(LPC_BACKGROUND_COLD_BACKUP, &_tracker, [this, backup_context]() {
                generate_backup_checkpoint(backup_context);
            });
            response.err = ERR_BUSY;
        } else if ((backup_status == ColdBackupCheckpointed || backup_status == ColdBackupPaused) &&
                   backup_context->start_upload()) {
            // start uploading checkpoint
            LOG_INFO("{}: start uploading checkpoint, response ERR_BUSY", backup_context->name);
            tasking::enqueue(LPC_BACKGROUND_COLD_BACKUP, nullptr, [backup_context]() {
                backup_context->upload_checkpoint_to_remote();
            });
            response.err = ERR_BUSY;
        } else if (backup_status == ColdBackupFailed) {
            LOG_ERROR("{}: upload checkpoint failed, reason = {}, response ERR_LOCAL_APP_FAILURE",
                      backup_context->name,
                      backup_context->reason());
            response.err = ERR_LOCAL_APP_FAILURE;
            backup_context->cancel();
            _cold_backup_contexts.erase(policy_name);
        } else if (backup_status == ColdBackupCompleted) {
            LOG_INFO("{}: upload checkpoint completed, response ERR_OK", backup_context->name);
            _backup_mgr->send_clear_request_to_secondaries(backup_context->request.pid,
                                                           policy_name);

            // clear local checkpoint dirs in background thread
            _backup_mgr->background_clear_backup_checkpoint(policy_name);
            response.err = ERR_OK;
        } else {
            LOG_WARNING(
                "{}: unhandled case, handle_status = {}, real_time_status = {}, response ERR_BUSY",
                backup_context->name,
                cold_backup_status_to_string(backup_status),
                cold_backup_status_to_string(backup_context->status()));
            response.err = ERR_BUSY;
        }

        response.progress = backup_context->progress();
        response.checkpoint_total_size = backup_context->get_checkpoint_total_size();
        LOG_INFO("{}: backup progress is {}", backup_context->name, response.progress);
    } else {
        LOG_ERROR(
            "{}: invalid state for cold backup, partition_status = {}, response ERR_INVALID_STATE",
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
        LOG_WARNING("{}: found a invalid checkpoint dir({})", backup_context->name, chkpt_dirname);
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
        LOG_ERROR("{}: list sub dirs of dir {} failed", backup_context->name, dir);
        return false;
    }

    for (std::string &d : sub_dirs) {
        std::string dirname = utils::filesystem::get_file_name(d);
        int ret = is_related_or_valid_checkpoint(dirname, backup_context);
        if (ret == 1) {
            related_chkpt_dirs.emplace_back(std::move(dirname));
        } else if (ret == 2) {
            CHECK(valid_chkpt_dir.empty(),
                  "{}: there are two valid backup checkpoint dir, {} VS {}",
                  backup_context->name,
                  valid_chkpt_dir,
                  dirname);
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
        LOG_ERROR("list sub files of dir {} failed", dir);
        return false;
    }

    total_size = 0;
    file_infos.clear();

    // TODO(yingchun): check if there are any files that are not sensitive (not encrypted).
    for (std::string &file : sub_files) {
        std::pair<std::string, int64_t> file_info;

        if (!utils::filesystem::file_size(
                file, dsn::utils::FileDataType::kSensitive, file_info.second)) {
            LOG_ERROR("get file size of {} failed", file);
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
        LOG_INFO("{}: ignore generating backup checkpoint because backup_status = {}",
                 backup_context->name,
                 cold_backup_status_to_string(backup_context->status()));
        backup_context->ignore_checkpoint();
        return;
    }

    // prepare back dir
    auto backup_dir = _app->backup_dir();
    if (!utils::filesystem::directory_exists(backup_dir) &&
        !utils::filesystem::create_directory(backup_dir)) {
        LOG_ERROR("{}: create backup dir {} failed", backup_context->name, backup_dir);
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
        CHECK(backup_parse_dir_name(
                  valid_backup_chkpt_dirname.c_str(), policy_name, backup_id, decree, timestamp),
              "{}: valid chekpoint dirname {}",
              backup_context->name,
              valid_backup_chkpt_dirname);

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

            LOG_INFO(
                "{}: backup checkpoint aleady exist, dir = {}, file_count = {}, total_size = {}",
                backup_context->name,
                backup_context->checkpoint_dir,
                file_infos.size(),
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
        LOG_INFO("{}: backup checkpoint not exist, start to trigger async checkpoint",
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
        LOG_INFO("{}: found obsolete backup checkpoint dir({}), remove it",
                 backup_context->name,
                 full_path);
        if (!utils::filesystem::remove_path(full_path)) {
            LOG_WARNING("{}: remove obsolete backup checkpoint dir({}) failed",
                        backup_context->name,
                        full_path);
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
        LOG_INFO("{}: ignore triggering async checkpoint because backup_status = {}",
                 backup_context->name,
                 cold_backup_status_to_string(backup_context->status()));
        backup_context->ignore_checkpoint();
        return;
    }

    if (status() != partition_status::PS_PRIMARY && status() != partition_status::PS_SECONDARY) {
        LOG_INFO("{}: ignore triggering async checkpoint because partition_status = {}",
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
        LOG_INFO("{}: do not trigger async checkpoint because it is already triggered, "
                 "checkpoint_decree = {}, checkpoint_timestamp = {} ({}), "
                 "durable_decree_when_checkpoint = {}",
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
            CHECK_LT(backup_context->durable_decree_when_checkpoint, durable_decree);
            LOG_INFO("{}: need trigger async checkpoint again", backup_context->name);
        }
        backup_context->checkpoint_timestamp = dsn_now_ms();
        backup_context->durable_decree_when_checkpoint = durable_decree;
        char time_buf[20];
        dsn::utils::time_ms_to_date_time(backup_context->checkpoint_timestamp, time_buf, 20);
        LOG_INFO("{}: trigger async checkpoint, "
                 "checkpoint_decree = {}, checkpoint_timestamp = {} ({}), "
                 "durable_decree_when_checkpoint = {}",
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
        LOG_INFO("{}: ignore waiting async checkpoint because backup_status = {}",
                 backup_context->name,
                 cold_backup_status_to_string(backup_context->status()));
        backup_context->ignore_checkpoint();
        return;
    }

    if (status() != partition_status::PS_PRIMARY && status() != partition_status::PS_SECONDARY) {
        LOG_INFO("{}: ignore waiting async checkpoint because partition_status = {}",
                 backup_context->name,
                 enum_to_string(status()));
        backup_context->ignore_checkpoint();
        return;
    }

    decree du = last_durable_decree();
    if (du < backup_context->checkpoint_decree) {
        LOG_INFO("{}: async checkpoint not done, we just wait it done, "
                 "last_durable_decree = {}, backup_checkpoint_decree = {}",
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
        LOG_INFO("{}: async checkpoint done, last_durable_decree = {}, "
                 "backup_context->checkpoint_decree = {}",
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
        LOG_INFO("{}: ignore generating backup checkpoint because backup_status = {}",
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
        LOG_INFO("{}: create backup checkpoint failed with err = {}, try call "
                 "local_create_backup_checkpoint 10s later",
                 backup_context->name,
                 err);
        utils::filesystem::remove_path(backup_checkpoint_tmp_dir_path);
        tasking::enqueue(
            LPC_BACKGROUND_COLD_BACKUP,
            &_tracker,
            [this, backup_context]() { local_create_backup_checkpoint(backup_context); },
            0,
            std::chrono::seconds(10));
    } else {
        CHECK_GE(last_decree, backup_context->checkpoint_decree);
        backup_context->checkpoint_decree = last_decree; // update to real decree
        std::string backup_checkpoint_dir_path = utils::filesystem::path_combine(
            _app->backup_dir(),
            backup_get_dir_name(backup_context->request.policy.policy_name,
                                backup_context->request.backup_id,
                                backup_context->checkpoint_decree,
                                backup_context->checkpoint_timestamp));
        if (!utils::filesystem::rename_path(backup_checkpoint_tmp_dir_path,
                                            backup_checkpoint_dir_path)) {
            LOG_ERROR("{}: rename checkpoint dir({}) to dir({}) failed",
                      backup_context->name,
                      backup_checkpoint_tmp_dir_path,
                      backup_checkpoint_dir_path);
            utils::filesystem::remove_path(backup_checkpoint_tmp_dir_path);
            utils::filesystem::remove_path(backup_checkpoint_dir_path);
            backup_context->fail_checkpoint("rename checkpoint dir failed");
            return;
        }

        std::vector<std::pair<std::string, int64_t>> file_infos;
        int64_t total_size = 0;
        if (!statistic_file_infos_under_dir(backup_checkpoint_dir_path, file_infos, total_size)) {
            LOG_ERROR("{}: statistic file info under dir({}) failed",
                      backup_context->name,
                      backup_checkpoint_dir_path);
            backup_context->fail_checkpoint("statistic file info under dir failed");
            return;
        }

        LOG_INFO(
            "{}: generate backup checkpoint succeed, dir = {}, file_count = {}, total_size = {}",
            backup_context->name,
            backup_checkpoint_dir_path,
            file_infos.size(),
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
        LOG_INFO_PREFIX("cancel backup progress, backup_request = {}",
                        boost::lexical_cast<std::string>(pair.second->request));
    }
}

void replica::clear_cold_backup_state() { _cold_backup_contexts.clear(); }
} // namespace replication
} // namespace dsn
