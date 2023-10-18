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

#include "cold_backup_context.h"

#include <chrono>
#include <cstdint>
#include <memory>

#include "common/backup_common.h"
#include "common/replication.codes.h"
#include "perf_counter/perf_counter.h"
#include "perf_counter/perf_counter_wrapper.h"
#include "replica/replica.h"
#include "replica/replica_stub.h"
#include "runtime/api_layer1.h"
#include "runtime/task/async_calls.h"
#include "utils/blob.h"
#include "utils/error_code.h"
#include "utils/filesystem.h"
#include "utils/utils.h"

namespace dsn {
namespace replication {

const char *cold_backup_status_to_string(cold_backup_status status)
{
    switch (status) {
    case ColdBackupInvalid:
        return "ColdBackupInvalid";
    case ColdBackupChecking:
        return "ColdBackupChecking";
    case ColdBackupChecked:
        return "ColdBackupChecked";
    case ColdBackupCheckpointing:
        return "ColdBackupCheckpointing";
    case ColdBackupCheckpointed:
        return "ColdBackupCheckpointed";
    case ColdBackupUploading:
        return "ColdBackupUploading";
    case ColdBackupPaused:
        return "ColdBackupPaused";
    case ColdBackupCanceled:
        return "ColdBackupCanceled";
    case ColdBackupCompleted:
        return "ColdBackupCompleted";
    case ColdBackupFailed:
        return "ColdBackupFailed";
    default:
        CHECK(false, "");
    }
    return "ColdBackupXXX";
}

void cold_backup_context::cancel()
{
    _status.store(ColdBackupCanceled);
    if (_owner_replica != nullptr) {
        _owner_replica->get_replica_stub()->_counter_cold_backup_recent_cancel_count->increment();
    }
}

bool cold_backup_context::start_check()
{
    int invalid = ColdBackupInvalid;
    if (_status.compare_exchange_strong(invalid, ColdBackupChecking)) {
        _start_time_ms = dsn_now_ms();
        return true;
    } else {
        return false;
    }
}

bool cold_backup_context::fail_check(const char *failure_reason)
{
    int checking = ColdBackupChecking;
    if (_status.compare_exchange_strong(checking, ColdBackupFailed)) {
        strncpy(_reason, failure_reason, sizeof(_reason) - 1);
        _reason[sizeof(_reason) - 1] = '\0';
        if (_owner_replica != nullptr) {
            _owner_replica->get_replica_stub()->_counter_cold_backup_recent_fail_count->increment();
        }
        return true;
    } else {
        return false;
    }
}

bool cold_backup_context::complete_check(bool uploaded)
{
    int checking = ColdBackupChecking;
    if (uploaded) {
        _progress.store(cold_backup_constant::PROGRESS_FINISHED);
        if (_owner_replica != nullptr) {
            _owner_replica->get_replica_stub()->_counter_cold_backup_recent_succ_count->increment();
        }
        return _status.compare_exchange_strong(checking, ColdBackupCompleted);
    } else {
        return _status.compare_exchange_strong(checking, ColdBackupChecked);
    }
}

bool cold_backup_context::start_checkpoint()
{
    int checked = ColdBackupChecked;
    if (_status.compare_exchange_strong(checked, ColdBackupCheckpointing)) {
        return true;
    } else {
        return false;
    }
}

bool cold_backup_context::fail_checkpoint(const char *failure_reason)
{
    int checkpointing = ColdBackupCheckpointing;
    if (_status.compare_exchange_strong(checkpointing, ColdBackupFailed)) {
        strncpy(_reason, failure_reason, sizeof(_reason) - 1);
        _reason[sizeof(_reason) - 1] = '\0';
        if (_owner_replica != nullptr) {
            _owner_replica->get_replica_stub()->_counter_cold_backup_recent_fail_count->increment();
        }
        return true;
    } else {
        return false;
    }
}

bool cold_backup_context::complete_checkpoint()
{
    int checkpointing = ColdBackupCheckpointing;
    if (_status.compare_exchange_strong(checkpointing, ColdBackupCheckpointed)) {
        return true;
    } else {
        return false;
    }
}
bool cold_backup_context::fail_upload(const char *failure_reason)
{
    int uploading = ColdBackupUploading;
    int paused = ColdBackupPaused;
    if (_status.compare_exchange_strong(uploading, ColdBackupFailed) ||
        _status.compare_exchange_strong(paused, ColdBackupFailed)) {
        strncpy(_reason, failure_reason, sizeof(_reason) - 1);
        _reason[sizeof(_reason) - 1] = '\0';
        if (_owner_replica != nullptr) {
            _owner_replica->get_replica_stub()->_counter_cold_backup_recent_fail_count->increment();
        }
        return true;
    } else {
        return false;
    }
}

bool cold_backup_context::complete_upload()
{
    int uploading = ColdBackupUploading;
    int paused = ColdBackupPaused;
    if (_status.compare_exchange_strong(uploading, ColdBackupCompleted) ||
        _status.compare_exchange_strong(paused, ColdBackupCompleted)) {
        _progress.store(cold_backup_constant::PROGRESS_FINISHED);
        if (_owner_replica != nullptr) {
            _owner_replica->get_replica_stub()->_counter_cold_backup_recent_succ_count->increment();
        }
        return true;
    } else {
        return false;
    }
}

// run in REPLICATION_LONG thread
void cold_backup_context::check_backup_on_remote()
{
    // check whether current checkpoint file is exist on remote, and verify whether the checkpoint
    // directory is exist
    std::string current_chkpt_file = cold_backup::get_current_chkpt_file(
        backup_root, request.app_name, request.pid, request.backup_id);
    dist::block_service::create_file_request req;
    req.file_name = current_chkpt_file;
    req.ignore_metadata = false;

    // incr the ref counter, and must release_ref() after callback is execute
    add_ref();

    block_service->create_file(
        std::move(req),
        LPC_BACKGROUND_COLD_BACKUP,
        [this, current_chkpt_file](const dist::block_service::create_file_response &resp) {
            if (!is_ready_for_check()) {
                LOG_INFO("{}: backup status has changed to {}, ignore checking backup on remote",
                         name,
                         cold_backup_status_to_string(status()));
                ignore_check();
            } else if (resp.err == ERR_OK) {
                const dist::block_service::block_file_ptr &file_handle = resp.file_handle;
                CHECK_NOTNULL(file_handle, "");
                if (file_handle->get_md5sum().empty() && file_handle->get_size() <= 0) {
                    LOG_INFO("{}: check backup on remote, current_checkpoint file {} is not exist",
                             name,
                             current_chkpt_file);
                    complete_check(false);
                } else {
                    LOG_INFO("{}: check backup on remote, current_checkpoint file {} is exist",
                             name,
                             current_chkpt_file);
                    read_current_chkpt_file(file_handle);
                }
            } else if (resp.err == ERR_TIMEOUT) {
                LOG_ERROR(
                    "{}: block service create file timeout, retry after 10 seconds, file = {}",
                    name,
                    current_chkpt_file);

                // before retry, should add_ref(), and must release_ref() after retry
                add_ref();

                tasking::enqueue(LPC_BACKGROUND_COLD_BACKUP,
                                 nullptr,
                                 [this]() {
                                     // before retry, should check whether the status is ready for
                                     // check
                                     if (!is_ready_for_check()) {
                                         LOG_INFO("{}: backup status has changed to {}, ignore "
                                                  "checking backup on remote",
                                                  name,
                                                  cold_backup_status_to_string(status()));
                                         ignore_check();
                                     } else {
                                         check_backup_on_remote();
                                     }
                                     release_ref();
                                 },
                                 0,
                                 std::chrono::seconds(10));
            } else {
                LOG_ERROR("{}: block service create file failed, file = {}, err = {}",
                          name,
                          current_chkpt_file,
                          resp.err);
                fail_check("block service create file failed");
            }
            release_ref();
        });
}

void cold_backup_context::read_current_chkpt_file(
    const dist::block_service::block_file_ptr &file_handle)
{
    dist::block_service::read_request req;
    req.remote_pos = 0;
    req.remote_length = -1;

    add_ref();

    file_handle->read(
        std::move(req),
        LPC_BACKGROUND_COLD_BACKUP,
        [this, file_handle](const dist::block_service::read_response &resp) {
            if (!is_ready_for_check()) {
                LOG_INFO("{}: backup status has changed to {}, ignore checking backup on remote",
                         name,
                         cold_backup_status_to_string(status()));
                ignore_check();
            } else if (resp.err == ERR_OK) {
                std::string chkpt_dirname(resp.buffer.data(), resp.buffer.length());
                if (chkpt_dirname.empty()) {
                    complete_check(false);
                } else {
                    LOG_INFO("{}: after read current_checkpoint_file, check whether remote "
                             "checkpoint dir = {} is exist",
                             name,
                             chkpt_dirname);
                    remote_chkpt_dir_exist(chkpt_dirname);
                }
            } else if (resp.err == ERR_TIMEOUT) {
                LOG_ERROR("{}: read remote file timeout, retry after 10s, file = {}",
                          name,
                          file_handle->file_name());
                add_ref();

                tasking::enqueue(LPC_BACKGROUND_COLD_BACKUP,
                                 nullptr,
                                 [this, file_handle]() {
                                     if (!is_ready_for_check()) {
                                         LOG_INFO("{}: backup status has changed to {}, ignore "
                                                  "checking backup on remote",
                                                  name,
                                                  cold_backup_status_to_string(status()));
                                         ignore_check();
                                     } else {
                                         read_current_chkpt_file(file_handle);
                                     }
                                     release_ref();
                                 },
                                 0,
                                 std::chrono::seconds(10));
            } else {
                LOG_ERROR("{}: read remote file failed, file = {}, err = {}",
                          name,
                          file_handle->file_name(),
                          resp.err);
                fail_check("read remote file failed");
            }
            release_ref();
            return;
        });
}

void cold_backup_context::remote_chkpt_dir_exist(const std::string &chkpt_dirname)
{
    dist::block_service::ls_request req;
    req.dir_name = cold_backup::get_replica_backup_path(
        backup_root, request.app_name, request.pid, request.backup_id);

    add_ref();

    block_service->list_dir(
        std::move(req),
        LPC_BACKGROUND_COLD_BACKUP,
        [this, chkpt_dirname](const dist::block_service::ls_response &resp) {
            if (!is_ready_for_check()) {
                LOG_INFO("{}: backup status has changed to {}, ignore checking backup on remote",
                         name,
                         cold_backup_status_to_string(status()));
                ignore_check();
            } else if (resp.err == ERR_OK) {
                bool found_chkpt_dir = false;
                for (const auto &entry : (*resp.entries)) {
                    if (entry.is_directory && entry.entry_name == chkpt_dirname) {
                        found_chkpt_dir = true;
                        break;
                    }
                }
                if (found_chkpt_dir) {
                    LOG_INFO("{}: remote checkpoint dir is already exist, so upload have already "
                             "complete, remote_checkpoint_dirname = {}",
                             name,
                             chkpt_dirname);
                    complete_check(true);
                } else {
                    LOG_INFO("{}: remote checkpoint dir is not exist, should re-upload checkpoint "
                             "dir, remote_checkpoint_dirname = {}",
                             name,
                             chkpt_dirname);
                    complete_check(false);
                }
            } else if (resp.err == ERR_OBJECT_NOT_FOUND) {
                LOG_INFO("{}: remote checkpoint dir is not exist, should re-upload checkpoint dir, "
                         "remote_checkpoint_dirname = {}",
                         name,
                         chkpt_dirname);
                complete_check(false);
            } else if (resp.err == ERR_TIMEOUT) {
                LOG_ERROR(
                    "{}: block service list remote dir timeout, retry after 10s, dirname = {}",
                    name,
                    chkpt_dirname);
                add_ref();

                tasking::enqueue(LPC_BACKGROUND_COLD_BACKUP,
                                 nullptr,
                                 [this, chkpt_dirname]() {
                                     if (!is_ready_for_check()) {
                                         LOG_INFO("{}: backup status has changed to {}, ignore "
                                                  "checking backup on remote",
                                                  name,
                                                  cold_backup_status_to_string(status()));
                                         ignore_check();
                                     } else {
                                         remote_chkpt_dir_exist(chkpt_dirname);
                                     }
                                     release_ref();
                                 },
                                 0,
                                 std::chrono::seconds(10));
            } else {
                LOG_ERROR("{}: block service list remote dir failed, dirname = {}, err = {}",
                          name,
                          chkpt_dirname,
                          resp.err);
                fail_check("list remote dir failed");
            }
            release_ref();
            return;
        });
}

void cold_backup_context::upload_checkpoint_to_remote()
{
    if (!is_ready_for_upload()) {
        LOG_INFO("{}: backup status has changed to {}, ignore upload checkpoint",
                 name,
                 cold_backup_status_to_string(status()));
        return;
    }

    bool old_status = false;
    // here, just allow one task to check upload status, and it will set _upload_status base on
    // the result it has checked; But, because of upload_checkpoint_to_remote maybe call multi-times
    // (for pause - uploading), so we use the atomic variant to implement
    if (!_have_check_upload_status.compare_exchange_strong(old_status, true)) {
        LOG_INFO("{}: upload status has already been checked, start upload checkpoint dir directly",
                 name);
        on_upload_chkpt_dir();
        return;
    }

    // check whether cold_backup_metadata is exist and verify cold_backup_metadata if exist
    std::string metadata = cold_backup::get_remote_chkpt_meta_file(
        backup_root, request.app_name, request.pid, request.backup_id);
    dist::block_service::create_file_request req;
    req.file_name = metadata;
    req.ignore_metadata = false;

    add_ref();

    block_service->create_file(
        std::move(req),
        LPC_BACKGROUND_COLD_BACKUP,
        [this, metadata](const dist::block_service::create_file_response &resp) {
            if (resp.err == ERR_OK) {
                CHECK_NOTNULL(resp.file_handle, "");
                if (resp.file_handle->get_md5sum().empty() && resp.file_handle->get_size() <= 0) {
                    _upload_status.store(UploadUncomplete);
                    LOG_INFO("{}: check upload_status complete, cold_backup_metadata isn't exist, "
                             "start upload checkpoint dir",
                             name);
                    on_upload_chkpt_dir();
                } else {
                    LOG_INFO("{}: cold_backup_metadata is exist, read it's context", name);
                    read_backup_metadata(resp.file_handle);
                }
            } else if (resp.err == ERR_TIMEOUT) {
                LOG_ERROR("{}: block service create file timeout, retry after 10s, file = {}",
                          name,
                          metadata);
                // when create backup_metadata timeout, should reset _have_check_upload_status
                // false to allow re-check
                _have_check_upload_status.store(false);
                add_ref();

                tasking::enqueue(
                    LPC_BACKGROUND_COLD_BACKUP,
                    nullptr,
                    [this]() {
                        if (!is_ready_for_upload()) {
                            LOG_INFO(
                                "{}: backup status has changed to {}, stop check upload status",
                                name,
                                cold_backup_status_to_string(status()));
                        } else {
                            upload_checkpoint_to_remote();
                        }
                        release_ref();
                    },
                    0,
                    std::chrono::seconds(10));
            } else {
                LOG_ERROR("{}: block service create file failed, file = {}, err = {}",
                          name,
                          metadata,
                          resp.err);
                _have_check_upload_status.store(false);
                fail_upload("block service create file failed");
            }
            release_ref();
            return;
        });
}

void cold_backup_context::read_backup_metadata(
    const dist::block_service::block_file_ptr &file_handle)
{
    dist::block_service::read_request req;
    req.remote_pos = 0;
    req.remote_length = -1;

    add_ref();

    file_handle->read(
        std::move(req),
        LPC_BACKGROUND_COLD_BACKUP,
        [this, file_handle](const dist::block_service::read_response &resp) {
            if (resp.err == ERR_OK) {
                LOG_INFO("{}: read cold_backup_metadata succeed, verify it's context, file = {}",
                         name,
                         file_handle->file_name());
                verify_backup_metadata(resp.buffer);
                on_upload_chkpt_dir();
            } else if (resp.err == ERR_TIMEOUT) {
                LOG_ERROR("{}: read remote file timeout, retry after 10s, file = {}",
                          name,
                          file_handle->file_name());
                add_ref();

                tasking::enqueue(
                    LPC_BACKGROUND_COLD_BACKUP,
                    nullptr,
                    [this, file_handle] {
                        if (!is_ready_for_upload()) {
                            LOG_INFO(
                                "{}: backup status has changed to {}, stop check upload status",
                                name,
                                cold_backup_status_to_string(status()));
                            _have_check_upload_status.store(false);
                        } else {
                            read_backup_metadata(file_handle);
                        }
                        release_ref();
                    },
                    0,
                    std::chrono::seconds(10));
            } else {
                LOG_ERROR("{}: read remote file failed, file = {}, err = {}",
                          name,
                          file_handle->file_name(),
                          resp.err);
                _have_check_upload_status.store(false);
                fail_upload("read remote file failed");
            }
            release_ref();
            return;
        });
}

void cold_backup_context::verify_backup_metadata(const blob &value)
{
    cold_backup_metadata tmp;
    if (value.length() > 0 && json::json_forwarder<cold_backup_metadata>::decode(value, tmp)) {
        LOG_INFO("{}: check upload status complete, checkpoint dir uploading has already complete",
                 name);
        _upload_status.store(UploadComplete);
    } else {
        LOG_INFO("{}: check upload status complete, checkpoint dir uploading isn't complete yet",
                 name);
        _upload_status.store(UploadUncomplete);
    }
}

void cold_backup_context::on_upload_chkpt_dir()
{
    if (_upload_status.load() == UploadInvalid || !is_ready_for_upload()) {
        LOG_INFO("{}: replica is not ready for uploading, ignore upload, cold_backup_status({})",
                 name,
                 cold_backup_status_to_string(status()));
        return;
    }

    if (_upload_status.load() == UploadComplete) {
        // TODO: if call upload_checkpint_to_remote multi times, maybe write_current_chkpt_file
        // multi times
        std::string chkpt_dirname = cold_backup::get_remote_chkpt_dirname();
        write_current_chkpt_file(chkpt_dirname);
        return;
    }

    prepare_upload();

    // prepare_upload maybe fail, so here check status
    if (!is_ready_for_upload()) {
        LOG_ERROR("{}: backup status has changed to {}, stop upload checkpoint dir",
                  name,
                  cold_backup_status_to_string(status()));
        return;
    }

    if (checkpoint_files.size() <= 0) {
        LOG_INFO("{}: checkpoint dir is empty, so upload is complete and just start write "
                 "backup_metadata",
                 name);
        bool old_status = false;
        // using atomic variant _have_write_backup_metadata is to allow one task to
        // write backup_metadata because on_upload_chkpt_dir maybe call multi-time
        if (_have_write_backup_metadata.compare_exchange_strong(old_status, true)) {
            write_backup_metadata();
        }
    } else {
        LOG_INFO("{}: start upload checkpoint dir, checkpoint dir = {}, total checkpoint file = {}",
                 name,
                 checkpoint_dir,
                 checkpoint_files.size());
        std::vector<std::string> files;
        if (!upload_complete_or_fetch_uncomplete_files(files)) {
            for (auto &file : files) {
                LOG_INFO("{}: start upload checkpoint file to remote, file = {}", name, file);
                upload_file(file);
            }
        } else {
            LOG_INFO("{}: upload checkpoint dir to remote complete, total_file_cnt = {}",
                     name,
                     checkpoint_files.size());
            bool old_status = false;
            if (_have_write_backup_metadata.compare_exchange_strong(old_status, true)) {
                write_backup_metadata();
            }
        }
    }
}

void cold_backup_context::prepare_upload()
{
    zauto_lock l(_lock);
    // only need initialize once
    if (_metadata.files.size() > 0) {
        return;
    }
    _file_remain_cnt = checkpoint_files.size();

    _metadata.checkpoint_decree = checkpoint_decree;
    _metadata.checkpoint_timestamp = checkpoint_timestamp;
    _metadata.checkpoint_total_size = checkpoint_file_total_size;
    for (int32_t idx = 0; idx < checkpoint_files.size(); idx++) {
        std::string &file = checkpoint_files[idx];
        file_meta f_meta;
        f_meta.name = file;
        std::string file_full_path = ::dsn::utils::filesystem::path_combine(checkpoint_dir, file);
        int64_t file_size = checkpoint_file_sizes[idx];
        std::string file_md5;
        if (::dsn::utils::filesystem::md5sum(file_full_path, file_md5) != ERR_OK) {
            LOG_ERROR("{}: get local file size or md5 fail, file = {}", name, file_full_path);
            fail_upload("compute local file size or md5 failed");
            return;
        }
        f_meta.md5 = file_md5;
        f_meta.size = file_size;
        _metadata.files.emplace_back(f_meta);
        _file_status.insert(std::make_pair(file, FileUploadUncomplete));
        _file_infos.insert(std::make_pair(file, std::make_pair(file_size, file_md5)));
    }
    _upload_file_size.store(0);
}

void cold_backup_context::upload_file(const std::string &local_filename)
{
    std::string remote_chkpt_dir = cold_backup::get_remote_chkpt_dir(
        backup_root, request.app_name, request.pid, request.backup_id);
    dist::block_service::create_file_request req;
    req.file_name = ::dsn::utils::filesystem::path_combine(remote_chkpt_dir, local_filename);
    req.ignore_metadata = false;

    add_ref();

    block_service->create_file(
        std::move(req),
        LPC_BACKGROUND_COLD_BACKUP,
        [this, local_filename](const dist::block_service::create_file_response &resp) {
            if (resp.err == ERR_OK) {
                const dist::block_service::block_file_ptr &file_handle = resp.file_handle;
                CHECK_NOTNULL(file_handle, "");
                int64_t local_file_size = _file_infos.at(local_filename).first;
                std::string md5 = _file_infos.at(local_filename).second;
                std::string full_path_local_file =
                    ::dsn::utils::filesystem::path_combine(checkpoint_dir, local_filename);
                if (md5 == file_handle->get_md5sum() &&
                    local_file_size == file_handle->get_size()) {
                    LOG_INFO("{}: checkpoint file already exist on remote, file = {}",
                             name,
                             full_path_local_file);
                    on_upload_file_complete(local_filename);
                } else {
                    LOG_INFO("{}: start upload checkpoint file to remote, file = {}",
                             name,
                             full_path_local_file);
                    on_upload(file_handle, full_path_local_file);
                }
            } else if (resp.err == ERR_TIMEOUT) {
                LOG_ERROR("{}: block service create file timeout, retry after 10s, file = {}",
                          name,
                          local_filename);
                add_ref();

                tasking::enqueue(LPC_BACKGROUND_COLD_BACKUP,
                                 nullptr,
                                 [this, local_filename]() {
                                     // TODO: status change from ColdBackupUploading to
                                     // ColdBackupPaused, and upload file timeout, but when callback
                                     // is executed it catches the status(ColdBackupPaused)
                                     // now, if status back to ColdBackupUploading very soon, and
                                     // call upload_checkpoint_to_remote() here,
                                     // upload_checkpoint_to_remote() maybe acquire the _lock first,
                                     // then stop give back file(upload timeout), the file is still
                                     // in uploading this file will not be uploaded until you call
                                     // upload_checkpoint_to_remote() after it's given back
                                     if (!is_ready_for_upload()) {
                                         std::string full_path_local_file =
                                             ::dsn::utils::filesystem::path_combine(checkpoint_dir,
                                                                                    local_filename);
                                         LOG_INFO("{}: backup status has changed to {}, stop "
                                                  "upload checkpoint file to remote, file = {}",
                                                  name,
                                                  cold_backup_status_to_string(status()),
                                                  full_path_local_file);
                                         file_upload_uncomplete(local_filename);
                                     } else {
                                         upload_file(local_filename);
                                     }
                                     release_ref();
                                 },
                                 0,
                                 std::chrono::seconds(10));
            } else {
                LOG_ERROR("{}: block service create file failed, file = {}, err = {}",
                          name,
                          local_filename,
                          resp.err);
                fail_upload("create file failed");
            }
            if (resp.err != ERR_OK && _owner_replica != nullptr) {
                _owner_replica->get_replica_stub()
                    ->_counter_cold_backup_recent_upload_file_fail_count->increment();
            }
            release_ref();
            return;
        });
}

void cold_backup_context::on_upload(const dist::block_service::block_file_ptr &file_handle,
                                    const std::string &full_path_local_file)
{
    dist::block_service::upload_request req;
    req.input_local_name = full_path_local_file;

    add_ref();

    file_handle->upload(
        std::move(req),
        LPC_BACKGROUND_COLD_BACKUP,
        [this, file_handle, full_path_local_file](
            const dist::block_service::upload_response &resp) {
            if (resp.err == ERR_OK) {
                std::string local_filename =
                    ::dsn::utils::filesystem::get_file_name(full_path_local_file);
                CHECK_EQ(_file_infos.at(local_filename).first,
                         static_cast<int64_t>(resp.uploaded_size));
                LOG_INFO(
                    "{}: upload checkpoint file complete, file = {}", name, full_path_local_file);
                on_upload_file_complete(local_filename);
            } else if (resp.err == ERR_TIMEOUT) {
                LOG_ERROR("{}: upload checkpoint file timeout, retry after 10s, file = {}",
                          name,
                          full_path_local_file);
                add_ref();

                tasking::enqueue(
                    LPC_BACKGROUND_COLD_BACKUP,
                    nullptr,
                    [this, file_handle, full_path_local_file]() {
                        if (!is_ready_for_upload()) {
                            LOG_ERROR("{}: backup status has changed to {}, stop upload "
                                      "checkpoint file to remote, file = {}",
                                      name,
                                      cold_backup_status_to_string(status()),
                                      full_path_local_file);
                            std::string local_filename =
                                ::dsn::utils::filesystem::get_file_name(full_path_local_file);
                            file_upload_uncomplete(local_filename);
                        } else {
                            on_upload(file_handle, full_path_local_file);
                        }
                        release_ref();
                    },
                    0,
                    std::chrono::seconds(10));
            } else {
                LOG_ERROR("{}: upload checkpoint file to remote failed, file = {}, err = {}",
                          name,
                          full_path_local_file,
                          resp.err);
                fail_upload("upload checkpoint file to remote failed");
            }
            if (resp.err != ERR_OK && _owner_replica != nullptr) {
                _owner_replica->get_replica_stub()
                    ->_counter_cold_backup_recent_upload_file_fail_count->increment();
            }
            release_ref();
            return;
        });
}

void cold_backup_context::write_backup_metadata()
{
    if (_upload_status.load() == UploadComplete) {
        LOG_INFO("{}: upload have already done, no need write metadata again", name);
        return;
    }
    std::string metadata = cold_backup::get_remote_chkpt_meta_file(
        backup_root, request.app_name, request.pid, request.backup_id);
    dist::block_service::create_file_request req;
    req.file_name = metadata;
    req.ignore_metadata = true;

    add_ref();

    block_service->create_file(
        std::move(req),
        LPC_BACKGROUND_COLD_BACKUP,
        [this, metadata](const dist::block_service::create_file_response &resp) {
            if (resp.err == ERR_OK) {
                CHECK_NOTNULL(resp.file_handle, "");
                blob buffer = json::json_forwarder<cold_backup_metadata>::encode(_metadata);
                // hold itself until callback is executed
                add_ref();
                LOG_INFO("{}: create backup metadata file succeed, start to write file, file = {}",
                         name,
                         metadata);
                this->on_write(resp.file_handle, buffer, [this](bool succeed) {
                    if (succeed) {
                        std::string chkpt_dirname = cold_backup::get_remote_chkpt_dirname();
                        _upload_status.store(UploadComplete);
                        LOG_INFO(
                            "{}: write backup metadata complete, write current checkpoint file",
                            name);
                        write_current_chkpt_file(chkpt_dirname);
                    }
                    // NOTICE: write file fail will internal error be processed in on_write()
                    release_ref();
                });
            } else if (resp.err == ERR_TIMEOUT) {
                LOG_ERROR("{}: block service create file timeout, retry after 10s, file = {}",
                          name,
                          metadata);
                add_ref();

                tasking::enqueue(
                    LPC_BACKGROUND_COLD_BACKUP,
                    nullptr,
                    [this]() {
                        if (!is_ready_for_upload()) {
                            _have_write_backup_metadata.store(false);
                            LOG_ERROR(
                                "{}: backup status has changed to {}, stop write backup_metadata",
                                name,
                                cold_backup_status_to_string(status()));
                        } else {
                            write_backup_metadata();
                        }
                        release_ref();
                    },
                    0,
                    std::chrono::seconds(10));
            } else {
                LOG_ERROR("{}: block service create file failed, file = {}, err = {}",
                          name,
                          metadata,
                          resp.err);
                _have_write_backup_metadata.store(false);
                fail_upload("create file failed");
            }
            release_ref();
            return;
        });
}

void cold_backup_context::write_current_chkpt_file(const std::string &value)
{
    // before we write current checkpoint file, we can release the memory occupied by _metadata,
    // _file_status and _file_infos, because even if write current checkpoint file failed, the
    // backup_metadata is uploading succeed, so we will not re-upload
    _metadata.files.clear();
    _file_infos.clear();
    _file_status.clear();

    if (!is_ready_for_upload()) {
        LOG_INFO("{}: backup status has changed to {}, stop write current checkpoint file",
                 name,
                 cold_backup_status_to_string(status()));
        return;
    }

    std::string current_chkpt_file = cold_backup::get_current_chkpt_file(
        backup_root, request.app_name, request.pid, request.backup_id);
    dist::block_service::create_file_request req;
    req.file_name = current_chkpt_file;
    req.ignore_metadata = true;

    add_ref();

    block_service->create_file(
        std::move(req),
        LPC_BACKGROUND_COLD_BACKUP,
        [this, value, current_chkpt_file](const dist::block_service::create_file_response &resp) {
            if (resp.err == ERR_OK) {
                CHECK_NOTNULL(resp.file_handle, "");
                auto len = value.length();
                std::shared_ptr<char> buf = utils::make_shared_array<char>(len);
                ::memcpy(buf.get(), value.c_str(), len);
                blob write_buf(std::move(buf), static_cast<unsigned int>(len));
                LOG_INFO("{}: create current checkpoint file succeed, start write file ,file = {}",
                         name,
                         current_chkpt_file);
                add_ref();
                this->on_write(resp.file_handle, write_buf, [this](bool succeed) {
                    if (succeed) {
                        complete_upload();
                    }
                    release_ref();
                });
            } else if (resp.err == ERR_TIMEOUT) {
                LOG_ERROR("{}: block file create file timeout, retry after 10s, file = {}",
                          name,
                          current_chkpt_file);
                add_ref();

                tasking::enqueue(LPC_BACKGROUND_COLD_BACKUP,
                                 nullptr,
                                 [this, value]() {
                                     if (!is_ready_for_upload()) {
                                         LOG_INFO("{}: backup status has changed to {}, stop write "
                                                  "current checkpoint file",
                                                  name,
                                                  cold_backup_status_to_string(status()));
                                     } else {
                                         write_current_chkpt_file(value);
                                     }

                                     release_ref();
                                 },
                                 0,
                                 std::chrono::seconds(10));
            } else {
                LOG_ERROR("{}: block service create file failed, file = {}, err = {}",
                          name,
                          current_chkpt_file,
                          resp.err);
                fail_upload("create file failed");
            }
            release_ref();
            return;
        });
}

void cold_backup_context::on_write(const dist::block_service::block_file_ptr &file_handle,
                                   const blob &value,
                                   const std::function<void(bool)> &callback)
{
    CHECK_NOTNULL(file_handle, "");
    dist::block_service::write_request req;
    req.buffer = value;

    add_ref();

    file_handle->write(
        std::move(req),
        LPC_BACKGROUND_COLD_BACKUP,
        [this, value, file_handle, callback](const dist::block_service::write_response &resp) {
            if (resp.err == ERR_OK) {
                LOG_INFO(
                    "{}: write remote file succeed, file = {}", name, file_handle->file_name());
                callback(true);
            } else if (resp.err == ERR_TIMEOUT) {
                LOG_INFO("{}: write remote file timeout, retry after 10s, file = {}",
                         name,
                         file_handle->file_name());
                add_ref();

                tasking::enqueue(LPC_BACKGROUND_COLD_BACKUP,
                                 nullptr,
                                 [this, file_handle, value, callback]() {
                                     if (!is_ready_for_upload()) {
                                         LOG_INFO("{}: backup status has changed to {}, stop write "
                                                  "remote file, file = {}",
                                                  name,
                                                  cold_backup_status_to_string(status()),
                                                  file_handle->file_name());
                                     } else {
                                         on_write(file_handle, value, callback);
                                     }
                                     release_ref();
                                 },
                                 0,
                                 std::chrono::seconds(10));
            } else {
                // here, must call the callback to release_ref
                callback(false);
                LOG_ERROR("{}: write remote file failed, file = {}, err = {}",
                          name,
                          file_handle->file_name(),
                          resp.err);
                fail_upload("write remote file failed");
            }
            release_ref();
            return;
        });
}

void cold_backup_context::on_upload_file_complete(const std::string &local_filename)
{
    const int64_t &f_size = _file_infos.at(local_filename).first;
    _upload_file_size.fetch_add(f_size);
    file_upload_complete(local_filename);
    if (_owner_replica != nullptr) {
        _owner_replica->get_replica_stub()
            ->_counter_cold_backup_recent_upload_file_succ_count->increment();
        _owner_replica->get_replica_stub()->_counter_cold_backup_recent_upload_file_size->add(
            f_size);
    }
    // update progress
    // int a = 10; int b = 3; then  b/a = 0;
    // double a = 10; double b = 3; then b/a = 0.3
    auto total = static_cast<double>(checkpoint_file_total_size);
    auto complete_size = static_cast<double>(_upload_file_size.load());

    if (total <= complete_size) {
        LOG_INFO("{}: upload checkpoint to remote complete, checkpoint dir = {}, total file size "
                 "= {}, file count = {}",
                 name,
                 checkpoint_dir,
                 total,
                 checkpoint_files.size());
        bool old_status = false;
        if (_have_write_backup_metadata.compare_exchange_strong(old_status, true)) {
            write_backup_metadata();
        }
        return;
    } else {
        CHECK_GT(total, 0.0);
        update_progress(static_cast<int>(complete_size / total * 1000));
        LOG_INFO("{}: the progress of upload checkpoint is {}", name, _progress.load());
    }
    if (is_ready_for_upload()) {
        std::vector<std::string> upload_files;
        upload_complete_or_fetch_uncomplete_files(upload_files);
        for (auto &file : upload_files) {
            LOG_INFO("{}: start upload checkpoint file to remote, file = {}", name, file);
            upload_file(file);
        }
    }
}

bool cold_backup_context::upload_complete_or_fetch_uncomplete_files(std::vector<std::string> &files)
{
    bool upload_complete = false;

    zauto_lock l(_lock);
    if (_file_remain_cnt > 0 && _cur_upload_file_cnt < _max_concurrent_uploading_file_cnt) {
        for (const auto &_pair : _file_status) {
            if (_pair.second == file_status::FileUploadUncomplete) {
                files.emplace_back(_pair.first);
                _file_remain_cnt -= 1;
                _file_status[_pair.first] = file_status::FileUploading;
                _cur_upload_file_cnt += 1;
            }
            if (_file_remain_cnt <= 0 ||
                _cur_upload_file_cnt >= _max_concurrent_uploading_file_cnt) {
                break;
            }
        }
    }
    if (_file_remain_cnt <= 0 && _cur_upload_file_cnt <= 0) {
        upload_complete = true;
    }
    return upload_complete;
}

void cold_backup_context::file_upload_uncomplete(const std::string &filename)
{
    zauto_lock l(_lock);

    CHECK_GE(_cur_upload_file_cnt, 1);
    _cur_upload_file_cnt -= 1;
    _file_remain_cnt += 1;
    _file_status[filename] = file_status::FileUploadUncomplete;
}

void cold_backup_context::file_upload_complete(const std::string &filename)
{
    zauto_lock l(_lock);

    CHECK_GE(_cur_upload_file_cnt, 1);
    _cur_upload_file_cnt -= 1;
    _file_status[filename] = file_status::FileUploadComplete;
}

} // namespace replication
} // namespace dsn
