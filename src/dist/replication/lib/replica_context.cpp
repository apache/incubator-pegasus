/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015 Microsoft Corporation
 *
 * -=- Robust Distributed System Nucleus (rDSN) -=-
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

/*
 * Description:
 *     context for replica with different roles
 *
 * Revision history:
 *     Mar., 2015, @imzhenyu (Zhenyu Guo), first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

#include <dsn/utility/filesystem.h>
#include <dsn/utility/utils.h>

#include "replica_context.h"
#include "replica.h"
#include "replica_stub.h"
#include "mutation.h"
#include "mutation_log.h"
#include "dist/block_service/block_service_manager.h"

namespace dsn {
namespace replication {

void primary_context::cleanup(bool clean_pending_mutations)
{
    do_cleanup_pending_mutations(clean_pending_mutations);

    // clean up group check
    CLEANUP_TASK_ALWAYS(group_check_task)

    for (auto it = group_check_pending_replies.begin(); it != group_check_pending_replies.end();
         ++it) {
        CLEANUP_TASK_ALWAYS(it->second)
        // it->second->cancel(true);
    }

    group_check_pending_replies.clear();

    // clean up reconfiguration
    CLEANUP_TASK_ALWAYS(reconfiguration_task)

    // clean up checkpoint
    CLEANUP_TASK_ALWAYS(checkpoint_task)

    // clean up register child task
    CLEANUP_TASK_ALWAYS(register_child_task)

    // cleanup group bulk load
    for (auto &kv : group_bulk_load_pending_replies) {
        CLEANUP_TASK_ALWAYS(kv.second);
    }
    group_bulk_load_pending_replies.clear();

    membership.ballot = 0;

    caught_up_children.clear();

    sync_send_write_request = false;

    cleanup_bulk_load_states();
}

bool primary_context::is_cleaned()
{
    return nullptr == group_check_task && nullptr == reconfiguration_task &&
           nullptr == checkpoint_task && group_check_pending_replies.empty() &&
           nullptr == register_child_task && group_bulk_load_pending_replies.empty();
}

void primary_context::do_cleanup_pending_mutations(bool clean_pending_mutations)
{
    if (clean_pending_mutations) {
        write_queue.clear();
    }
}

void primary_context::reset_membership(const partition_configuration &config, bool clear_learners)
{
    statuses.clear();
    if (clear_learners) {
        learners.clear();
    }

    if (config.ballot > membership.ballot)
        next_learning_version = (((uint64_t)config.ballot) << 32) + 1;
    else
        ++next_learning_version;

    membership = config;

    if (membership.primary.is_invalid() == false) {
        statuses[membership.primary] = partition_status::PS_PRIMARY;
    }

    for (auto it = config.secondaries.begin(); it != config.secondaries.end(); ++it) {
        statuses[*it] = partition_status::PS_SECONDARY;
        learners.erase(*it);
    }

    for (auto it = learners.begin(); it != learners.end(); ++it) {
        statuses[it->first] = partition_status::PS_POTENTIAL_SECONDARY;
    }
}

void primary_context::get_replica_config(partition_status::type st,
                                         /*out*/ replica_configuration &config,
                                         uint64_t learner_signature /*= invalid_signature*/)
{
    config.pid = membership.pid;
    config.primary = membership.primary;
    config.ballot = membership.ballot;
    config.status = st;
    config.learner_signature = learner_signature;
}

bool primary_context::check_exist(::dsn::rpc_address node, partition_status::type st)
{
    switch (st) {
    case partition_status::PS_PRIMARY:
        return membership.primary == node;
    case partition_status::PS_SECONDARY:
        return std::find(membership.secondaries.begin(), membership.secondaries.end(), node) !=
               membership.secondaries.end();
    case partition_status::PS_POTENTIAL_SECONDARY:
        return learners.find(node) != learners.end();
    default:
        dassert(false, "invalid partition_status, status = %s", enum_to_string(st));
        return false;
    }
}

void primary_context::cleanup_bulk_load_states()
{
    secondary_bulk_load_states.erase(secondary_bulk_load_states.begin(),
                                     secondary_bulk_load_states.end());
    ingestion_is_empty_prepare_sent = false;
}

bool secondary_context::cleanup(bool force)
{
    CLEANUP_TASK(checkpoint_task, force)

    if (!force) {
        CLEANUP_TASK_ALWAYS(checkpoint_completed_task);
    } else {
        CLEANUP_TASK(checkpoint_completed_task, force)
    }

    CLEANUP_TASK(catchup_with_private_log_task, force)

    checkpoint_is_running = false;
    return true;
}

bool secondary_context::is_cleaned() { return checkpoint_is_running == false; }

bool potential_secondary_context::cleanup(bool force)
{
    task_ptr t = nullptr;

    if (!force) {
        CLEANUP_TASK_ALWAYS(delay_learning_task)

        CLEANUP_TASK_ALWAYS(learning_task)

        CLEANUP_TASK_ALWAYS(learn_remote_files_completed_task)

        CLEANUP_TASK_ALWAYS(completion_notify_task)
    } else {
        CLEANUP_TASK(delay_learning_task, true)

        CLEANUP_TASK(learning_task, true)

        CLEANUP_TASK(learn_remote_files_completed_task, true)

        CLEANUP_TASK(completion_notify_task, true)
    }

    CLEANUP_TASK(learn_remote_files_task, force)

    CLEANUP_TASK(catchup_with_private_log_task, force)

    learning_version = 0;
    learning_start_ts_ns = 0;
    learning_copy_file_count = 0;
    learning_copy_file_size = 0;
    learning_copy_buffer_size = 0;
    learning_round_is_running = false;
    if (learn_app_concurrent_count_increased) {
        --owner_replica->get_replica_stub()->_learn_app_concurrent_count;
        learn_app_concurrent_count_increased = false;
    }
    learning_start_prepare_decree = invalid_decree;
    first_learn_start_decree = invalid_decree;
    learning_status = learner_status::LearningInvalid;
    return true;
}

bool potential_secondary_context::is_cleaned()
{
    return nullptr == delay_learning_task && nullptr == learning_task &&
           nullptr == learn_remote_files_task && nullptr == learn_remote_files_completed_task &&
           nullptr == catchup_with_private_log_task && nullptr == completion_notify_task;
}

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
        dassert(false, "");
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

bool cold_backup_context::pause_upload()
{
    int uploading = ColdBackupUploading;
    if (_status.compare_exchange_strong(uploading, ColdBackupPaused)) {
        if (_owner_replica != nullptr) {
            _owner_replica->get_replica_stub()
                ->_counter_cold_backup_recent_pause_count->increment();
        }
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
        backup_root, request.policy.policy_name, request.app_name, request.pid, request.backup_id);
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
                ddebug("%s: backup status has changed to %s, ignore checking backup on remote",
                       name,
                       cold_backup_status_to_string(status()));
                ignore_check();
            } else if (resp.err == ERR_OK) {
                const dist::block_service::block_file_ptr &file_handle = resp.file_handle;
                dassert(file_handle != nullptr, "");
                if (file_handle->get_md5sum().empty() && file_handle->get_size() <= 0) {
                    ddebug("%s: check backup on remote, current_checkpoint file %s is not exist",
                           name,
                           current_chkpt_file.c_str());
                    complete_check(false);
                } else {
                    ddebug("%s: check backup on remote, current_checkpoint file %s is exist",
                           name,
                           current_chkpt_file.c_str());
                    read_current_chkpt_file(file_handle);
                }
            } else if (resp.err == ERR_TIMEOUT) {
                derror("%s: block service create file timeout, retry after 10 seconds, file = %s",
                       name,
                       current_chkpt_file.c_str());

                // before retry, should add_ref(), and must release_ref() after retry
                add_ref();

                tasking::enqueue(LPC_BACKGROUND_COLD_BACKUP,
                                 nullptr,
                                 [this]() {
                                     // before retry, should check whether the status is ready for
                                     // check
                                     if (!is_ready_for_check()) {
                                         ddebug("%s: backup status has changed to %s, ignore "
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
                derror("%s: block service create file failed, file = %s, err = %s",
                       name,
                       current_chkpt_file.c_str(),
                       resp.err.to_string());
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
                ddebug("%s: backup status has changed to %s, ignore checking backup on remote",
                       name,
                       cold_backup_status_to_string(status()));
                ignore_check();
            } else if (resp.err == ERR_OK) {
                std::string chkpt_dirname(resp.buffer.data(), resp.buffer.length());
                if (chkpt_dirname.empty()) {
                    complete_check(false);
                } else {
                    ddebug("%s: after read current_checkpoint_file, check whether remote "
                           "checkpoint dir = %s is exist",
                           name,
                           chkpt_dirname.c_str());
                    remote_chkpt_dir_exist(chkpt_dirname);
                }
            } else if (resp.err == ERR_TIMEOUT) {
                derror("%s: read remote file timeout, retry after 10s, file = %s",
                       name,
                       file_handle->file_name().c_str());
                add_ref();

                tasking::enqueue(LPC_BACKGROUND_COLD_BACKUP,
                                 nullptr,
                                 [this, file_handle]() {
                                     if (!is_ready_for_check()) {
                                         ddebug("%s: backup status has changed to %s, ignore "
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
                derror("%s: read remote file failed, file = %s, err = %s",
                       name,
                       file_handle->file_name().c_str(),
                       resp.err.to_string());
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
        backup_root, request.policy.policy_name, request.app_name, request.pid, request.backup_id);

    add_ref();

    block_service->list_dir(
        std::move(req),
        LPC_BACKGROUND_COLD_BACKUP,
        [this, chkpt_dirname](const dist::block_service::ls_response &resp) {
            if (!is_ready_for_check()) {
                ddebug("%s: backup status has changed to %s, ignore checking backup on remote",
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
                    ddebug("%s: remote checkpoint dir is already exist, so upload have already "
                           "complete, remote_checkpoint_dirname = %s",
                           name,
                           chkpt_dirname.c_str());
                    complete_check(true);
                } else {
                    ddebug("%s: remote checkpoint dir is not exist, should re-upload checkpoint "
                           "dir, remote_checkpoint_dirname = %s",
                           name,
                           chkpt_dirname.c_str());
                    complete_check(false);
                }
            } else if (resp.err == ERR_OBJECT_NOT_FOUND) {
                ddebug("%s: remote checkpoint dir is not exist, should re-upload checkpoint dir, "
                       "remote_checkpoint_dirname = %s",
                       name,
                       chkpt_dirname.c_str());
                complete_check(false);
            } else if (resp.err == ERR_TIMEOUT) {
                derror("%s: block service list remote dir timeout, retry after 10s, dirname = %s",
                       name,
                       chkpt_dirname.c_str());
                add_ref();

                tasking::enqueue(LPC_BACKGROUND_COLD_BACKUP,
                                 nullptr,
                                 [this, chkpt_dirname]() {
                                     if (!is_ready_for_check()) {
                                         ddebug("%s: backup status has changed to %s, ignore "
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
                derror("%s: block service list remote dir failed, dirname = %s, err = %s",
                       name,
                       chkpt_dirname.c_str(),
                       resp.err.to_string());
                fail_check("list remote dir failed");
            }
            release_ref();
            return;
        });
}

void cold_backup_context::upload_checkpoint_to_remote()
{
    if (!is_ready_for_upload()) {
        ddebug("%s: backup status has changed to %s, ignore upload checkpoint",
               name,
               cold_backup_status_to_string(status()));
        return;
    }

    bool old_status = false;
    // here, just allow one task to check upload status, and it will set _upload_status base on
    // the result it has checked; But, because of upload_checkpoint_to_remote maybe call multi-times
    // (for pause - uploading), so we use the atomic variant to implement
    if (!_have_check_upload_status.compare_exchange_strong(old_status, true)) {
        ddebug("%s: upload status has already been checked, start upload checkpoint dir directly",
               name);
        on_upload_chkpt_dir();
        return;
    }

    // check whether cold_backup_metadata is exist and verify cold_backup_metadata if exist
    std::string metadata = cold_backup::get_remote_chkpt_meta_file(
        backup_root, request.policy.policy_name, request.app_name, request.pid, request.backup_id);
    dist::block_service::create_file_request req;
    req.file_name = metadata;
    req.ignore_metadata = false;

    add_ref();

    block_service->create_file(
        std::move(req),
        LPC_BACKGROUND_COLD_BACKUP,
        [this, metadata](const dist::block_service::create_file_response &resp) {
            if (resp.err == ERR_OK) {
                dassert(resp.file_handle != nullptr, "");
                if (resp.file_handle->get_md5sum().empty() && resp.file_handle->get_size() <= 0) {
                    _upload_status.store(UploadUncomplete);
                    ddebug("%s: check upload_status complete, cold_backup_metadata isn't exist, "
                           "start upload checkpoint dir",
                           name);
                    on_upload_chkpt_dir();
                } else {
                    ddebug("%s: cold_backup_metadata is exist, read it's context", name);
                    read_backup_metadata(resp.file_handle);
                }
            } else if (resp.err == ERR_TIMEOUT) {
                derror("%s: block service create file timeout, retry after 10s, file = %s",
                       name,
                       metadata.c_str());
                // when create backup_metadata timeout, should reset _have_check_upload_status
                // false to allow re-check
                _have_check_upload_status.store(false);
                add_ref();

                tasking::enqueue(
                    LPC_BACKGROUND_COLD_BACKUP,
                    nullptr,
                    [this]() {
                        if (!is_ready_for_upload()) {
                            ddebug("%s: backup status has changed to %s, stop check upload status",
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
                derror("%s: block service create file failed, file = %s, err = %s",
                       name,
                       metadata.c_str(),
                       resp.err.to_string());
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
                ddebug("%s: read cold_backup_metadata succeed, verify it's context, file = %s",
                       name,
                       file_handle->file_name().c_str());
                verify_backup_metadata(resp.buffer);
                on_upload_chkpt_dir();
            } else if (resp.err == ERR_TIMEOUT) {
                derror("%s: read remote file timeout, retry after 10s, file = %s",
                       name,
                       file_handle->file_name().c_str());
                add_ref();

                tasking::enqueue(
                    LPC_BACKGROUND_COLD_BACKUP,
                    nullptr,
                    [this, file_handle] {
                        if (!is_ready_for_upload()) {
                            ddebug("%s: backup status has changed to %s, stop check upload status",
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
                derror("%s: read remote file failed, file = %s, err = %s",
                       name,
                       file_handle->file_name().c_str(),
                       resp.err.to_string());
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
        ddebug("%s: check upload status complete, checkpoint dir uploading has already complete",
               name);
        _upload_status.store(UploadComplete);
    } else {
        ddebug("%s: check upload status complete, checkpoint dir uploading isn't complete yet",
               name);
        _upload_status.store(UploadUncomplete);
    }
}

void cold_backup_context::on_upload_chkpt_dir()
{
    if (_upload_status.load() == UploadInvalid || !is_ready_for_upload()) {
        ddebug("%s: replica is not ready for uploading, ignore upload, cold_backup_status(%s)",
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
        derror("%s: backup status has changed to %s, stop upload checkpoint dir",
               name,
               cold_backup_status_to_string(status()));
        return;
    }

    if (checkpoint_files.size() <= 0) {
        ddebug("%s: checkpoint dir is empty, so upload is complete and just start write "
               "backup_metadata",
               name);
        bool old_status = false;
        // using atomic variant _have_write_backup_metadata is to allow one task to
        // write backup_metadata because on_upload_chkpt_dir maybe call multi-time
        if (_have_write_backup_metadata.compare_exchange_strong(old_status, true)) {
            write_backup_metadata();
        }
    } else {
        ddebug("%s: start upload checkpoint dir, checkpoint dir = %s, total checkpoint file = %d",
               name,
               checkpoint_dir.c_str(),
               checkpoint_files.size());
        std::vector<std::string> files;
        if (!upload_complete_or_fetch_uncomplete_files(files)) {
            for (auto &file : files) {
                ddebug("%s: start upload checkpoint file to remote, file = %s", name, file.c_str());
                upload_file(file);
            }
        } else {
            ddebug("%s: upload checkpoint dir to remote complete, total_file_cnt = %d",
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
            derror("%s: get local file size or md5 fail, file = %s", name, file_full_path.c_str());
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
        backup_root, request.policy.policy_name, request.app_name, request.pid, request.backup_id);
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
                dassert(file_handle != nullptr, "");
                int64_t local_file_size = _file_infos.at(local_filename).first;
                std::string md5 = _file_infos.at(local_filename).second;
                std::string full_path_local_file =
                    ::dsn::utils::filesystem::path_combine(checkpoint_dir, local_filename);
                if (md5 == file_handle->get_md5sum() &&
                    local_file_size == file_handle->get_size()) {
                    ddebug("%s: checkpoint file already exist on remote, file = %s",
                           name,
                           full_path_local_file.c_str());
                    on_upload_file_complete(local_filename);
                } else {
                    ddebug("%s: start upload checkpoint file to remote, file = %s",
                           name,
                           full_path_local_file.c_str());
                    on_upload(file_handle, full_path_local_file);
                }
            } else if (resp.err == ERR_TIMEOUT) {
                derror("%s: block service create file timeout, retry after 10s, file = %s",
                       name,
                       local_filename.c_str());
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
                                         ddebug("%s: backup status has changed to %s, stop upload "
                                                "checkpoint file to remote, file = %s",
                                                name,
                                                cold_backup_status_to_string(status()),
                                                full_path_local_file.c_str());
                                         file_upload_uncomplete(local_filename);
                                     } else {
                                         upload_file(local_filename);
                                     }
                                     release_ref();
                                 },
                                 0,
                                 std::chrono::seconds(10));
            } else {
                derror("%s: block service create file failed, file = %s, err = %s",
                       name,
                       local_filename.c_str(),
                       resp.err.to_string());
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
                dassert(_file_infos.at(local_filename).first ==
                            static_cast<int64_t>(resp.uploaded_size),
                        "");
                ddebug("%s: upload checkpoint file complete, file = %s",
                       name,
                       full_path_local_file.c_str());
                on_upload_file_complete(local_filename);
            } else if (resp.err == ERR_TIMEOUT) {
                derror("%s: upload checkpoint file timeout, retry after 10s, file = %s",
                       name,
                       full_path_local_file.c_str());
                add_ref();

                tasking::enqueue(LPC_BACKGROUND_COLD_BACKUP,
                                 nullptr,
                                 [this, file_handle, full_path_local_file]() {
                                     if (!is_ready_for_upload()) {
                                         derror("%s: backup status has changed to %s, stop upload "
                                                "checkpoint file to remote, file = %s",
                                                name,
                                                cold_backup_status_to_string(status()),
                                                full_path_local_file.c_str());
                                         std::string local_filename =
                                             ::dsn::utils::filesystem::get_file_name(
                                                 full_path_local_file);
                                         file_upload_uncomplete(local_filename);
                                     } else {
                                         on_upload(file_handle, full_path_local_file);
                                     }
                                     release_ref();
                                 },
                                 0,
                                 std::chrono::seconds(10));
            } else {
                derror("%s: upload checkpoint file to remote failed, file = %s, err = %s",
                       name,
                       full_path_local_file.c_str(),
                       resp.err.to_string());
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
        ddebug("%s: upload have already done, no need write metadata again", name);
        return;
    }
    std::string metadata = cold_backup::get_remote_chkpt_meta_file(
        backup_root, request.policy.policy_name, request.app_name, request.pid, request.backup_id);
    dist::block_service::create_file_request req;
    req.file_name = metadata;
    req.ignore_metadata = true;

    add_ref();

    block_service->create_file(
        std::move(req),
        LPC_BACKGROUND_COLD_BACKUP,
        [this, metadata](const dist::block_service::create_file_response &resp) {
            if (resp.err == ERR_OK) {
                dassert(resp.file_handle != nullptr, "");
                blob buffer = json::json_forwarder<cold_backup_metadata>::encode(_metadata);
                // hold itself until callback is executed
                add_ref();
                ddebug("%s: create backup metadata file succeed, start to write file, file = %s",
                       name,
                       metadata.c_str());
                this->on_write(resp.file_handle, buffer, [this](bool succeed) {
                    if (succeed) {
                        std::string chkpt_dirname = cold_backup::get_remote_chkpt_dirname();
                        _upload_status.store(UploadComplete);
                        ddebug("%s: write backup metadata complete, write current checkpoint file",
                               name);
                        write_current_chkpt_file(chkpt_dirname);
                    }
                    // NOTICE: write file fail will internal error be processed in on_write()
                    release_ref();
                });
            } else if (resp.err == ERR_TIMEOUT) {
                derror("%s: block service create file timeout, retry after 10s, file = %s",
                       name,
                       metadata.c_str());
                add_ref();

                tasking::enqueue(
                    LPC_BACKGROUND_COLD_BACKUP,
                    nullptr,
                    [this]() {
                        if (!is_ready_for_upload()) {
                            _have_write_backup_metadata.store(false);
                            derror(
                                "%s: backup status has changed to %s, stop write backup_metadata",
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
                derror("%s: block service create file failed, file = %s, err = %s",
                       name,
                       metadata.c_str(),
                       resp.err.to_string());
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
        ddebug("%s: backup status has changed to %s, stop write current checkpoint file",
               name,
               cold_backup_status_to_string(status()));
        return;
    }

    std::string current_chkpt_file = cold_backup::get_current_chkpt_file(
        backup_root, request.policy.policy_name, request.app_name, request.pid, request.backup_id);
    dist::block_service::create_file_request req;
    req.file_name = current_chkpt_file;
    req.ignore_metadata = true;

    add_ref();

    block_service->create_file(
        std::move(req),
        LPC_BACKGROUND_COLD_BACKUP,
        [this, value, current_chkpt_file](const dist::block_service::create_file_response &resp) {
            if (resp.err == ERR_OK) {
                dassert(resp.file_handle != nullptr, "");
                auto len = value.length();
                std::shared_ptr<char> buf = utils::make_shared_array<char>(len);
                ::memcpy(buf.get(), value.c_str(), len);
                blob write_buf(std::move(buf), static_cast<unsigned int>(len));
                ddebug("%s: create current checkpoint file succeed, start write file ,file = %s",
                       name,
                       current_chkpt_file.c_str());
                add_ref();
                this->on_write(resp.file_handle, write_buf, [this](bool succeed) {
                    if (succeed) {
                        complete_upload();
                    }
                    release_ref();
                });
            } else if (resp.err == ERR_TIMEOUT) {
                derror("%s: block file create file timeout, retry after 10s, file = %s",
                       name,
                       current_chkpt_file.c_str());
                add_ref();

                tasking::enqueue(LPC_BACKGROUND_COLD_BACKUP,
                                 nullptr,
                                 [this, value]() {
                                     if (!is_ready_for_upload()) {
                                         ddebug("%s: backup status has changed to %s, stop write "
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
                derror("%s: block service create file failed, file = %s, err = %s",
                       name,
                       current_chkpt_file.c_str(),
                       resp.err.to_string());
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
    dassert(file_handle != nullptr, "");
    dist::block_service::write_request req;
    req.buffer = value;

    add_ref();

    file_handle->write(
        std::move(req),
        LPC_BACKGROUND_COLD_BACKUP,
        [this, value, file_handle, callback](const dist::block_service::write_response &resp) {
            if (resp.err == ERR_OK) {
                ddebug("%s: write remote file succeed, file = %s",
                       name,
                       file_handle->file_name().c_str());
                callback(true);
            } else if (resp.err == ERR_TIMEOUT) {
                ddebug("%s: write remote file timeout, retry after 10s, file = %s",
                       name,
                       file_handle->file_name().c_str());
                add_ref();

                tasking::enqueue(LPC_BACKGROUND_COLD_BACKUP,
                                 nullptr,
                                 [this, file_handle, value, callback]() {
                                     if (!is_ready_for_upload()) {
                                         ddebug("%s: backup status has changed to %s, stop write "
                                                "remote file, file = %s",
                                                name,
                                                cold_backup_status_to_string(status()),
                                                file_handle->file_name().c_str());
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
                derror("%s: write remote file failed, file = %s, err = %s",
                       name,
                       file_handle->file_name().c_str(),
                       resp.err.to_string());
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
        ddebug("%s: upload checkpoint to remote complete, checkpoint dir = %s, total file size = "
               "%" PRId64 ", file count = %d",
               name,
               checkpoint_dir.c_str(),
               static_cast<int64_t>(total),
               checkpoint_files.size());
        bool old_status = false;
        if (_have_write_backup_metadata.compare_exchange_strong(old_status, true)) {
            write_backup_metadata();
        }
        return;
    } else {
        dassert(total != 0.0, "total = %" PRId64 "", total);
        update_progress(static_cast<int>(complete_size / total * 1000));
        ddebug("%s: the progress of upload checkpoint is %d", name, _progress.load());
    }
    if (is_ready_for_upload()) {
        std::vector<std::string> upload_files;
        upload_complete_or_fetch_uncomplete_files(upload_files);
        for (auto &file : upload_files) {
            ddebug("%s: start upload checkpoint file to remote, file = %s", name, file.c_str());
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

    dassert(_cur_upload_file_cnt >= 1, "cur_upload_file_cnt = %d", _cur_upload_file_cnt);
    _cur_upload_file_cnt -= 1;
    _file_remain_cnt += 1;
    _file_status[filename] = file_status::FileUploadUncomplete;
}

void cold_backup_context::file_upload_complete(const std::string &filename)
{
    zauto_lock l(_lock);

    dassert(_cur_upload_file_cnt >= 1, "cur_upload_file_cnt = %d", _cur_upload_file_cnt);
    _cur_upload_file_cnt -= 1;
    _file_status[filename] = file_status::FileUploadComplete;
}

bool partition_split_context::cleanup(bool force)
{
    CLEANUP_TASK(async_learn_task, force)

    parent_gpid.set_app_id(0);
    is_prepare_list_copied = false;
    is_caught_up = false;
    return true;
}

bool partition_split_context::is_cleaned() const { return async_learn_task == nullptr; }

} // namespace replication
} // namespace dsn
