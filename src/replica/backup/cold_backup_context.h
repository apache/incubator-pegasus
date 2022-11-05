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

#pragma once

#include "block_service/block_service.h"
#include "common/backup_common.h"
#include "common/json_helper.h"
#include "utils/fmt_logging.h"
#include "utils/zlocks.h"

class replication_service_test_app;

namespace dsn {
namespace replication {

class replica;

//
//                                  ColdBackupInvalid
//                                           |
//                                           V
//                         |<------ ColdBackupChecking ---------------------------------->|
//                         |                 |                                            |
//                         |                 V                                            |
//                         |        ColdBackupChecked ----------------------------------->|
//                         |                 |                                            |
//                         |                 V                                            |
// ColdBackupCompleted <---|        ColdBackupCheckpointing ----------------------------->|
//          |              |                 |                                            |
//          |              |                 V                                            |--->
//          ColdBackupCanceled
//          |              |        ColdBackupCheckpointed ------------------------------>|
//          |              |                 |                                            |
//          |              |                 V                                            |
//          |              |<------ ColdBackupUploading  <======> ColdBackupPaused ------>|
//          |                                |                            |               |
//          |                                |____________________________|               |
//          |                                               |                             |
//          |                                               V                             |
//          |                                       ColdBackupFailed -------------------->|
//          |                                                                             |
//          |---------------------------------------------------------------------------->|
//
enum cold_backup_status
{
    ColdBackupInvalid = 0,
    ColdBackupChecking,
    ColdBackupChecked,
    ColdBackupCheckpointing,
    ColdBackupCheckpointed,
    ColdBackupUploading,
    ColdBackupPaused,
    ColdBackupCanceled,
    ColdBackupCompleted,
    ColdBackupFailed
};
const char *cold_backup_status_to_string(cold_backup_status status);

struct cold_backup_metadata
{
    int64_t checkpoint_decree;
    int64_t checkpoint_timestamp;
    std::vector<file_meta> files;
    int64_t checkpoint_total_size;
    DEFINE_JSON_SERIALIZATION(checkpoint_decree, checkpoint_timestamp, files, checkpoint_total_size)
};

//
// the process of uploading the checkpoint directory to block filesystem:
//      1, upload all the file of the checkpoint to block filesystem
//      2, write a cold_backup_metadata to block filesystem(which includes all the file's name, size
//         and md5 and so on)
//      3, write a current_checkpoint file to block filesystem, which is used to mark which
//         checkpoint is invalid
//

//
// the process of check whether uploading is finished on block filesystem:
//      1, check whether the current checkpoint file exist, if exist continue, otherwise not finish
//      2, read the context of the current checkpoint file, the context of this file is the valid
//         checkpoint dirname on block filesystem
//      3, verify whether the checkpoint dirname is exist, if exist uploading is already finished,
//         otherwise uploading is not finished
//

class cold_backup_context : public ref_counter
{
public:
    explicit cold_backup_context(replica *r_,
                                 const backup_request &request_,
                                 int max_upload_file_cnt)
        : request(request_),
          block_service(nullptr),
          checkpoint_decree(0),
          checkpoint_timestamp(0),
          durable_decree_when_checkpoint(-1),
          checkpoint_file_total_size(0),
          _status(ColdBackupInvalid),
          _progress(0),
          _upload_file_size(0),
          _have_check_upload_status(false),
          _have_write_backup_metadata(false),
          _upload_status(UploadInvalid),
          _max_concurrent_uploading_file_cnt(max_upload_file_cnt),
          _cur_upload_file_cnt(0),
          _file_remain_cnt(0),
          _owner_replica(r_),
          _start_time_ms(0)
    {
        sprintf(name,
                "backup{%d.%d.%s.%" PRId64 "}",
                request.pid.get_app_id(),
                request.pid.get_partition_index(),
                request.policy.policy_name.c_str(),
                request.backup_id);
        memset(_reason, 0, sizeof(_reason));
    }

    ~cold_backup_context() {}

    // cancel backup.
    //   {*} --> ColdBackupCanceled
    //
    // Will be called in replication thread.
    void cancel();

    // start checking backup on remote.
    //   ColdBackupInvalid --> ColdBackupChecking
    // Returns:
    //   - true if status is successfully changed to ColdBackupChecking.
    bool start_check();

    // ignore checking backup on remote and switch backward status.
    //   ColdBackupChecking --> ColdBackupInvalid
    // Returns:
    //   - true if status is successfully changed to ColdBackupInvalid.
    bool ignore_check()
    {
        int checking = ColdBackupChecking;
        return _status.compare_exchange_strong(checking, ColdBackupInvalid);
    }

    // mark failed when checking backup on remote.
    //   ColdBackupChecking --> ColdBackupFailed
    // Returns:
    //   - true if status is successfully changed to ColdBackupFailed.
    bool fail_check(const char *failure_reason);

    // complete checking backup on remote.
    //   ColdBackupChecking --> { ColdBackupChecked | ColdBackupCompleted }
    // Returns:
    //   - true if status is successfully changed to ColdBackupChecked or ColdBackupCompleted.
    bool complete_check(bool uploaded);

    // start generating checkpoint.
    //   ColdBackupChecked --> ColdBackupCheckpointing
    // Returns:
    //   - true if status is successfully changed to ColdBackupCheckpointing.
    bool start_checkpoint();

    // ignore generating checkpoint and switch backward status.
    //   ColdBackupCheckpointing --> ColdBackupChecked
    // Returns:
    //   - true if status is successfully changed to ColdBackupChecked.
    bool ignore_checkpoint()
    {
        int checkpointing = ColdBackupCheckpointing;
        return _status.compare_exchange_strong(checkpointing, ColdBackupChecked);
    }

    // mark failed when generating checkpoint.
    //   ColdBackupCheckpointing --> ColdBackupFailed
    // Returns:
    //   - true if status is successfully changed to ColdBackupFailed.
    bool fail_checkpoint(const char *failure_reason);

    // complete generating checkpoint.
    //   ColdBackupCheckpointing --> ColdBackupCheckpointed
    // Returns:
    //   - true if status is successfully changed to ColdBackupCheckpointed.
    bool complete_checkpoint();

    // start uploading checkpoint to remote.
    //   { ColdBackupCheckpointed | ColdBackupPaused } --> ColdBackupUploading
    //
    // Will be called in replication thread.
    // Returns:
    //   - true if status is successfully changed to ColdBackupUploading.
    bool start_upload()
    {
        int checkpointed = ColdBackupCheckpointed;
        int paused = ColdBackupPaused;
        return _status.compare_exchange_strong(checkpointed, ColdBackupUploading) ||
               _status.compare_exchange_strong(paused, ColdBackupUploading);
    }

    // mark failed when uploading checkpoint to remote.
    //   { ColdBackupUploading | ColdBackupPaused } --> ColdBackupFailed
    // Returns:
    //   - true if status is successfully changed to ColdBackupFailed.
    bool fail_upload(const char *failure_reason);

    // complete uploading checkpoint to remote.
    //   { ColdBackupUploading | ColdBackupPaused } --> ColdBackupCompleted
    // Returns:
    //   - true if status is successfully changed to ColdBackupCompleted.
    bool complete_upload();

    // update progress.
    // Progress should be in range of [0, 1000].
    void update_progress(int progress)
    {
        CHECK(progress >= 0 && progress <= cold_backup_constant::PROGRESS_FINISHED,
              "invalid progress {}",
              progress);
        _progress.store(progress);
    }

    // check if it is ready for checking.
    bool is_ready_for_check() const { return _status.load() == ColdBackupChecking; }

    // check if it is ready for checkpointing.
    bool is_checkpointing() const { return _status.load() == ColdBackupCheckpointing; }

    // check if it is ready for uploading.
    bool is_ready_for_upload() const { return _status.load() == ColdBackupUploading; }

    // get current status.
    cold_backup_status status() const { return (cold_backup_status)_status.load(); }

    // get current progress.
    int progress() const { return _progress.load(); }

    // get failure reason.
    const char *reason() const { return _reason; }

    // check if backup is aleady exist on remote.
    // Preconditions:
    //   - name/request are set
    //   - checkpoint_dir/checkpoint_decree/checkpoint_files are not set
    //   - status is one of { ColdBackupChecking, ColdBackupCanceled }
    // Will be called in background thread.
    void check_backup_on_remote();

    // upload backup checkpoint to remote.
    // Preconditions:
    //   - name/request are set
    //   - checkpoint_dir/checkpoint_decree/checkpoint_files are set
    //   - status is one of { ColdBackupUploading, ColdBackupPaused, ColdBackupCanceled }
    // Will be called in background thread.
    void upload_checkpoint_to_remote();

    uint64_t get_start_time_ms() { return _start_time_ms; }

    uint64_t get_upload_file_size() { return _upload_file_size.load(); }

    int64_t get_checkpoint_total_size() { return checkpoint_file_total_size; }

private:
    void read_current_chkpt_file(const dist::block_service::block_file_ptr &file_handle);
    void remote_chkpt_dir_exist(const std::string &chkpt_dirname);

    void read_backup_metadata(const dist::block_service::block_file_ptr &file_handle);
    // value is a json string, verify it's validity
    // validity means uploading checkpoint directory complete, so just write_current_chkpt_file
    // otherwise, upload checkpoint directory
    void verify_backup_metadata(const blob &value);
    // after upload_checkpoint_directory ---> write_backup_metadata --> write_current_chkpt_file -->
    // notify meta
    void write_backup_metadata();

    void write_current_chkpt_file(const std::string &value);
    // write value to file, if succeed then callback(true), else callback(false)
    void on_write(const dist::block_service::block_file_ptr &file_handle,
                  const blob &value,
                  const std::function<void(bool)> &callback);
    void prepare_upload();
    void on_upload_chkpt_dir();
    void upload_file(const std::string &local_filename);
    void on_upload(const dist::block_service::block_file_ptr &file_handle,
                   const std::string &full_path_local_file);
    void on_upload_file_complete(const std::string &local_filename);

    // functions access the structure protected by _lock
    // return:
    //  -- true, uploading is complete
    //  -- false, uploading is not complete; and put uncomplete file into 'files'
    bool upload_complete_or_fetch_uncomplete_files(std::vector<std::string> &files);
    void file_upload_uncomplete(const std::string &filename);
    void file_upload_complete(const std::string &filename);

public:
    /// the following variables are public, and will only be set once, and will not be changed once
    /// set.
    char name[256]; // backup{<app_id>.<partition_index>.<policy_name>.<backup_id>}
    // all logging should print the name
    backup_request request;
    dist::block_service::block_filesystem *block_service;
    std::string backup_root;
    decree checkpoint_decree;
    int64_t checkpoint_timestamp;
    decree durable_decree_when_checkpoint;
    std::string checkpoint_dir;
    std::vector<std::string> checkpoint_files;
    std::vector<int64_t> checkpoint_file_sizes;
    int64_t checkpoint_file_total_size;

private:
    friend class ::replication_service_test_app;

    /// state variables
    std::atomic_int _status;
    std::atomic_int _progress; // [0,1000], 1000 means completed
    char _reason[1024];        // failure reason

    std::atomic_llong _upload_file_size;
    // TODO: if chechpoint directory has many files, cold_backup_metadata may
    // occupy large amount of memory
    // for example, if a single file occupy 32B, then 1,000,000 files may occupy 32MB
    cold_backup_metadata _metadata;

    enum upload_status
    {
        UploadInvalid = 0,
        UploadUncomplete,
        UploadComplete
    };
    enum file_status
    {
        FileUploadUncomplete = 0,
        FileUploading,
        FileUploadComplete
    };

    // two atomic variants is to ensure check_upload_status and write_backup_metadata just be
    // executed once
    std::atomic_bool _have_check_upload_status;
    std::atomic_bool _have_write_backup_metadata;

    std::atomic_int _upload_status;

    int32_t _max_concurrent_uploading_file_cnt;
    // filename -> <filesize, md5>
    std::map<std::string, std::pair<int64_t, std::string>> _file_infos;

    zlock _lock; // lock the structure below
    std::map<std::string, file_status> _file_status;
    int32_t _cur_upload_file_cnt;
    int32_t _file_remain_cnt;

    replica *_owner_replica;
    uint64_t _start_time_ms;
};

typedef dsn::ref_ptr<cold_backup_context> cold_backup_context_ptr;

} // namespace replication
} // namespace dsn
