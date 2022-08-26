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
#include <dsn/utility/fail_point.h>

#include "replica_backup_manager.h"

namespace dsn {
namespace replication {

// TODO(heyuchen): implement it

replica_backup_manager::replica_backup_manager(replica *r)
    : replica_base(r), _replica(r), _stub(r->get_replica_stub())
{
}

replica_backup_manager::~replica_backup_manager() {}

// ThreadPool: THREAD_POOL_REPLICATION
void replica_backup_manager::on_backup(const backup_request &request,
                                       /*out*/ backup_response &response)
{
    // TODO(heyuchen): add other status

    if (request.status == backup_status::CHECKPOINTING) {
        try_to_checkpoint(request.backup_id, response);
        return;
    }

    if (request.status == backup_status::UPLOADING) {
        try_to_upload(request.backup_provider_type,
                      request.__isset.backup_root_path ? request.backup_root_path : "",
                      request.app_name,
                      response);
        return;
    }
}

// ThreadPool: THREAD_POOL_REPLICATION
void replica_backup_manager::try_to_checkpoint(const int64_t &backup_id,
                                               /*out*/ backup_response &response)
{
    switch (get_backup_status()) {
    case backup_status::UNINITIALIZED:
        start_checkpointing(backup_id, response);
        break;
    case backup_status::CHECKPOINTING:
    case backup_status::CHECKPOINTED:
        report_checkpointing(response);
        break;
    default:
        response.err = ERR_INVALID_STATE;
        derror_replica("invalid local status({}) while request status = {}",
                       enum_to_string(_status),
                       enum_to_string(backup_status::CHECKPOINTING));
        break;
    }
}

// ThreadPool: THREAD_POOL_REPLICATION
void replica_backup_manager::start_checkpointing(int64_t backup_id,
                                                 /*out*/ backup_response &response)
{
    FAIL_POINT_INJECT_F("replica_backup_start_checkpointing", [&](dsn::string_view) {
        _status = backup_status::CHECKPOINTING;
        response.err = ERR_OK;
    });

    ddebug_replica("start to checkpoint, backup_id = {}", backup_id);
    zauto_write_lock l(_lock);
    _status = backup_status::CHECKPOINTING;
    _backup_id = backup_id;
    _checkpointing_task =
        tasking::enqueue(LPC_BACKGROUND_COLD_BACKUP,
                         tracker(),
                         std::bind(&replica_backup_manager::generate_checkpoint, this));
    fill_response_unlock(response);
}

// ThreadPool: THREAD_POOL_REPLICATION
void replica_backup_manager::report_checkpointing(/*out*/ backup_response &response)
{
    ddebug_replica("check checkpoint, backup_id = {}", _backup_id);
    zauto_read_lock l(_lock);
    if (_checkpoint_err != ERR_OK) {
        derror_replica("checkpoint failed, error = {}", _checkpoint_err);
        response.__set_checkpoint_upload_err(_checkpoint_err);
    }
    fill_response_unlock(response);
}

// ThreadPool: THREAD_POOL_REPLICATION
void replica_backup_manager::try_to_upload(const std::string &provider_type,
                                           const std::string &root_path,
                                           const std::string app_name,
                                           /*out*/ backup_response &response)
{
    switch (get_backup_status()) {
    case backup_status::CHECKPOINTED:
        start_uploading(provider_type, root_path, app_name, response);
        break;
    case backup_status::UPLOADING:
        report_uploading(response);
        break;
    case backup_status::SUCCEED:
        upload_completed(response);
        break;
    default:
        response.err = ERR_INVALID_STATE;
        derror_replica("invalid local status({}) while request status = {}",
                       enum_to_string(_status),
                       enum_to_string(backup_status::UPLOADING));
        break;
    }
}

// ThreadPool: THREAD_POOL_REPLICATION
void replica_backup_manager::start_uploading(const std::string &provider_name,
                                             const std::string &root_path,
                                             const std::string &app_name,
                                             /*out*/ backup_response &response)
{
    FAIL_POINT_INJECT_F("replica_backup_start_uploading", [&](dsn::string_view) {
        _status = backup_status::UPLOADING;
        response.err = ERR_OK;
    });
    zauto_write_lock l(_lock);
    _uploading_task = tasking::enqueue(
        LPC_BACKGROUND_COLD_BACKUP,
        tracker(),
        std::bind(
            &replica_backup_manager::upload_checkpoint, this, provider_name, root_path, app_name));
    _status = backup_status::UPLOADING;
    fill_response_unlock(response);
}

// ThreadPool: THREAD_POOL_REPLICATION
void replica_backup_manager::report_uploading(/*out*/ backup_response &response)
{
    zauto_read_lock l(_lock);
    if (_upload_err == ERR_IO_PENDING || _upload_err == ERR_OK) {
        response.__set_upload_progress(calc_upload_progress());
    } else {
        response.__set_checkpoint_upload_err(_upload_err);
    }
    fill_response_unlock(response);
}

// ThreadPool: THREAD_POOL_REPLICATION
void replica_backup_manager::upload_completed(/*out*/ backup_response &response)
{
    FAIL_POINT_INJECT_F("replica_backup_upload_completed",
                        [&](dsn::string_view) { response.err = ERR_OK; });

    zauto_read_lock l(_lock);
    fill_response_unlock(response);
    response.__set_checkpoint_total_size(_backup_metadata.checkpoint_total_size);
    tasking::enqueue(LPC_REPLICATION_COLD_BACKUP,
                     tracker(),
                     std::bind(&replica_backup_manager::clear_context, this),
                     get_gpid().thread_hash(),
                     std::chrono::seconds(5));
}

// ThreadPool: THREAD_POOL_REPLICATION
void replica_backup_manager::fill_response_unlock(/*out*/ backup_response &response)
{
    response.err = ERR_OK;
    response.pid = get_gpid();
    response.backup_id = _backup_id;
    response.status = _status;
}

// ThreadPool: THREAD_POOL_REPLICATION_LONG
void replica_backup_manager::generate_checkpoint()
{
    const auto &local_checkpoint_dir = get_local_checkpoint_dir();

    if (!utils::filesystem::directory_exists(local_checkpoint_dir) &&
        !utils::filesystem::create_directory(local_checkpoint_dir)) {
        derror_replica("create local backup dir {} failed", local_checkpoint_dir);
        set_checkpoint_err(ERR_FILE_OPERATION_FAILED);
        return;
    }

    // generate checkpoint and flush memtable
    int64_t checkpoint_decree;
    const auto &ec = _replica->_app->copy_checkpoint_to_dir(
        local_checkpoint_dir.c_str(), &checkpoint_decree, true);
    if (ec != ERR_OK) {
        derror_replica("generate backup checkpoint failed, error = {}", ec);
        set_checkpoint_err(ec);
        return;
    }
    ddebug_replica(
        "generate backup checkpoint succeed: checkpoint dir = {}, checkpoint decree = {}",
        local_checkpoint_dir,
        checkpoint_decree);

    {
        zauto_write_lock l(_lock);
        if (!set_backup_metadata_unlock(
                local_checkpoint_dir, checkpoint_decree, static_cast<int64_t>(dsn_now_ms()))) {
            _checkpoint_err = ERR_FILE_OPERATION_FAILED;
            return;
        }
        _checkpoint_err = ERR_OK;
        _status = backup_status::CHECKPOINTED;
    }
}

// ThreadPool: THREAD_POOL_REPLICATION_LONG
bool replica_backup_manager::set_backup_metadata_unlock(const std::string &local_checkpoint_dir,
                                                        int64_t checkpoint_decree,
                                                        int64_t checkpoint_timestamp)
{
    FAIL_POINT_INJECT_F("replica_set_backup_metadata", [](dsn::string_view) { return true; });

    std::vector<std::string> sub_files;
    if (!utils::filesystem::get_subfiles(local_checkpoint_dir, sub_files, false)) {
        derror_replica("list sub files of local checkpoint dir = {} failed", local_checkpoint_dir);
        return false;
    }

    int64_t total_file_size = 0;
    for (const auto &file : sub_files) {
        file_meta meta;
        meta.name = utils::filesystem::get_file_name(file);
        if (!utils::filesystem::file_size(file, meta.size)) {
            derror_replica("get file size of {} failed", file);
            return false;
        }
        if (utils::filesystem::md5sum(file, meta.md5) != ERR_OK) {
            derror_replica("get file md5 of {} failed", file);
            return false;
        }
        total_file_size += meta.size;
        _backup_metadata.files.emplace_back(meta);
    }

    if (total_file_size <= 0) {
        derror_replica(
            "wrong metadata, total_size={}, file_count={}", total_file_size, sub_files.size());
        return false;
    }

    _backup_metadata.checkpoint_decree = checkpoint_decree;
    _backup_metadata.checkpoint_timestamp = checkpoint_timestamp;
    _backup_metadata.checkpoint_total_size = total_file_size;
    ddebug_replica("set backup metadata succeed, decree = {}, timestamp = {}, file_count = {}, "
                   "total_size = {}",
                   _backup_metadata.checkpoint_decree,
                   _backup_metadata.checkpoint_timestamp,
                   _backup_metadata.files.size(),
                   _backup_metadata.checkpoint_total_size);

    return true;
}

// ThreadPool: THREAD_POOL_REPLICATION_LONG
void replica_backup_manager::upload_checkpoint(const std::string &provider_name,
                                               const std::string &root_path,
                                               const std::string &app_name)
{
    ddebug_replica("start to upload files");
    dist::block_service::block_filesystem *fs =
        _stub->_block_service_manager.get_or_create_block_filesystem(provider_name);

    // get remote partition directory path
    auto pid = get_gpid();
    std::string remote_partition_dir =
        get_backup_partition_path(get_backup_root(FLAGS_cold_backup_root, root_path),
                                  app_name,
                                  pid.get_app_id(),
                                  _backup_id,
                                  pid.get_partition_index());
    {
        zauto_write_lock l(_lock);
        if (!_backup_metadata.files.empty()) {
            const file_meta &f_meta = _backup_metadata.files[0];
            _upload_files_task[f_meta.name] =
                tasking::enqueue(LPC_BACKGROUND_COLD_BACKUP,
                                 tracker(),
                                 std::bind(&replica_backup_manager::upload_file,
                                           this,
                                           fs,
                                           remote_partition_dir,
                                           f_meta,
                                           1));
        }
    }
}

// ThreadPool: THREAD_POOL_REPLICATION_LONG
void replica_backup_manager::upload_file(dist::block_service::block_filesystem *fs,
                                         const std::string &remote_partition_dir,
                                         const file_meta &f_meta,
                                         int32_t next_index)
{
    const auto &local_checkpoint_dir = get_local_checkpoint_dir();
    std::string remote_checkpoint_dir = get_backup_checkpoint_path(remote_partition_dir);
    const auto &err = _stub->_block_service_manager.upload_file(
        remote_checkpoint_dir, local_checkpoint_dir, f_meta.name, fs);
    if (err != ERR_OK) {
        derror_replica("upload file {} failed, error = {}", f_meta.name, err);
        set_upload_err(err);
        return;
    }

    std::vector<file_meta> files;
    {
        zauto_write_lock l(_lock);
        _upload_file_size.fetch_add(f_meta.size);
        files = _backup_metadata.files;
    }
    // update next file
    if (next_index < files.size()) {
        zauto_write_lock l(_lock);
        const file_meta &f_meta = files[next_index];
        _upload_files_task[f_meta.name] =
            tasking::enqueue(LPC_BACKGROUND_COLD_BACKUP,
                             tracker(),
                             std::bind(&replica_backup_manager::upload_file,
                                       this,
                                       fs,
                                       remote_partition_dir,
                                       f_meta,
                                       next_index + 1));
    } else if (next_index == files.size()) {
        upload_file_completed(fs, remote_partition_dir);
    }
}

// ThreadPool: THREAD_POOL_REPLICATION_LONG
void replica_backup_manager::upload_file_completed(dist::block_service::block_filesystem *fs,
                                                   const std::string &remote_partition_dir)
{
    auto checkpoint_str = get_checkpoint_str();
    auto remote_checkpoint_dir = get_backup_checkpoint_path(remote_partition_dir);
    auto data_version_str = std::to_string(_replica->query_data_version());
    cold_backup_metadata metadata;
    {
        zauto_read_lock l(_lock);
        metadata = _backup_metadata;
    }

    if (write_file_to_blockfs(fs,
                              remote_checkpoint_dir,
                              backup_constant::BACKUP_METADATA,
                              json::json_forwarder<cold_backup_metadata>::encode(metadata)) !=
            ERR_OK ||
        write_file_to_blockfs(fs,
                              remote_partition_dir,
                              backup_constant::CURRENT_CHECKPOINT,
                              blob::create_from_bytes(std::move(checkpoint_str))) != ERR_OK ||
        write_file_to_blockfs(fs,
                              remote_partition_dir,
                              backup_constant::DATA_VERSION,
                              blob::create_from_bytes(std::move(data_version_str))) != ERR_OK) {
        return;
    }

    {
        zauto_write_lock l(_lock);
        _upload_err = ERR_OK;
        _status = backup_status::SUCCEED;
    }
}

// ThreadPool: THREAD_POOL_REPLICATION_LONG
error_code replica_backup_manager::write_file_to_blockfs(dist::block_service::block_filesystem *fs,
                                                         const std::string &remote_dir,
                                                         const std::string &file_name,
                                                         const blob &buffer)
{
    const auto &err = _stub->_block_service_manager.write_file(remote_dir, file_name, buffer, fs);
    if (err != ERR_OK) {
        derror_replica("write {} failed, error = {}", file_name, err);
        set_upload_err(err);
    }
    return err;
}

// ThreadPool: THREAD_POOL_REPLICATION
int32_t replica_backup_manager::calc_upload_progress()
{
    if (_backup_metadata.checkpoint_total_size <= 0) {
        return 0;
    }
    auto total_size = static_cast<double>(_backup_metadata.checkpoint_total_size);
    auto cur_upload_size = static_cast<double>(_upload_file_size.load());
    return static_cast<int32_t>((cur_upload_size / total_size) * 100);
}

// ThreadPool: THREAD_POOL_REPLICATION
void replica_backup_manager::clear_context()
{
    FAIL_POINT_INJECT_F("replica_backup_clear_context",
                        [this](dsn::string_view) { _status = backup_status::UNINITIALIZED; });
    // TODO(heyuchen): TBD
}

} // namespace replication
} // namespace dsn
