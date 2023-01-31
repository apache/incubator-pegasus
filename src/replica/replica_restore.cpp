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

#include <fstream>
#include <boost/lexical_cast.hpp>

#include "utils/error_code.h"
#include "utils/factory_store.h"
#include "utils/filesystem.h"
#include "utils/utils.h"

#include "replica/replication_app_base.h"
#include "utils/fmt_logging.h"

#include "replica.h"
#include "mutation_log.h"
#include "replica_stub.h"
#include "block_service/block_service_manager.h"
#include "backup/cold_backup_context.h"

using namespace dsn::dist::block_service;

namespace dsn {
namespace replication {

bool replica::remove_useless_file_under_chkpt(const std::string &chkpt_dir,
                                              const cold_backup_metadata &metadata)
{
    std::vector<std::string> sub_files;
    // filename --> file_path such as: file --> ***/***/file
    std::map<std::string, std::string> name_to_filepath;
    if (!::dsn::utils::filesystem::get_subfiles(chkpt_dir, sub_files, false)) {
        LOG_ERROR_PREFIX("get subfile of dir({}) failed", chkpt_dir);
        return false;
    }

    for (const auto &file : sub_files) {
        name_to_filepath.insert(
            std::make_pair(::dsn::utils::filesystem::get_file_name(file), file));
    }

    for (const auto &f_meta : metadata.files) {
        name_to_filepath.erase(f_meta.name);
    }

    // remove useless files execpt cold_backup_constant::BACKUP_METADATA file
    for (const auto &pair : name_to_filepath) {
        if (pair.first == cold_backup_constant::BACKUP_METADATA)
            continue;
        if (::dsn::utils::filesystem::file_exists(pair.second) &&
            !::dsn::utils::filesystem::remove_path(pair.second)) {
            LOG_ERROR_PREFIX("remove useless file({}) failed", pair.second);
            return false;
        }
        LOG_INFO_PREFIX("remove useless file({}) succeed", pair.second);
    }
    return true;
}

bool replica::read_cold_backup_metadata(const std::string &file,
                                        cold_backup_metadata &backup_metadata)
{
    if (!::dsn::utils::filesystem::file_exists(file)) {
        LOG_ERROR_PREFIX(
            "checkpoint on remote storage media is damaged, coz file({}) doesn't exist", file);
        return false;
    }
    int64_t file_sz = 0;
    if (!::dsn::utils::filesystem::file_size(file, file_sz)) {
        LOG_ERROR_PREFIX("get file({}) size failed", file);
        return false;
    }
    std::shared_ptr<char> buf = utils::make_shared_array<char>(file_sz + 1);

    std::ifstream fin(file, std::ifstream::in);
    if (!fin.is_open()) {
        LOG_ERROR_PREFIX("open file({}) failed", file);
        return false;
    }
    fin.read(buf.get(), file_sz);
    CHECK_EQ_MSG(file_sz,
                 fin.gcount(),
                 "{}: read file({}) failed, need {}, but read {}",
                 name(),
                 file,
                 file_sz,
                 fin.gcount());
    fin.close();

    buf.get()[fin.gcount()] = '\0';
    blob bb;
    bb.assign(std::move(buf), 0, file_sz);
    if (!::dsn::json::json_forwarder<cold_backup_metadata>::decode(bb, backup_metadata)) {
        LOG_ERROR_PREFIX("file({}) under checkpoint is damaged", file);
        return false;
    }
    return true;
}

error_code replica::download_checkpoint(const configuration_restore_request &req,
                                        const std::string &remote_chkpt_dir,
                                        const std::string &local_chkpt_dir)
{
    block_filesystem *fs =
        _stub->_block_service_manager.get_or_create_block_filesystem(req.backup_provider_name);

    // download metadata file and parse it into cold_backup_meta
    cold_backup_metadata backup_metadata;
    error_code err = get_backup_metadata(fs, remote_chkpt_dir, local_chkpt_dir, backup_metadata);
    if (err != ERR_OK) {
        return err;
    }

    // download checkpoint files
    task_tracker tracker;
    for (const auto &f_meta : backup_metadata.files) {
        tasking::enqueue(
            TASK_CODE_EXEC_INLINED,
            &tracker,
            [this, &err, remote_chkpt_dir, local_chkpt_dir, f_meta, fs]() {
                uint64_t f_size = 0;
                error_code download_err = _stub->_block_service_manager.download_file(
                    remote_chkpt_dir, local_chkpt_dir, f_meta.name, fs, f_size);
                const std::string file_name =
                    utils::filesystem::path_combine(local_chkpt_dir, f_meta.name);
                if (download_err == ERR_OK || download_err == ERR_PATH_ALREADY_EXIST) {
                    if (!utils::filesystem::verify_file(file_name, f_meta.md5, f_meta.size)) {
                        download_err = ERR_CORRUPTION;
                    } else if (download_err == ERR_PATH_ALREADY_EXIST) {
                        download_err = ERR_OK;
                        f_size = f_meta.size;
                    }
                }

                if (download_err != ERR_OK) {
                    LOG_ERROR_PREFIX(
                        "failed to download file({}), error = {}", f_meta.name, download_err);
                    // ERR_CORRUPTION means we should rollback restore, so we can't change err if it
                    // is ERR_CORRUPTION now, otherwise it will be overridden by other errors
                    if (err != ERR_CORRUPTION) {
                        err = download_err;
                        return;
                    }
                }

                // update progress if download file succeed
                update_restore_progress(f_size);
                // report current status to meta server
                report_restore_status_to_meta();
            });
    }
    tracker.wait_outstanding_tasks();

    // clear useless files for restore.
    // if err != ERR_OK, the entire directory of this replica will be deleted later.
    // so in this situation, there is no need to clear restore.
    if (ERR_OK == err) {
        clear_restore_useless_files(local_chkpt_dir, backup_metadata);
    }

    return err;
}

error_code replica::get_backup_metadata(block_filesystem *fs,
                                        const std::string &remote_chkpt_dir,
                                        const std::string &local_chkpt_dir,
                                        cold_backup_metadata &backup_metadata)
{
    // download metadata file
    uint64_t download_file_size = 0;
    error_code err =
        _stub->_block_service_manager.download_file(remote_chkpt_dir,
                                                    local_chkpt_dir,
                                                    cold_backup_constant::BACKUP_METADATA,
                                                    fs,
                                                    download_file_size);
    if (err != ERR_OK && err != ERR_PATH_ALREADY_EXIST) {
        LOG_ERROR_PREFIX("download backup_metadata failed, file({}), reason({})",
                         utils::filesystem::path_combine(remote_chkpt_dir,
                                                         cold_backup_constant::BACKUP_METADATA),
                         err);
        return err;
    }

    // parse cold_backup_meta from metadata file
    const std::string local_backup_metada_file =
        utils::filesystem::path_combine(local_chkpt_dir, cold_backup_constant::BACKUP_METADATA);
    if (!read_cold_backup_metadata(local_backup_metada_file, backup_metadata)) {
        LOG_ERROR_PREFIX("read cold_backup_metadata from file({}) failed",
                         local_backup_metada_file);
        return ERR_FILE_OPERATION_FAILED;
    }

    _chkpt_total_size = backup_metadata.checkpoint_total_size;
    LOG_INFO_PREFIX(
        "recover cold_backup_metadata from file({}) succeed, total checkpoint size({}), file "
        "count({})",
        local_backup_metada_file,
        _chkpt_total_size,
        backup_metadata.files.size());
    return ERR_OK;
}

void replica::clear_restore_useless_files(const std::string &local_chkpt_dir,
                                          const cold_backup_metadata &metadata)
{
    if (!remove_useless_file_under_chkpt(local_chkpt_dir, metadata)) {
        LOG_WARNING_PREFIX("remove useless file failed, chkpt = {}", local_chkpt_dir);
    } else {
        LOG_INFO_PREFIX("remove useless file succeed, chkpt = {}", local_chkpt_dir);
    }

    const std::string metadata_file =
        utils::filesystem::path_combine(local_chkpt_dir, cold_backup_constant::BACKUP_METADATA);
    if (!utils::filesystem::remove_path(metadata_file)) {
        LOG_WARNING_PREFIX("remove backup_metadata failed, file = {}", metadata_file);
    } else {
        LOG_INFO_PREFIX("remove backup_metadata succeed, file = {}", metadata_file);
    }
}

dsn::error_code replica::find_valid_checkpoint(const configuration_restore_request &req,
                                               std::string &remote_chkpt_dir)
{
    LOG_INFO("{}: start to find valid checkpoint of backup_id {}", name(), req.time_stamp);

    // we should base on old gpid to combine the path on cold backup media
    dsn::gpid old_gpid;
    old_gpid.set_app_id(req.app_id);
    old_gpid.set_partition_index(_config.pid.get_partition_index());
    std::string backup_root = req.cluster_name;
    if (!req.restore_path.empty()) {
        backup_root = dsn::utils::filesystem::path_combine(req.restore_path, backup_root);
    }
    if (!req.policy_name.empty()) {
        backup_root = dsn::utils::filesystem::path_combine(backup_root, req.policy_name);
    }
    int64_t backup_id = req.time_stamp;

    std::string manifest_file =
        cold_backup::get_current_chkpt_file(backup_root, req.app_name, old_gpid, backup_id);
    block_filesystem *fs =
        _stub->_block_service_manager.get_or_create_block_filesystem(req.backup_provider_name);
    if (fs == nullptr) {
        LOG_ERROR("{}: get block filesystem by provider {} failed",
                  std::string(name()),
                  req.backup_provider_name);
        return ERR_CORRUPTION;
    }

    create_file_response create_response;
    fs->create_file(
          create_file_request{manifest_file, false},
          TASK_CODE_EXEC_INLINED,
          [&create_response](const create_file_response &resp) { create_response = resp; },
          nullptr)
        ->wait();

    if (create_response.err != dsn::ERR_OK) {
        LOG_ERROR("{}: create file of block_service failed, reason {}",
                  name(),
                  create_response.err.to_string());
        return create_response.err;
    }

    // TODO: check the md5sum
    read_response r;
    create_response.file_handle
        ->read(read_request{0, -1},
               TASK_CODE_EXEC_INLINED,
               [&r](const read_response &resp) { r = resp; },
               nullptr)
        ->wait();

    if (r.err != dsn::ERR_OK) {
        LOG_ERROR("{}: read file {} failed, reason {}",
                  name(),
                  create_response.file_handle->file_name(),
                  r.err.to_string());
        return r.err;
    }

    std::string valid_chkpt_entry(r.buffer.data(), r.buffer.length());
    LOG_INFO("{}: got a valid chkpt {}", name(), valid_chkpt_entry);
    remote_chkpt_dir = ::dsn::utils::filesystem::path_combine(
        cold_backup::get_replica_backup_path(backup_root, req.app_name, old_gpid, backup_id),
        valid_chkpt_entry);
    return dsn::ERR_OK;
}

dsn::error_code replica::restore_checkpoint()
{
    // first check the parameter
    configuration_restore_request restore_req;
    auto iter = _app_info.envs.find(backup_restore_constant::BLOCK_SERVICE_PROVIDER);
    CHECK_PREFIX_MSG(iter != _app_info.envs.end(),
                     "can't find {} in app_info.envs",
                     backup_restore_constant::BLOCK_SERVICE_PROVIDER);
    restore_req.backup_provider_name = iter->second;
    iter = _app_info.envs.find(backup_restore_constant::CLUSTER_NAME);
    CHECK_PREFIX_MSG(iter != _app_info.envs.end(),
                     "can't find {} in app_info.envs",
                     backup_restore_constant::CLUSTER_NAME);
    restore_req.cluster_name = iter->second;
    iter = _app_info.envs.find(backup_restore_constant::POLICY_NAME);
    CHECK_PREFIX_MSG(iter != _app_info.envs.end(),
                     "can't find {} in app_info.envs",
                     backup_restore_constant::POLICY_NAME);
    restore_req.policy_name = iter->second;
    iter = _app_info.envs.find(backup_restore_constant::APP_NAME);
    CHECK_PREFIX_MSG(iter != _app_info.envs.end(),
                     "can't find {} in app_info.envs",
                     backup_restore_constant::APP_NAME);
    restore_req.app_name = iter->second;
    iter = _app_info.envs.find(backup_restore_constant::APP_ID);
    CHECK_PREFIX_MSG(iter != _app_info.envs.end(),
                     "can't find {} in app_info.envs",
                     backup_restore_constant::APP_ID);
    restore_req.app_id = boost::lexical_cast<int32_t>(iter->second);

    iter = _app_info.envs.find(backup_restore_constant::BACKUP_ID);
    CHECK_PREFIX_MSG(iter != _app_info.envs.end(),
                     "can't find {} in app_info.envs",
                     backup_restore_constant::BACKUP_ID);
    restore_req.time_stamp = boost::lexical_cast<int64_t>(iter->second);

    bool skip_bad_partition = false;
    if (_app_info.envs.find(backup_restore_constant::SKIP_BAD_PARTITION) != _app_info.envs.end()) {
        skip_bad_partition = true;
    }

    iter = _app_info.envs.find(backup_restore_constant::RESTORE_PATH);
    if (iter != _app_info.envs.end()) {
        restore_req.__set_restore_path(iter->second);
    }

    LOG_INFO("{}: restore checkpoint(policy_name {}, backup_id {}), restore_path({}) from {} to "
             "local dir {}",
             name(),
             restore_req.policy_name,
             restore_req.time_stamp,
             restore_req.restore_path,
             restore_req.backup_provider_name,
             _dir);

    // then create a local restore dir if it doesn't exist
    if (!utils::filesystem::directory_exists(_dir) && !utils::filesystem::create_directory(_dir)) {
        LOG_ERROR("create dir {} failed", _dir);
        return ERR_FILE_OPERATION_FAILED;
    }

    std::ostringstream os;
    os << _dir << "/restore." << restore_req.policy_name << "." << restore_req.time_stamp;
    std::string restore_dir = os.str();
    if (!utils::filesystem::directory_exists(restore_dir) &&
        !utils::filesystem::create_directory(restore_dir)) {
        LOG_ERROR("create restore dir {} failed", restore_dir);
        return ERR_FILE_OPERATION_FAILED;
    }

    // then find a valid checkpoint dir and download it
    std::string remote_chkpt_dir;
    error_code err = find_valid_checkpoint(restore_req, remote_chkpt_dir);
    if (err == ERR_OK) {
        err = download_checkpoint(restore_req, remote_chkpt_dir, restore_dir);
    }

    if (err == ERR_OBJECT_NOT_FOUND || err == ERR_CORRUPTION) {
        if (skip_bad_partition) {
            _restore_status = ERR_IGNORE_BAD_DATA;
            err = skip_restore_partition(restore_dir);
        } else {
            _restore_status = ERR_CORRUPTION;
            tell_meta_to_restore_rollback();
            return ERR_CORRUPTION;
        }
    }
    report_restore_status_to_meta();
    return err;
}

dsn::error_code replica::skip_restore_partition(const std::string &restore_dir)
{
    // Attention: when skip restore partition, we should not delete restore_dir, but we must clear
    // it because we use restore_dir to tell storage engine that start an app from restore
    if (utils::filesystem::remove_path(restore_dir) &&
        utils::filesystem::create_directory(restore_dir)) {
        LOG_INFO_PREFIX("clear restore_dir({}) succeed", restore_dir);
        _restore_progress.store(cold_backup_constant::PROGRESS_FINISHED);
        return ERR_OK;
    } else {
        LOG_ERROR("clear dir {} failed", restore_dir);
        return ERR_FILE_OPERATION_FAILED;
    }
}

void replica::tell_meta_to_restore_rollback()
{
    configuration_drop_app_request request;
    drop_app_options options;
    options.success_if_not_exist = true;
    options.__set_reserve_seconds(1);
    request.app_name = _app_info.app_name;
    request.options = std::move(options);

    dsn::message_ex *msg = dsn::message_ex::create_request(RPC_CM_DROP_APP);
    ::dsn::marshall(msg, request);

    rpc_address target(_stub->_failure_detector->get_servers());
    rpc::call(target,
              msg,
              &_tracker,
              [this](error_code err, dsn::message_ex *request, dsn::message_ex *resp) {
                  if (err == ERR_OK) {
                      configuration_drop_app_response response;
                      ::dsn::unmarshall(resp, response);
                      if (response.err == ERR_OK) {
                          LOG_INFO_PREFIX("restore rolling backup succeed");
                          return;
                      } else {
                          tell_meta_to_restore_rollback();
                      }
                  } else if (err == ERR_TIMEOUT) {
                      tell_meta_to_restore_rollback();
                  }
              });
}

void replica::report_restore_status_to_meta()
{
    configuration_report_restore_status_request request;
    request.restore_status = _restore_status;
    request.pid = _config.pid;
    request.progress = _restore_progress.load();

    dsn::message_ex *msg = dsn::message_ex::create_request(RPC_CM_REPORT_RESTORE_STATUS);
    ::dsn::marshall(msg, request);
    rpc_address target(_stub->_failure_detector->get_servers());
    rpc::call(target,
              msg,
              &_tracker,
              [this](error_code err, dsn::message_ex *request, dsn::message_ex *resp) {
                  if (err == ERR_OK) {
                      configuration_report_restore_status_response response;
                      ::dsn::unmarshall(resp, response);
                      if (response.err == ERR_OK) {
                          LOG_DEBUG_PREFIX("report restore status succeed");
                          return;
                      }
                  } else if (err == ERR_TIMEOUT) {
                      // TODO: we should retry to make the result more precisely
                      // report_restore_status_to_meta();
                  }
              });
}

void replica::update_restore_progress(uint64_t f_size)
{
    if (_chkpt_total_size <= 0) {
        LOG_ERROR_PREFIX("cold_backup_metadata has invalid file_total_size({})", _chkpt_total_size);
        return;
    }

    _cur_download_size.fetch_add(f_size);
    auto total_size = static_cast<double>(_chkpt_total_size);
    auto cur_download_size = static_cast<double>(_cur_download_size.load());
    auto cur_porgress = static_cast<int32_t>((cur_download_size / total_size) * 1000);
    _restore_progress.store(cur_porgress);
    LOG_INFO_PREFIX("total_size = {}, cur_downloaded_size = {}, progress = {}",
                    total_size,
                    cur_download_size,
                    cur_porgress);
}
}
}
