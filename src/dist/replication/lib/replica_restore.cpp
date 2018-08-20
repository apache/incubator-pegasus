#include <fstream>
#include <boost/lexical_cast.hpp>

#include <dsn/utility/error_code.h>
#include <dsn/utility/factory_store.h>
#include <dsn/utility/filesystem.h>
#include <dsn/utility/utils.h>

#include <dsn/dist/replication/replication_app_base.h>

#include "replica.h"
#include "mutation_log.h"
#include "replica_stub.h"
#include "../common/block_service_manager.h"

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
        derror("%s: get subfile of dir(%s) failed", name(), chkpt_dir.c_str());
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
            derror("%s: remove useless file(%s) failed", name(), pair.second.c_str());
            return false;
        }
        ddebug("%s: remove useless file(%s) succeed", name(), pair.second.c_str());
    }
    return true;
}

bool replica::read_cold_backup_metadata(const std::string &file,
                                        cold_backup_metadata &backup_metadata)
{
    if (!::dsn::utils::filesystem::file_exists(file)) {
        derror("%s: checkpoint on remote storage media is damaged, coz file(%s) doesn't exist",
               name(),
               file.c_str());
        return false;
    }
    int64_t file_sz = 0;
    if (!::dsn::utils::filesystem::file_size(file, file_sz)) {
        derror("%s: get file(%s) size failed", name(), file.c_str());
        return false;
    }
    std::shared_ptr<char> buf = utils::make_shared_array<char>(file_sz + 1);

    std::ifstream fin(file, std::ifstream::in);
    if (!fin.is_open()) {
        derror("%s: open file(%s) failed", name(), file.c_str());
        return false;
    }
    fin.read(buf.get(), file_sz);
    dassert(file_sz == fin.gcount(),
            "%s: read file(%s) failed, need %" PRId64 ", but read %" PRId64 "",
            name(),
            file.c_str(),
            file_sz,
            fin.gcount());
    fin.close();

    buf.get()[fin.gcount()] = '\0';
    blob bb;
    bb.assign(std::move(buf), 0, file_sz);
    if (!::dsn::json::json_forwarder<cold_backup_metadata>::decode(bb, backup_metadata)) {
        derror("%s: file(%s) under checkpoint is damaged", name(), file.c_str());
        return false;
    }
    return true;
}

// verify whether the checkpoint directory is damaged base on backup_metadata under the chkpt
bool replica::verify_checkpoint(const cold_backup_metadata &backup_metadata,
                                const std::string &chkpt_dir)
{
    for (const auto &f_meta : backup_metadata.files) {
        std::string local_file = ::dsn::utils::filesystem::path_combine(chkpt_dir, f_meta.name);
        int64_t file_sz = 0;
        std::string md5;
        if (!::dsn::utils::filesystem::file_size(local_file, file_sz)) {
            derror("%s: get file(%s) size failed", name(), local_file.c_str());
            return false;
        }
        if (::dsn::utils::filesystem::md5sum(local_file, md5) != ERR_OK) {
            derror("%s: get file(%s) md5 failed", name(), local_file.c_str());
            return false;
        }
        if (file_sz != f_meta.size || md5 != f_meta.md5) {
            derror("%s: file(%s) under checkpoint is damaged", name(), local_file.c_str());
            return false;
        }
    }
    return remove_useless_file_under_chkpt(chkpt_dir, backup_metadata);
}

dsn::error_code replica::download_checkpoint(const configuration_restore_request &req,
                                             const std::string &remote_chkpt_dir,
                                             const std::string &local_chkpt_dir)
{
    block_filesystem *fs =
        _stub->_block_service_manager.get_block_filesystem(req.backup_provider_name);

    dsn::error_code err = dsn::ERR_OK;
    dsn::task_tracker tracker;

    auto download_file_callback_func = [this, &err](
        const download_response &d_resp, block_file_ptr f, const std::string &local_file) {
        if (d_resp.err != dsn::ERR_OK) {
            if (d_resp.err == ERR_OBJECT_NOT_FOUND) {
                derror("%s: partition-data on cold backup media is damaged", name());
                _restore_status = ERR_CORRUPTION;
            }
            err = d_resp.err;
        } else {
            // TODO: find a better way to replace dassert
            dassert(d_resp.downloaded_size == f->get_size(),
                    "%s: size not match when download file(%s), total(%lld) vs downloaded(%lld)",
                    name(),
                    f->file_name().c_str(),
                    f->get_size(),
                    d_resp.downloaded_size);
            std::string current_md5;
            dsn::error_code e = utils::filesystem::md5sum(local_file, current_md5);
            if (e != dsn::ERR_OK) {
                derror("%s: calc md5sum(%s) failed", name(), local_file.c_str());
                err = e;
            } else if (current_md5 != f->get_md5sum()) {
                ddebug(
                    "%s: local file(%s) not same with remote file(%s), download failed, %s VS %s",
                    name(),
                    local_file.c_str(),
                    f->file_name().c_str(),
                    current_md5.c_str(),
                    f->get_md5sum().c_str());
                err = ERR_FILE_OPERATION_FAILED;
            } else {
                _cur_download_size.fetch_add(f->get_size());
                update_restore_progress();
                ddebug("%s: download file(%s) succeed, size(%" PRId64 "), progress(%d)",
                       name(),
                       local_file.c_str(),
                       d_resp.downloaded_size,
                       _restore_progress.load());
                report_restore_status_to_meta();
            }
        }
    };

    auto create_file_callback_func = [this,
                                      &err,
                                      &local_chkpt_dir,
                                      &tracker,
                                      &download_file_callback_func](
        const create_file_response &cr, const std::string &remote_file) {
        if (cr.err != dsn::ERR_OK) {
            derror("%s: create file(%s) failed with err(%s)",
                   name(),
                   remote_file.c_str(),
                   cr.err.to_string());
            err = cr.err;
        } else {
            block_file *f = cr.file_handle.get();
            // dassert(!f->get_md5sum().empty(), "can't get md5 for (%s)",
            // f->file_name().c_str());
            if (f->get_md5sum().empty()) {
                derror("%s: file(%s) doesn't on cold backup media", name(), f->file_name().c_str());
                // partition-data is damaged
                _restore_status = ERR_CORRUPTION;
                err = ERR_CORRUPTION;
                return;
            }
            std::string local_file = utils::filesystem::path_combine(local_chkpt_dir, remote_file);
            bool download_file = false;
            if (!utils::filesystem::file_exists(local_file)) {
                ddebug("%s: local file(%s) not exist, download it from remote file(%s)",
                       name(),
                       local_file.c_str(),
                       f->file_name().c_str());
                download_file = true;
            } else {
                std::string current_md5;
                dsn::error_code e = utils::filesystem::md5sum(local_file, current_md5);
                if (e != dsn::ERR_OK) {
                    derror("%s: calc md5sum(%s) failed", name(), local_file.c_str());
                    // here we just retry and download it
                    if (!utils::filesystem::remove_path(local_file)) {
                        err = e;
                        return;
                    }
                    download_file = true;
                } else if (current_md5 != f->get_md5sum()) {
                    ddebug("%s: local file(%s) not same with remote file(%s), redownload, "
                           "%s VS %s",
                           name(),
                           local_file.c_str(),
                           f->file_name().c_str(),
                           current_md5.c_str(),
                           f->get_md5sum().c_str());
                    download_file = true;
                } else {
                    ddebug("%s: local file(%s) has been downloaded, just ignore",
                           name(),
                           local_file.c_str());
                }
            }

            if (download_file) {
                f->download(download_request{local_file, 0, -1},
                            TASK_CODE_EXEC_INLINED,
                            std::bind(download_file_callback_func,
                                      std::placeholders::_1,
                                      cr.file_handle,
                                      local_file),
                            &tracker);
            }
        }
    };

    // first get the total size of checkpoint through download backup_metadata
    std::string remote_backup_metadata_file =
        utils::filesystem::path_combine(remote_chkpt_dir, cold_backup_constant::BACKUP_METADATA);

    fs->create_file(create_file_request{remote_backup_metadata_file, false},
                    TASK_CODE_EXEC_INLINED,
                    std::bind(create_file_callback_func,
                              std::placeholders::_1,
                              cold_backup_constant::BACKUP_METADATA),
                    &tracker);
    tracker.wait_outstanding_tasks();

    if (err != ERR_OK) {
        derror("%s: download backup_metadata failed, file(%s), reason(%s)",
               name(),
               remote_backup_metadata_file.c_str(),
               err.to_string());
        return err;
    }
    cold_backup_metadata backup_metadata;
    std::string local_backup_metada_file =
        utils::filesystem::path_combine(local_chkpt_dir, cold_backup_constant::BACKUP_METADATA);
    if (!read_cold_backup_metadata(local_backup_metada_file, backup_metadata)) {
        derror("%s: recover cold_backup_metadata from file(%s) failed",
               name(),
               local_backup_metada_file.c_str());
        return ERR_FILE_OPERATION_FAILED;
    }

    _chkpt_total_size = backup_metadata.checkpoint_total_size;
    // after downloading backup_metadata succeed, _cur_download_size will incr by the size of
    // backup_metadata, so will reset it
    _cur_download_size.store(0);
    ddebug("%s: recover cold_backup_metadata from file(%s) succeed, total checkpoint size(%" PRId64
           "), file count(%d)",
           name(),
           local_backup_metada_file.c_str(),
           _chkpt_total_size,
           backup_metadata.files.size());

    for (const auto &f_meta : backup_metadata.files) {
        std::string remote_file = utils::filesystem::path_combine(remote_chkpt_dir, f_meta.name);
        fs->create_file(create_file_request{remote_file, false},
                        TASK_CODE_EXEC_INLINED,
                        std::bind(create_file_callback_func, std::placeholders::_1, f_meta.name),
                        &tracker);
    }
    tracker.wait_outstanding_tasks();

    if (err == ERR_OK) {
        if (!verify_checkpoint(backup_metadata, local_chkpt_dir)) {
            derror("%s: checkpoint is damaged, chkpt = %s", name(), local_chkpt_dir.c_str());
            // if checkpoint is damaged, using corruption to represent it
            err = ERR_CORRUPTION;
        } else {
            ddebug("%s: checkpoint is valid, chkpt = %s", name(), local_chkpt_dir.c_str());
            // checkpoint is valid, we should delete the backup_metadata under checkpoint
            std::string metadata_file = ::dsn::utils::filesystem::path_combine(
                local_chkpt_dir, cold_backup_constant::BACKUP_METADATA);
            if (!::dsn::utils::filesystem::remove_path(metadata_file)) {
                dwarn(
                    "%s: remove backup_metadata failed, file = %s", name(), metadata_file.c_str());
            } else {
                ddebug(
                    "%s: remove backup_metadata succeed, file = %s", name(), metadata_file.c_str());
            }
        }
    }
    return err;
}

dsn::error_code replica::find_valid_checkpoint(const configuration_restore_request &req,
                                               std::string &remote_chkpt_dir)
{
    const std::string &backup_root = req.cluster_name;
    const std::string &policy_name = req.policy_name;
    const int64_t &backup_id = req.time_stamp;
    ddebug("%s: retore from policy_name(%s), backup_id(%lld)",
           name(),
           req.policy_name.c_str(),
           req.time_stamp);

    // we should base on old gpid to combine the path on cold backup media
    dsn::gpid old_gpid;
    old_gpid.set_app_id(req.app_id);
    old_gpid.set_partition_index(_config.pid.get_partition_index());

    std::string manifest_file = cold_backup::get_current_chkpt_file(
        backup_root, policy_name, req.app_name, old_gpid, backup_id);
    block_filesystem *fs =
        _stub->_block_service_manager.get_block_filesystem(req.backup_provider_name);
    dassert(fs,
            "%s: get block filesystem by provider(%s) failed",
            name(),
            req.backup_provider_name.c_str());

    create_file_response create_response;
    fs->create_file(
          create_file_request{manifest_file, false},
          TASK_CODE_EXEC_INLINED,
          [&create_response](const create_file_response &resp) { create_response = resp; },
          nullptr)
        ->wait();

    if (create_response.err != dsn::ERR_OK) {
        derror("%s: create file of block_service failed, reason(%s)",
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
        derror("%s: read file %s failed, reason(%s)",
               name(),
               create_response.file_handle->file_name().c_str(),
               r.err.to_string());
        return r.err;
    }

    std::string valid_chkpt_entry(r.buffer.data(), r.buffer.length());
    ddebug("%s: get a valid chkpt(%s)", name(), valid_chkpt_entry.c_str());
    remote_chkpt_dir = ::dsn::utils::filesystem::path_combine(
        cold_backup::get_replica_backup_path(
            backup_root, policy_name, req.app_name, old_gpid, backup_id),
        valid_chkpt_entry);
    return dsn::ERR_OK;
}

dsn::error_code replica::restore_checkpoint()
{
    // first check the parameter
    configuration_restore_request restore_req;
    auto iter = _app_info.envs.find(backup_restore_constant::BLOCK_SERVICE_PROVIDER);
    dassert(iter != _app_info.envs.end(),
            "%s: can't find %s in app_info.envs",
            name(),
            backup_restore_constant::BLOCK_SERVICE_PROVIDER.c_str());
    restore_req.backup_provider_name = iter->second;
    iter = _app_info.envs.find(backup_restore_constant::CLUSTER_NAME);
    dassert(iter != _app_info.envs.end(),
            "%s: can't find %s in app_info.envs",
            name(),
            backup_restore_constant::CLUSTER_NAME.c_str());
    restore_req.cluster_name = iter->second;
    iter = _app_info.envs.find(backup_restore_constant::POLICY_NAME);
    dassert(iter != _app_info.envs.end(),
            "%s: can't find %s in app_info.envs",
            name(),
            backup_restore_constant::POLICY_NAME.c_str());
    restore_req.policy_name = iter->second;
    iter = _app_info.envs.find(backup_restore_constant::APP_NAME);
    dassert(iter != _app_info.envs.end(),
            "%s: can't find %s in app_info.envs",
            name(),
            backup_restore_constant::APP_NAME.c_str());
    restore_req.app_name = iter->second;
    iter = _app_info.envs.find(backup_restore_constant::APP_ID);
    dassert(iter != _app_info.envs.end(),
            "%s: can't find %s in app_info.envs",
            name(),
            backup_restore_constant::APP_ID.c_str());
    restore_req.app_id = boost::lexical_cast<int32_t>(iter->second);

    iter = _app_info.envs.find(backup_restore_constant::BACKUP_ID);
    dassert(iter != _app_info.envs.end(),
            "%s: can't find %s in app_info.envs",
            name(),
            backup_restore_constant::BACKUP_ID.c_str());
    restore_req.time_stamp = boost::lexical_cast<int64_t>(iter->second);

    bool skip_bad_partition = false;
    if (_app_info.envs.find(backup_restore_constant::SKIP_BAD_PARTITION) != _app_info.envs.end()) {
        skip_bad_partition = true;
    }

    // then create a local restore dir
    std::ostringstream os;
    os << _dir << "/restore." << restore_req.policy_name << "." << restore_req.time_stamp;
    std::string restore_dir = os.str();
    if (!utils::filesystem::directory_exists(_dir) && !utils::filesystem::create_directory(_dir)) {
        derror("create dir %s failed", _dir.c_str());
        return dsn::ERR_FILE_OPERATION_FAILED;
    }
    // we don't remove the old restore.policy_name.backup_id
    if (!utils::filesystem::directory_exists(restore_dir) &&
        !utils::filesystem::create_directory(restore_dir)) {
        derror("create dir %s failed", restore_dir.c_str());
        return dsn::ERR_FILE_OPERATION_FAILED;
    }

    // then find a valid checkpoint dir to copy
    std::string remote_chkpt_dir;
    dsn::error_code err = find_valid_checkpoint(restore_req, remote_chkpt_dir);

    if (err == dsn::ERR_OK) {
        err = download_checkpoint(restore_req, remote_chkpt_dir, restore_dir);
        if (err != ERR_OK) {
            if (_restore_status == ERR_CORRUPTION) {
                if (skip_bad_partition) {
                    err = skip_restore_partition(restore_dir);
                } else {
                    tell_meta_to_restore_rollback();
                    return ERR_CORRUPTION;
                }
            }
        }
    } else { // find valid checkpoint failed
        if (err == ERR_OBJECT_NOT_FOUND) {
            if (skip_bad_partition) {
                err = skip_restore_partition(restore_dir);
            } else {
                // current_checkpoint doesn't exist, we think partition is damaged
                tell_meta_to_restore_rollback();
                _restore_status = ERR_CORRUPTION;
                return ERR_CORRUPTION;
            }
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
        ddebug("%s: clear restore_dir(%s) succeed", name(), restore_dir.c_str());
        _restore_status = ERR_IGNORE_BAD_DATA;
        _restore_progress.store(cold_backup_constant::PROGRESS_FINISHED);
        return ERR_OK;
    } else {
        derror("clear dir %s failed", restore_dir.c_str());
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
                          ddebug("restore rolling backup succeed");
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
              [](error_code err, dsn::message_ex *request, dsn::message_ex *resp) {
                  if (err == ERR_OK) {
                      configuration_report_restore_status_response response;
                      ::dsn::unmarshall(resp, response);
                      if (response.err == ERR_OK) {
                          dinfo("report restore status succeed");
                          return;
                      }
                  } else if (err == ERR_TIMEOUT) {
                      // TODO: we should retry to make the result more precisely
                      // report_restore_status_to_meta();
                  }
              });
}

void replica::update_restore_progress()
{
    if (_chkpt_total_size <= 0) {
        // have not be initialized, just return 0
        return;
    }
    auto total_size = static_cast<double>(_chkpt_total_size);
    auto cur_download_size = static_cast<double>(_cur_download_size.load());
    _restore_progress.store(static_cast<int32_t>((cur_download_size / total_size) * 1000));
}
}
}
