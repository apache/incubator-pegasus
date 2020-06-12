#include <fstream>
#include <boost/lexical_cast.hpp>

#include <dsn/utility/error_code.h>
#include <dsn/utility/factory_store.h>
#include <dsn/utility/filesystem.h>
#include <dsn/utility/utils.h>

#include <dsn/dist/replication/replication_app_base.h>
#include <dsn/dist/fmt_logging.h>

#include "replica.h"
#include "mutation_log.h"
#include "replica_stub.h"
#include "dist/block_service/block_service_manager.h"

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

error_code replica::download_checkpoint(const configuration_restore_request &req,
                                        const std::string &remote_chkpt_dir,
                                        const std::string &local_chkpt_dir)
{
    block_filesystem *fs =
        _stub->_block_service_manager.get_block_filesystem(req.backup_provider_name);

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
                if (download_err == ERR_OK &&
                    !utils::filesystem::verify_file(file_name, f_meta.md5, f_meta.size)) {
                    download_err = ERR_CORRUPTION;
                }

                if (download_err != ERR_OK) {
                    derror_replica(
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
    if (err != ERR_OK) {
        derror_replica("download backup_metadata failed, file({}), reason({})",
                       utils::filesystem::path_combine(remote_chkpt_dir,
                                                       cold_backup_constant::BACKUP_METADATA),
                       err);
        return err;
    }

    // parse cold_backup_meta from metadata file
    const std::string local_backup_metada_file =
        utils::filesystem::path_combine(local_chkpt_dir, cold_backup_constant::BACKUP_METADATA);
    if (!read_cold_backup_metadata(local_backup_metada_file, backup_metadata)) {
        derror_replica("read cold_backup_metadata from file({}) failed", local_backup_metada_file);
        return ERR_FILE_OPERATION_FAILED;
    }

    _chkpt_total_size = backup_metadata.checkpoint_total_size;
    ddebug_replica(
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
        dwarn_replica("remove useless file failed, chkpt = {}", local_chkpt_dir);
    } else {
        ddebug_replica("remove useless file succeed, chkpt = {}", local_chkpt_dir);
    }

    const std::string metadata_file =
        utils::filesystem::path_combine(local_chkpt_dir, cold_backup_constant::BACKUP_METADATA);
    if (!utils::filesystem::remove_path(metadata_file)) {
        dwarn_replica("remove backup_metadata failed, file = {}", metadata_file);
    } else {
        ddebug_replica("remove backup_metadata succeed, file = {}", metadata_file);
    }
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

    // then create a local restore dir if it doesn't exist
    if (!utils::filesystem::directory_exists(_dir) && !utils::filesystem::create_directory(_dir)) {
        derror("create dir %s failed", _dir.c_str());
        return ERR_FILE_OPERATION_FAILED;
    }

    // we don't remove the old restore.policy_name.backup_id
    std::ostringstream os;
    os << _dir << "/restore." << restore_req.policy_name << "." << restore_req.time_stamp;
    std::string restore_dir = os.str();
    if (!utils::filesystem::directory_exists(restore_dir) &&
        !utils::filesystem::create_directory(restore_dir)) {
        derror("create dir %s failed", restore_dir.c_str());
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
        ddebug("%s: clear restore_dir(%s) succeed", name(), restore_dir.c_str());
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

void replica::update_restore_progress(uint64_t f_size)
{
    if (_chkpt_total_size <= 0) {
        derror_replica("cold_backup_metadata has invalid file_total_size({})", _chkpt_total_size);
        return;
    }

    _cur_download_size.fetch_add(f_size);
    auto total_size = static_cast<double>(_chkpt_total_size);
    auto cur_download_size = static_cast<double>(_cur_download_size.load());
    auto cur_porgress = static_cast<int32_t>((cur_download_size / total_size) * 1000);
    _restore_progress.store(cur_porgress);
    ddebug_replica("total_size = {}, cur_downloaded_size = {}, progress = {}",
                   total_size,
                   cur_download_size,
                   cur_porgress);
}
}
}
