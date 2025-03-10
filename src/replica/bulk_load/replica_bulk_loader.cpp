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

#include <fmt/core.h>
#include <algorithm>
#include <functional>
#include <memory>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "block_service/block_service_manager.h"
#include "common/bulk_load_common.h"
#include "common/gpid.h"
#include "common/replication.codes.h"
#include "common/replication_common.h"
#include "common/replication_enums.h"
#include "dsn.layer2_types.h"
#include "replica/disk_cleaner.h"
#include "replica/mutation.h"
#include "replica/replica_context.h"
#include "replica/replica_stub.h"
#include "replica/replication_app_base.h"
#include "replica_bulk_loader.h"
#include "rpc/dns_resolver.h"
#include "rpc/rpc_address.h"
#include "rpc/rpc_holder.h"
#include "rpc/rpc_host_port.h"
#include "task/async_calls.h"
#include "utils/autoref_ptr.h"
#include "utils/chrono_literals.h"
#include "utils/env.h"
#include "utils/fail_point.h"
#include "utils/filesystem.h"
#include "utils/flags.h"
#include "utils/fmt_logging.h"
#include "utils/load_dump_object.h"
#include "utils/thread_access_checker.h"

METRIC_DEFINE_counter(replica,
                      bulk_load_downloading_count,
                      dsn::metric_unit::kBulkLoads,
                      "The number of downloading bulk loads");

METRIC_DEFINE_counter(replica,
                      bulk_load_ingesting_count,
                      dsn::metric_unit::kBulkLoads,
                      "The number of ingesting bulk loads");

METRIC_DEFINE_counter(replica,
                      bulk_load_successful_count,
                      dsn::metric_unit::kBulkLoads,
                      "The number of successful bulk loads");

METRIC_DEFINE_counter(replica,
                      bulk_load_failed_count,
                      dsn::metric_unit::kBulkLoads,
                      "The number of failed bulk loads");

METRIC_DEFINE_counter(replica,
                      bulk_load_download_file_successful_count,
                      dsn::metric_unit::kFiles,
                      "The number of files that have been downloaded successfully for bulk loads");

METRIC_DEFINE_counter(replica,
                      bulk_load_download_file_failed_count,
                      dsn::metric_unit::kFiles,
                      "The number of files that have failed to be downloaded for bulk loads");

METRIC_DEFINE_counter(replica,
                      bulk_load_download_file_bytes,
                      dsn::metric_unit::kBytes,
                      "The size of files that have been downloaded successfully for bulk loads");

DSN_DECLARE_int32(max_concurrent_bulk_load_downloading_count);

namespace dsn {
namespace dist {
namespace block_service {
class block_filesystem;
} // namespace block_service
} // namespace dist

namespace replication {

replica_bulk_loader::replica_bulk_loader(replica *r)
    : replica_base(r),
      _replica(r),
      _stub(r->get_replica_stub()),
      METRIC_VAR_INIT_replica(bulk_load_downloading_count),
      METRIC_VAR_INIT_replica(bulk_load_ingesting_count),
      METRIC_VAR_INIT_replica(bulk_load_successful_count),
      METRIC_VAR_INIT_replica(bulk_load_failed_count),
      METRIC_VAR_INIT_replica(bulk_load_download_file_successful_count),
      METRIC_VAR_INIT_replica(bulk_load_download_file_failed_count),
      METRIC_VAR_INIT_replica(bulk_load_download_file_bytes)
{
}

replica_bulk_loader::~replica_bulk_loader() {}

// ThreadPool: THREAD_POOL_REPLICATION
void replica_bulk_loader::on_bulk_load(const bulk_load_request &request,
                                       /*out*/ bulk_load_response &response)
{
    _replica->_checker.only_one_thread_access();

    response.pid = request.pid;
    response.app_name = request.app_name;
    response.err = ERR_OK;

    if (status() != partition_status::PS_PRIMARY) {
        LOG_WARNING_PREFIX("receive bulk load request with wrong status {}",
                           enum_to_string(status()));
        response.err = ERR_INVALID_STATE;
        return;
    }

    if (request.ballot != get_ballot()) {
        LOG_WARNING_PREFIX(
            "receive bulk load request with wrong version, remote ballot={}, local ballot={}",
            request.ballot,
            get_ballot());
        response.err = ERR_INVALID_STATE;
        return;
    }

    LOG_INFO_PREFIX("receive bulk load request, remote provider = {}, remote_root_path = {}, "
                    "cluster_name = {}, app_name = {}, "
                    "meta_bulk_load_status = {}, local bulk_load_status = {}",
                    request.remote_provider_name,
                    request.remote_root_path,
                    request.cluster_name,
                    request.app_name,
                    enum_to_string(request.meta_bulk_load_status),
                    enum_to_string(_status));

    error_code ec = do_bulk_load(request.app_name,
                                 request.meta_bulk_load_status,
                                 request.cluster_name,
                                 request.remote_provider_name,
                                 request.remote_root_path);
    if (ec != ERR_OK) {
        response.err = ec;
        response.primary_bulk_load_status = _status;
        return;
    }

    report_bulk_load_states_to_meta(
        request.meta_bulk_load_status, request.query_bulk_load_metadata, response);
    if (response.err != ERR_OK) {
        return;
    }

    broadcast_group_bulk_load(request);
}

// ThreadPool: THREAD_POOL_REPLICATION
void replica_bulk_loader::broadcast_group_bulk_load(const bulk_load_request &meta_req)
{
    if (!_replica->_primary_states.learners.empty()) {
        LOG_WARNING_PREFIX("has learners, skip broadcast group bulk load request");
        return;
    }

    if (!_replica->_primary_states.group_bulk_load_pending_replies.empty()) {
        LOG_WARNING_PREFIX("{} group bulk_load replies are still pending, cancel it firstly",
                           _replica->_primary_states.group_bulk_load_pending_replies.size());
        for (auto &kv : _replica->_primary_states.group_bulk_load_pending_replies) {
            CLEANUP_TASK_ALWAYS(kv.second);
        }
        _replica->_primary_states.group_bulk_load_pending_replies.clear();
    }

    LOG_INFO_PREFIX("start to broadcast group bulk load");

    for (const auto &secondary : _replica->_primary_states.pc.hp_secondaries) {
        if (secondary == _stub->primary_host_port()) {
            continue;
        }

        auto request = std::make_unique<group_bulk_load_request>();
        request->app_name = _replica->_app_info.app_name;
        const auto &addr = dsn::dns_resolver::instance().resolve_address(secondary);
        SET_IP_AND_HOST_PORT(*request, target, addr, secondary);
        _replica->_primary_states.get_replica_config(partition_status::PS_SECONDARY,
                                                     request->config);
        request->cluster_name = meta_req.cluster_name;
        request->provider_name = meta_req.remote_provider_name;
        request->meta_bulk_load_status = meta_req.meta_bulk_load_status;
        request->remote_root_path = meta_req.remote_root_path;

        LOG_INFO_PREFIX("send group_bulk_load_request to {}({})", secondary, addr);

        group_bulk_load_rpc rpc(
            std::move(request), RPC_GROUP_BULK_LOAD, 0_ms, 0, get_gpid().thread_hash());
        auto callback_task = rpc.call(addr, tracker(), [this, rpc](error_code err) mutable {
            on_group_bulk_load_reply(err, rpc.request(), rpc.response());
        });
        _replica->_primary_states.group_bulk_load_pending_replies[secondary] = callback_task;
    }
}

// ThreadPool: THREAD_POOL_REPLICATION
void replica_bulk_loader::on_group_bulk_load(const group_bulk_load_request &request,
                                             /*out*/ group_bulk_load_response &response)
{
    _replica->_checker.only_one_thread_access();

    response.err = ERR_OK;

    if (request.config.ballot < get_ballot()) {
        response.err = ERR_VERSION_OUTDATED;
        LOG_WARNING_PREFIX(
            "receive outdated group_bulk_load request, request ballot({}) VS local ballot({})",
            request.config.ballot,
            get_ballot());
        return;
    }
    if (request.config.ballot > get_ballot()) {
        response.err = ERR_INVALID_STATE;
        LOG_WARNING_PREFIX("receive group_bulk_load request, local ballot is outdated, request "
                           "ballot({}) VS local ballot({})",
                           request.config.ballot,
                           get_ballot());
        return;
    }
    if (status() != request.config.status) {
        response.err = ERR_INVALID_STATE;
        LOG_WARNING_PREFIX("status changed, status should be {}, but {}",
                           enum_to_string(request.config.status),
                           enum_to_string(status()));
        return;
    }

    LOG_INFO_PREFIX("receive group_bulk_load request, primary address = {}, ballot = {}, "
                    "meta bulk_load_status = {}, local bulk_load_status = {}",
                    FMT_HOST_PORT_AND_IP(request.config, primary),
                    request.config.ballot,
                    enum_to_string(request.meta_bulk_load_status),
                    enum_to_string(_status));

    error_code ec = do_bulk_load(request.app_name,
                                 request.meta_bulk_load_status,
                                 request.cluster_name,
                                 request.provider_name,
                                 request.remote_root_path);
    if (ec != ERR_OK) {
        response.err = ec;
        response.status = _status;
        return;
    }

    report_bulk_load_states_to_primary(request.meta_bulk_load_status, response);
}

// ThreadPool: THREAD_POOL_REPLICATION
void replica_bulk_loader::on_group_bulk_load_reply(error_code err,
                                                   const group_bulk_load_request &req,
                                                   const group_bulk_load_response &resp)
{
    _replica->_checker.only_one_thread_access();

    if (partition_status::PS_PRIMARY != status()) {
        LOG_ERROR_PREFIX("replica status={}, should be {}",
                         enum_to_string(status()),
                         enum_to_string(partition_status::PS_PRIMARY));
        return;
    }

    _replica->_primary_states.group_bulk_load_pending_replies.erase(req.hp_target);

    if (err != ERR_OK) {
        LOG_ERROR_PREFIX("failed to receive group_bulk_load_reply from {}, error = {}",
                         FMT_HOST_PORT_AND_IP(req, target),
                         err);
        _replica->_primary_states.reset_node_bulk_load_states(req.hp_target);
        return;
    }

    if (resp.err != ERR_OK) {
        LOG_ERROR_PREFIX("receive group_bulk_load response from {} failed, error = {}",
                         FMT_HOST_PORT_AND_IP(req, target),
                         resp.err);
        _replica->_primary_states.reset_node_bulk_load_states(req.hp_target);
        return;
    }

    if (req.config.ballot != get_ballot()) {
        LOG_ERROR_PREFIX("recevied wrong group_bulk_load response from {}, request ballot = {}, "
                         "current ballot = {}",
                         FMT_HOST_PORT_AND_IP(req, target),
                         req.config.ballot,
                         get_ballot());
        _replica->_primary_states.reset_node_bulk_load_states(req.hp_target);
        return;
    }

    _replica->_primary_states.secondary_bulk_load_states[req.hp_target] = resp.bulk_load_state;
}

// ThreadPool: THREAD_POOL_REPLICATION
error_code replica_bulk_loader::do_bulk_load(const std::string &app_name,
                                             bulk_load_status::type meta_status,
                                             const std::string &cluster_name,
                                             const std::string &provider_name,
                                             const std::string &remote_root_path)
{
    if (status() != partition_status::PS_PRIMARY && status() != partition_status::PS_SECONDARY) {
        return ERR_INVALID_STATE;
    }

    bulk_load_status::type local_status = _status;
    error_code ec = validate_status(meta_status, local_status);
    if (ec != ERR_OK) {
        LOG_ERROR_PREFIX("invalid bulk load status, remote = {}, local = {}",
                         enum_to_string(meta_status),
                         enum_to_string(local_status));
        return ec;
    }

    switch (meta_status) {
    case bulk_load_status::BLS_DOWNLOADING:
        if (local_status == bulk_load_status::BLS_INVALID ||
            local_status == bulk_load_status::BLS_PAUSED ||
            local_status == bulk_load_status::BLS_INGESTING ||
            local_status == bulk_load_status::BLS_SUCCEED) {
            const std::string remote_dir = get_remote_bulk_load_dir(
                app_name, cluster_name, remote_root_path, get_gpid().get_partition_index());
            ec = start_download(remote_dir, provider_name);
        }
        break;
    case bulk_load_status::BLS_INGESTING:
        if (local_status == bulk_load_status::BLS_DOWNLOADED) {
            start_ingestion();
        } else if (local_status == bulk_load_status::BLS_INGESTING &&
                   status() == partition_status::PS_PRIMARY) {
            check_ingestion_finish();
        }
        break;
    case bulk_load_status::BLS_SUCCEED:
        if (local_status == bulk_load_status::BLS_DOWNLOADED ||
            local_status == bulk_load_status::BLS_INGESTING) {
            handle_bulk_load_succeed();
        } else if (local_status == bulk_load_status::BLS_SUCCEED ||
                   local_status == bulk_load_status::BLS_INVALID) {
            handle_bulk_load_finish(meta_status);
        }
        break;
    case bulk_load_status::BLS_PAUSING:
        pause_bulk_load();
        break;
    case bulk_load_status::BLS_CANCELED:
        handle_bulk_load_finish(bulk_load_status::BLS_CANCELED);
        break;
    case bulk_load_status::BLS_FAILED:
        handle_bulk_load_finish(bulk_load_status::BLS_FAILED);
        METRIC_VAR_INCREMENT(bulk_load_failed_count);
        break;
    default:
        break;
    }
    return ec;
}

/*static*/ error_code
replica_bulk_loader::validate_status(const bulk_load_status::type meta_status,
                                     const bulk_load_status::type local_status)
{
    error_code err = ERR_OK;
    switch (meta_status) {
    case bulk_load_status::BLS_DOWNLOADING:
        if (local_status == bulk_load_status::BLS_FAILED ||
            local_status == bulk_load_status::BLS_PAUSING ||
            local_status == bulk_load_status::BLS_CANCELED) {
            err = ERR_INVALID_STATE;
        }
        break;
    case bulk_load_status::BLS_DOWNLOADED:
        if (local_status != bulk_load_status::BLS_DOWNLOADED) {
            err = ERR_INVALID_STATE;
        }
        break;
    case bulk_load_status::BLS_INGESTING:
        if (local_status != bulk_load_status::BLS_DOWNLOADED &&
            local_status != bulk_load_status::BLS_INGESTING) {
            err = ERR_INVALID_STATE;
        }
        break;
    case bulk_load_status::BLS_SUCCEED:
        if (local_status != bulk_load_status::BLS_DOWNLOADED &&
            local_status != bulk_load_status::BLS_INGESTING &&
            local_status != bulk_load_status::BLS_SUCCEED &&
            local_status != bulk_load_status::BLS_INVALID) {
            err = ERR_INVALID_STATE;
        }
        break;
    case bulk_load_status::BLS_PAUSING:
        if (local_status != bulk_load_status::BLS_INVALID &&
            local_status != bulk_load_status::BLS_DOWNLOADING &&
            local_status != bulk_load_status::BLS_DOWNLOADED &&
            local_status != bulk_load_status::BLS_PAUSING &&
            local_status != bulk_load_status::BLS_PAUSED) {
            err = ERR_INVALID_STATE;
        }
        break;
    case bulk_load_status::BLS_PAUSED:
        err = ERR_INVALID_STATE;
        break;
    default:
        // no limit in other status
        break;
    }
    return err;
}

// ThreadPool: THREAD_POOL_REPLICATION
error_code replica_bulk_loader::start_download(const std::string &remote_dir,
                                               const std::string &provider_name)
{
    if (_stub->_bulk_load_downloading_count.load() >=
        FLAGS_max_concurrent_bulk_load_downloading_count) {
        LOG_WARNING_PREFIX("node[{}] already has {} replica downloading, wait for next round",
                           _stub->_primary_host_port_cache,
                           _stub->_bulk_load_downloading_count.load());
        return ERR_BUSY;
    }

    // reset local bulk load context and state
    if (_status == bulk_load_status::BLS_INVALID) {
        // try to remove possible garbage bulk load data when actually starting bulk load
        remove_local_bulk_load_dir(utils::filesystem::path_combine(
            _replica->_dir, bulk_load_constant::BULK_LOAD_LOCAL_ROOT_DIR));
    }
    if (status() == partition_status::PS_PRIMARY) {
        _replica->_primary_states.cleanup_bulk_load_states();
    }
    clear_bulk_load_states();

    _status = bulk_load_status::BLS_DOWNLOADING;
    ++_stub->_bulk_load_downloading_count;
    LOG_INFO_PREFIX("node[{}] has {} replica executing downloading",
                    _stub->_primary_host_port_cache,
                    _stub->_bulk_load_downloading_count.load());
    _bulk_load_start_time_ms = dsn_now_ms();
    METRIC_VAR_INCREMENT(bulk_load_downloading_count);

    // create local bulk load dir
    if (!utils::filesystem::directory_exists(_replica->_dir)) {
        LOG_ERROR_PREFIX("_dir({}) is not existed", _replica->_dir);
        return ERR_FILE_OPERATION_FAILED;
    }
    const std::string local_dir = utils::filesystem::path_combine(
        _replica->_dir, bulk_load_constant::BULK_LOAD_LOCAL_ROOT_DIR);
    if (!utils::filesystem::directory_exists(local_dir) &&
        !utils::filesystem::create_directory(local_dir)) {
        LOG_ERROR_PREFIX("create bulk_load_dir({}) failed", local_dir);
        return ERR_FILE_OPERATION_FAILED;
    }

    // start download
    _is_downloading.store(true);
    _download_task = tasking::enqueue(
        LPC_BACKGROUND_BULK_LOAD,
        tracker(),
        std::bind(
            &replica_bulk_loader::download_files, this, provider_name, remote_dir, local_dir));
    return ERR_OK;
}

// ThreadPool: THREAD_POOL_DEFAULT
void replica_bulk_loader::download_files(const std::string &provider_name,
                                         const std::string &remote_dir,
                                         const std::string &local_dir)
{
    FAIL_POINT_INJECT_F("replica_bulk_loader_download_files", [](std::string_view) {});

    LOG_INFO_PREFIX("start to download files");
    dist::block_service::block_filesystem *fs =
        _stub->_block_service_manager.get_or_create_block_filesystem(provider_name);

    // download metadata file synchronously
    uint64_t file_size = 0;
    error_code err = _stub->_block_service_manager.download_file(
        remote_dir, local_dir, bulk_load_constant::BULK_LOAD_METADATA, fs, file_size);
    {
        zauto_write_lock l(_lock);
        if (err != ERR_OK && err != ERR_PATH_ALREADY_EXIST) {
            try_decrease_bulk_load_download_count();
            _download_status.store(err);
            LOG_ERROR_PREFIX("download bulk load metadata file failed, error = {}", err);
            return;
        }

        // parse metadata
        const std::string &local_metadata_file_name =
            utils::filesystem::path_combine(local_dir, bulk_load_constant::BULK_LOAD_METADATA);
        err = parse_bulk_load_metadata(local_metadata_file_name);
        if (err != ERR_OK) {
            try_decrease_bulk_load_download_count();
            _download_status.store(err);
            LOG_ERROR_PREFIX("parse bulk load metadata failed, error = {}", err);
            return;
        }
    }

    // download sst files asynchronously
    {
        zauto_read_lock l(_lock);
        if (!_metadata.files.empty()) {
            _download_files_task[_metadata.files.back().name] = tasking::enqueue(
                LPC_BACKGROUND_BULK_LOAD,
                tracker(),
                [this, remote_dir, local_dir, file_meta = _metadata.files, fs]() mutable {
                    this->download_sst_file(remote_dir, local_dir, std::move(file_meta), fs);
                });
        }
    }
}

// ThreadPool: THREAD_POOL_DEFAULT
void replica_bulk_loader::download_sst_file(
    const std::string &remote_dir,
    const std::string &local_dir,
    std::vector<::dsn::replication::file_meta> &&download_file_metas,
    dist::block_service::block_filesystem *fs)
{
    if (_status != bulk_load_status::BLS_DOWNLOADING) {
        LOG_WARNING_PREFIX("Cancel download_sst_file task, because bulk_load local_status is {}.",
                           enum_to_string(_status));
        return;
    }
    const file_meta &f_meta = download_file_metas.back();
    uint64_t f_size = 0;
    std::string f_md5;
    error_code ec = _stub->_block_service_manager.download_file(
        remote_dir, local_dir, f_meta.name, fs, f_size, f_md5);
    const std::string &file_name = utils::filesystem::path_combine(local_dir, f_meta.name);
    bool verified = false;
    if (ec == ERR_PATH_ALREADY_EXIST) {
        // We are not sure if the file was cached by system. And we couldn't
        // afford the io overhead which is cased by reading file in verify_file(),
        // so if file exist we just verify file size
        if (utils::filesystem::verify_file_size(
                file_name, utils::FileDataType::kSensitive, f_meta.size)) {
            // local file exist and is verified
            ec = ERR_OK;
            f_size = f_meta.size;
            verified = true;
        } else {
            LOG_ERROR_PREFIX("file({}) exists, but not verified, try to remove local file "
                             "and redownload it",
                             file_name);
            if (!utils::filesystem::remove_path(file_name)) {
                LOG_ERROR_PREFIX("failed to remove file({})", file_name);
                ec = ERR_FILE_OPERATION_FAILED;
            } else {
                ec = _stub->_block_service_manager.download_file(
                    remote_dir, local_dir, f_meta.name, fs, f_size, f_md5);
            }
        }
    }
    // Here we verify md5 and file size, md5 was calculated
    // from download buffer, file size is get from filesystem
    if (ec == ERR_OK && !verified) {
        if (!f_meta.md5.empty() && f_md5 != f_meta.md5) {
            ec = ERR_CORRUPTION;
        } else if (!utils::filesystem::verify_file_size(
                       file_name, utils::FileDataType::kSensitive, f_meta.size)) {
            ec = ERR_CORRUPTION;
        }
    }
    if (ec != ERR_OK) {
        {
            zauto_write_lock l(_lock);
            try_decrease_bulk_load_download_count();
            _download_status.store(ec);
        }
        LOG_ERROR_PREFIX("failed to download file({}), error = {}", f_meta.name, ec);
        METRIC_VAR_INCREMENT(bulk_load_download_file_failed_count);
        return;
    }
    // download file succeed, update progress
    update_bulk_load_download_progress(f_size, f_meta.name);
    METRIC_VAR_INCREMENT(bulk_load_download_file_successful_count);
    METRIC_VAR_INCREMENT_BY(bulk_load_download_file_bytes, f_size);

    // download next file
    download_file_metas.pop_back();
    if (!download_file_metas.empty()) {
        _download_files_task[download_file_metas.back().name] = tasking::enqueue(
            LPC_BACKGROUND_BULK_LOAD,
            tracker(),
            [this, remote_dir, local_dir, download_file_metas, fs]() mutable {
                this->download_sst_file(remote_dir, local_dir, std::move(download_file_metas), fs);
            });
    }
}

// ThreadPool: THREAD_POOL_DEFAULT
// need to acquire write lock while calling it
error_code replica_bulk_loader::parse_bulk_load_metadata(const std::string &fname)
{
    auto ec = dsn::utils::load_rjobj_from_file(fname, &_metadata);
    if (ec != ERR_OK) {
        LOG_ERROR_PREFIX("load bulk_load_metadata from file {} failed", fname);
        return ec;
    }

    if (_metadata.file_total_size <= 0) {
        LOG_ERROR_PREFIX("bulk_load_metadata has invalid file_total_size({})",
                         _metadata.file_total_size);
        return ERR_CORRUPTION;
    }

    return ERR_OK;
}

// ThreadPool: THREAD_POOL_DEFAULT
void replica_bulk_loader::update_bulk_load_download_progress(uint64_t file_size,
                                                             const std::string &file_name)
{
    {
        zauto_write_lock l(_lock);
        if (_metadata.file_total_size <= 0) {
            LOG_ERROR_PREFIX("update downloading file({}) progress failed, metadata has invalid "
                             "file_total_size({}), current status = {}",
                             file_name,
                             _metadata.file_total_size,
                             enum_to_string(_status));
            return;
        }

        LOG_INFO_PREFIX("update progress after downloading file({})", file_name);
        _cur_downloaded_size.fetch_add(file_size);
        auto total_size = static_cast<double>(_metadata.file_total_size);
        auto cur_downloaded_size = static_cast<double>(_cur_downloaded_size.load());
        auto cur_progress = static_cast<int32_t>((cur_downloaded_size / total_size) * 100);
        _download_progress.store(cur_progress);
        LOG_INFO_PREFIX("total_size = {}, cur_downloaded_size = {}, progress = {}",
                        total_size,
                        cur_downloaded_size,
                        cur_progress);
    }

    tasking::enqueue(LPC_REPLICATION_COMMON,
                     tracker(),
                     std::bind(&replica_bulk_loader::check_download_finish, this),
                     get_gpid().thread_hash());
}

// ThreadPool: THREAD_POOL_REPLICATION, THREAD_POOL_DEFAULT
// need to acquire write lock while calling it
void replica_bulk_loader::try_decrease_bulk_load_download_count()
{
    if (!_is_downloading.load()) {
        return;
    }
    --_stub->_bulk_load_downloading_count;
    _is_downloading.store(false);
    LOG_INFO_PREFIX("node[{}] has {} replica executing downloading",
                    _stub->_primary_host_port_cache,
                    _stub->_bulk_load_downloading_count.load());
}

// ThreadPool: THREAD_POOL_REPLICATION
void replica_bulk_loader::check_download_finish()
{
    if (_download_progress.load() == bulk_load_constant::PROGRESS_FINISHED &&
        _status == bulk_load_status::BLS_DOWNLOADING) {
        LOG_INFO_PREFIX("download all files succeed");
        _status = bulk_load_status::BLS_DOWNLOADED;
        {
            zauto_write_lock l(_lock);
            try_decrease_bulk_load_download_count();
            cleanup_download_tasks();
        }
    }
}

// ThreadPool: THREAD_POOL_REPLICATION
void replica_bulk_loader::start_ingestion()
{
    _status = bulk_load_status::BLS_INGESTING;
    METRIC_VAR_INCREMENT(bulk_load_ingesting_count);
    if (status() == partition_status::PS_PRIMARY) {
        _replica->_primary_states.ingestion_is_empty_prepare_sent = false;
    }
}

// ThreadPool: THREAD_POOL_REPLICATION
void replica_bulk_loader::check_ingestion_finish()
{
    if (_replica->_app->get_ingestion_status() == ingestion_status::IS_SUCCEED &&
        !_replica->_primary_states.ingestion_is_empty_prepare_sent) {
        // send an empty prepare to gurantee secondary commit ingestion request, and set
        // `pop_all_committed_mutations` as true
        // ingestion is a special write request, replay this mutation can not learn data from
        // external files, so when ingestion succeed, we should create a checkpoint
        // if learn is evoked after ingestion, we should gurantee that learner should learn from
        // checkpoint, to gurantee the condition above, we should pop all committed mutations in
        // prepare list to gurantee learn type is LT_APP
        mutation_ptr mu = _replica->new_mutation(invalid_decree);
        mu->add_client_request(nullptr);
        _replica->init_prepare(mu, false, true);
        _replica->_primary_states.ingestion_is_empty_prepare_sent = true;
    }
}

// ThreadPool: THREAD_POOL_REPLICATION
void replica_bulk_loader::handle_bulk_load_succeed()
{
    // generate checkpoint
    _replica->init_checkpoint(true);

    _replica->_app->set_ingestion_status(ingestion_status::IS_INVALID);
    _status = bulk_load_status::BLS_SUCCEED;
    METRIC_VAR_INCREMENT(bulk_load_successful_count);

    // send an empty prepare again to gurantee that learner should learn from checkpoint
    if (status() == partition_status::PS_PRIMARY) {
        mutation_ptr mu = _replica->new_mutation(invalid_decree);
        mu->add_client_request(nullptr);
        _replica->init_prepare(mu, false, true);
    }
}

// ThreadPool: THREAD_POOL_REPLICATION
void replica_bulk_loader::handle_bulk_load_finish(bulk_load_status::type new_status)
{
    if (is_cleaned_up()) {
        LOG_INFO_PREFIX("bulk load states have been cleaned up");
        return;
    }

    if (status() == partition_status::PS_PRIMARY) {
        for (const auto &secondary : _replica->_primary_states.pc.hp_secondaries) {
            _replica->_primary_states.reset_node_bulk_load_states(secondary);
        }
    }

    LOG_INFO_PREFIX("bulk load finished, old_status = {}, new_status = {}",
                    enum_to_string(_status),
                    enum_to_string(new_status));

    // remove local bulk load dir
    std::string bulk_load_dir = utils::filesystem::path_combine(
        _replica->_dir, bulk_load_constant::BULK_LOAD_LOCAL_ROOT_DIR);
    remove_local_bulk_load_dir(bulk_load_dir);
    clear_bulk_load_states();
}

// ThreadPool: THREAD_POOL_REPLICATION
void replica_bulk_loader::remove_local_bulk_load_dir(const std::string &bulk_load_dir)
{
    if (!utils::filesystem::directory_exists(bulk_load_dir)) {
        return;
    }
    // Rename bulk_load_dir to ${replica_dir}.bulk_load.timestamp.gar before remove it.
    // Because we download sst files asynchronously and couldn't remove a directory while writing
    // files in it.
    std::string garbage_dir = fmt::format("{}.{}.{}{}",
                                          _replica->_dir,
                                          bulk_load_constant::BULK_LOAD_LOCAL_ROOT_DIR,
                                          std::to_string(dsn_now_ms()),
                                          kFolderSuffixGar);
    if (!utils::filesystem::rename_path(bulk_load_dir, garbage_dir)) {
        LOG_ERROR_PREFIX("rename bulk_load dir({}) failed.", bulk_load_dir);
        return;
    }
    if (!utils::filesystem::remove_path(garbage_dir)) {
        LOG_ERROR_PREFIX(
            "remove bulk_load gar dir({}) failed, disk cleaner would retry to remove it.",
            garbage_dir);
    }
    LOG_INFO_PREFIX("remove bulk_load dir({}) succeed.", garbage_dir);
}

// ThreadPool: THREAD_POOL_REPLICATION
// need to acquire write lock while calling it
void replica_bulk_loader::cleanup_download_tasks()
{
    for (auto &kv : _download_files_task) {
        cleanup_download_task(kv.second);
    }
    cleanup_download_task(_download_task);
}

// ThreadPool: THREAD_POOL_REPLICATION
bool replica_bulk_loader::cleanup_download_task(task_ptr task_)
{
    CLEANUP_TASK(task_, false)
    return true;
}

// ThreadPool: THREAD_POOL_REPLICATION
void replica_bulk_loader::clear_bulk_load_states()
{
    if (_status == bulk_load_status::BLS_DOWNLOADING) {
        try_decrease_bulk_load_download_count();
    }

    {
        zauto_write_lock l(_lock);
        cleanup_download_tasks();
        _download_files_task.clear();
        _download_task = nullptr;
        _metadata.files.clear();
        _metadata.file_total_size = 0;
        _cur_downloaded_size.store(0);
        _download_progress.store(0);
        _download_status.store(ERR_OK);
    }

    _replica->_is_bulk_load_ingestion = false;
    _replica->_app->set_ingestion_status(ingestion_status::IS_INVALID);

    _bulk_load_start_time_ms = 0;
    _replica->_bulk_load_ingestion_start_time_ms = 0;

    _status = bulk_load_status::BLS_INVALID;
}

// ThreadPool: THREAD_POOL_REPLICATION
bool replica_bulk_loader::is_cleaned_up()
{
    if (_status != bulk_load_status::BLS_INVALID) {
        return false;
    }
    {
        // download context not cleaned up
        zauto_read_lock l(_lock);
        if (_cur_downloaded_size.load() != 0 || _download_progress.load() != 0 ||
            _download_status.load() != ERR_OK || _download_files_task.size() != 0 ||
            _download_task != nullptr || _metadata.files.size() != 0 ||
            _metadata.file_total_size != 0) {
            return false;
        }
    }
    // ingestion context not cleaned up
    if (_replica->_is_bulk_load_ingestion ||
        _replica->_app->get_ingestion_status() != ingestion_status::IS_INVALID) {
        return false;
    }
    // local dir exists
    std::string bulk_load_dir = utils::filesystem::path_combine(
        _replica->_dir, bulk_load_constant::BULK_LOAD_LOCAL_ROOT_DIR);
    return !utils::filesystem::directory_exists(bulk_load_dir);
}

// ThreadPool: THREAD_POOL_REPLICATION
void replica_bulk_loader::pause_bulk_load()
{
    if (_status == bulk_load_status::BLS_PAUSED) {
        LOG_INFO_PREFIX("bulk load has been paused");
        return;
    }
    if (_status == bulk_load_status::BLS_DOWNLOADING) {
        zauto_write_lock l(_lock);
        cleanup_download_tasks();
        try_decrease_bulk_load_download_count();
    }
    _status = bulk_load_status::BLS_PAUSED;
    LOG_INFO_PREFIX("bulk load is paused");
}

// ThreadPool: THREAD_POOL_REPLICATION
void replica_bulk_loader::report_bulk_load_states_to_meta(bulk_load_status::type remote_status,
                                                          bool report_metadata,
                                                          /*out*/ bulk_load_response &response)
{
    if (status() != partition_status::PS_PRIMARY) {
        response.err = ERR_INVALID_STATE;
        return;
    }

    if (report_metadata) {
        zauto_read_lock l(_lock);
        if (!_metadata.files.empty()) {
            response.__set_metadata(_metadata);
        }
    }

    switch (remote_status) {
    case bulk_load_status::BLS_DOWNLOADING:
    case bulk_load_status::BLS_DOWNLOADED:
        report_group_download_progress(response);
        break;
    case bulk_load_status::BLS_INGESTING:
        report_group_ingestion_status(response);
        break;
    case bulk_load_status::BLS_SUCCEED:
    case bulk_load_status::BLS_CANCELED:
    case bulk_load_status::BLS_FAILED:
        report_group_cleaned_up(response);
        break;
    case bulk_load_status::BLS_PAUSING:
        report_group_is_paused(response);
        break;
    default:
        break;
    }

    response.primary_bulk_load_status = _status;
}

// ThreadPool: THREAD_POOL_REPLICATION
void replica_bulk_loader::report_group_download_progress(/*out*/ bulk_load_response &response)
{
    if (status() != partition_status::PS_PRIMARY) {
        LOG_WARNING_PREFIX("replica status={}, should be {}",
                           enum_to_string(status()),
                           enum_to_string(partition_status::PS_PRIMARY));
        response.err = ERR_INVALID_STATE;
        return;
    }

    partition_bulk_load_state primary_state;
    {
        zauto_read_lock l(_lock);
        primary_state.__set_download_progress(_download_progress.load());
        primary_state.__set_download_status(_download_status.load());
    }
    SET_VALUE_FROM_IP_AND_HOST_PORT(response,
                                    group_bulk_load_state,
                                    _replica->_primary_states.pc.primary,
                                    _replica->_primary_states.pc.hp_primary,
                                    primary_state);
    LOG_INFO_PREFIX("primary = {}, download progress = {}%, status = {}",
                    FMT_HOST_PORT_AND_IP(_replica->_primary_states.pc, primary),
                    primary_state.download_progress,
                    primary_state.download_status);

    int32_t total_progress = primary_state.download_progress;
    for (const auto &secondary : _replica->_primary_states.pc.hp_secondaries) {
        const auto &secondary_state =
            _replica->_primary_states.secondary_bulk_load_states[secondary];
        int32_t s_progress =
            secondary_state.__isset.download_progress ? secondary_state.download_progress : 0;
        error_code s_status =
            secondary_state.__isset.download_status ? secondary_state.download_status : ERR_OK;
        LOG_INFO_PREFIX(
            "secondary = {}, download progress = {}%, status={}", secondary, s_progress, s_status);
        SET_VALUE_FROM_HOST_PORT(response, group_bulk_load_state, secondary, secondary_state);
        total_progress += s_progress;
    }

    total_progress /= _replica->_primary_states.pc.max_replica_count;
    LOG_INFO_PREFIX("total download progress = {}%", total_progress);
    response.__set_total_download_progress(total_progress);
}

// ThreadPool: THREAD_POOL_REPLICATION
void replica_bulk_loader::report_group_ingestion_status(/*out*/ bulk_load_response &response)
{
    if (status() != partition_status::PS_PRIMARY) {
        LOG_WARNING_PREFIX("replica status={}, should be {}",
                           enum_to_string(status()),
                           enum_to_string(partition_status::PS_PRIMARY));
        response.err = ERR_INVALID_STATE;
        return;
    }

    partition_bulk_load_state primary_state;
    primary_state.__set_ingest_status(_replica->_app->get_ingestion_status());
    SET_VALUE_FROM_IP_AND_HOST_PORT(response,
                                    group_bulk_load_state,
                                    _replica->_primary_states.pc.primary,
                                    _replica->_primary_states.pc.hp_primary,
                                    primary_state);
    LOG_INFO_PREFIX("primary = {}, ingestion status = {}",
                    FMT_HOST_PORT_AND_IP(_replica->_primary_states.pc, primary),
                    enum_to_string(primary_state.ingest_status));

    bool is_group_ingestion_finish =
        (primary_state.ingest_status == ingestion_status::IS_SUCCEED) &&
        (_replica->_primary_states.pc.hp_secondaries.size() + 1 ==
         _replica->_primary_states.pc.max_replica_count);
    for (const auto &secondary : _replica->_primary_states.pc.hp_secondaries) {
        const auto &secondary_state =
            _replica->_primary_states.secondary_bulk_load_states[secondary];
        ingestion_status::type ingest_status = secondary_state.__isset.ingest_status
                                                   ? secondary_state.ingest_status
                                                   : ingestion_status::IS_INVALID;
        LOG_INFO_PREFIX(
            "secondary = {}, ingestion status={}", secondary, enum_to_string(ingest_status));
        SET_VALUE_FROM_HOST_PORT(response, group_bulk_load_state, secondary, secondary_state);
        is_group_ingestion_finish &= (ingest_status == ingestion_status::IS_SUCCEED);
    }
    response.__set_is_group_ingestion_finished(is_group_ingestion_finish);

    // if group ingestion finish, recover wirte immediately
    if (is_group_ingestion_finish) {
        LOG_INFO_PREFIX("finish ingestion, recover write");
        _replica->_is_bulk_load_ingestion = false;
        _replica->_bulk_load_ingestion_start_time_ms = 0;
    }
}

// ThreadPool: THREAD_POOL_REPLICATION
void replica_bulk_loader::report_group_cleaned_up(bulk_load_response &response)
{
    if (status() != partition_status::PS_PRIMARY) {
        LOG_WARNING_PREFIX("replica status={}, should be {}",
                           enum_to_string(status()),
                           enum_to_string(partition_status::PS_PRIMARY));
        response.err = ERR_INVALID_STATE;
        return;
    }

    partition_bulk_load_state primary_state;
    primary_state.__set_is_cleaned_up(is_cleaned_up());
    SET_VALUE_FROM_IP_AND_HOST_PORT(response,
                                    group_bulk_load_state,
                                    _replica->_primary_states.pc.primary,
                                    _replica->_primary_states.pc.hp_primary,
                                    primary_state);
    LOG_INFO_PREFIX("primary = {}, bulk load states cleaned_up = {}",
                    FMT_HOST_PORT_AND_IP(_replica->_primary_states.pc, primary),
                    primary_state.is_cleaned_up);

    bool group_flag =
        (primary_state.is_cleaned_up) && (_replica->_primary_states.pc.hp_secondaries.size() + 1 ==
                                          _replica->_primary_states.pc.max_replica_count);
    for (const auto &secondary : _replica->_primary_states.pc.hp_secondaries) {
        const auto &secondary_state =
            _replica->_primary_states.secondary_bulk_load_states[secondary];
        bool is_cleaned_up = secondary_state.__isset.is_cleaned_up ? secondary_state.is_cleaned_up
                                                                   : false;
        LOG_INFO_PREFIX(
            "secondary = {}, bulk load states cleaned_up = {}", secondary, is_cleaned_up);
        SET_VALUE_FROM_HOST_PORT(response, group_bulk_load_state, secondary, secondary_state);
        group_flag &= is_cleaned_up;
    }
    LOG_INFO_PREFIX("group bulk load states cleaned_up = {}", group_flag);
    response.__set_is_group_bulk_load_context_cleaned_up(group_flag);
}

// ThreadPool: THREAD_POOL_REPLICATION
void replica_bulk_loader::report_group_is_paused(bulk_load_response &response)
{
    if (status() != partition_status::PS_PRIMARY) {
        LOG_WARNING_PREFIX("replica status={}, should be {}",
                           enum_to_string(status()),
                           enum_to_string(partition_status::PS_PRIMARY));
        response.err = ERR_INVALID_STATE;
        return;
    }

    partition_bulk_load_state primary_state;
    primary_state.__set_is_paused(_status == bulk_load_status::BLS_PAUSED);
    SET_VALUE_FROM_IP_AND_HOST_PORT(response,
                                    group_bulk_load_state,
                                    _replica->_primary_states.pc.primary,
                                    _replica->_primary_states.pc.hp_primary,
                                    primary_state);
    LOG_INFO_PREFIX("primary = {}, bulk_load is_paused = {}",
                    FMT_HOST_PORT_AND_IP(_replica->_primary_states.pc, primary),
                    primary_state.is_paused);

    bool group_is_paused =
        primary_state.is_paused && (_replica->_primary_states.pc.hp_secondaries.size() + 1 ==
                                    _replica->_primary_states.pc.max_replica_count);
    for (const auto &secondary : _replica->_primary_states.pc.hp_secondaries) {
        partition_bulk_load_state secondary_state =
            _replica->_primary_states.secondary_bulk_load_states[secondary];
        bool is_paused = secondary_state.__isset.is_paused ? secondary_state.is_paused : false;
        LOG_INFO_PREFIX("secondary = {}, bulk_load is_paused = {}", secondary, is_paused);
        SET_VALUE_FROM_HOST_PORT(response, group_bulk_load_state, secondary, secondary_state);
        group_is_paused &= is_paused;
    }
    LOG_INFO_PREFIX("group bulk load is_paused = {}", group_is_paused);
    response.__set_is_group_bulk_load_paused(group_is_paused);
}

// ThreadPool: THREAD_POOL_REPLICATION
void replica_bulk_loader::report_bulk_load_states_to_primary(
    bulk_load_status::type remote_status,
    /*out*/ group_bulk_load_response &response)
{
    if (status() != partition_status::PS_SECONDARY) {
        response.err = ERR_INVALID_STATE;
        return;
    }

    partition_bulk_load_state bulk_load_state;
    auto local_status = _status;
    switch (remote_status) {
    case bulk_load_status::BLS_DOWNLOADING:
    case bulk_load_status::BLS_DOWNLOADED: {
        zauto_read_lock l(_lock);
        bulk_load_state.__set_download_progress(_download_progress.load());
        bulk_load_state.__set_download_status(_download_status.load());
    } break;
    case bulk_load_status::BLS_INGESTING:
        bulk_load_state.__set_ingest_status(_replica->_app->get_ingestion_status());
        break;
    case bulk_load_status::BLS_SUCCEED:
    case bulk_load_status::BLS_CANCELED:
    case bulk_load_status::BLS_FAILED:
        bulk_load_state.__set_is_cleaned_up(is_cleaned_up());
        break;
    case bulk_load_status::BLS_PAUSING:
        bulk_load_state.__set_is_paused(local_status == bulk_load_status::BLS_PAUSED);
        break;
    default:
        break;
    }

    response.status = local_status;
    response.bulk_load_state = bulk_load_state;
}

// ThreadPool: THREAD_POOL_REPLICATION
void replica_bulk_loader::clear_bulk_load_states_if_needed(partition_status::type old_status,
                                                           partition_status::type new_status)
{
    if ((new_status == partition_status::PS_PRIMARY ||
         new_status == partition_status::PS_SECONDARY) &&
        new_status != old_status) {
        if (_status == bulk_load_status::BLS_SUCCEED || _status == bulk_load_status::BLS_CANCELED ||
            _status == bulk_load_status::BLS_FAILED || _status == bulk_load_status::BLS_INVALID) {
            return;
        }
        LOG_INFO_PREFIX("prepare to clear bulk load states, current status = {}",
                        enum_to_string(_status));
        clear_bulk_load_states();
    }
}

} // namespace replication
} // namespace dsn
