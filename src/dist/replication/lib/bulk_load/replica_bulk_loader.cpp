// Copyright (c) 2017-present, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "replica_bulk_loader.h"

#include <fstream>

#include <dsn/dist/block_service.h>
#include <dsn/dist/fmt_logging.h>
#include <dsn/dist/replication/replication_app_base.h>
#include <dsn/utility/fail_point.h>
#include <dsn/utility/filesystem.h>

namespace dsn {
namespace replication {

typedef rpc_holder<group_bulk_load_request, group_bulk_load_response> group_bulk_load_rpc;

replica_bulk_loader::replica_bulk_loader(replica *r)
    : replica_base(r), _replica(r), _stub(r->get_replica_stub())
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
        dwarn_replica("receive bulk load request with wrong status {}", enum_to_string(status()));
        response.err = ERR_INVALID_STATE;
        return;
    }

    if (request.ballot != get_ballot()) {
        dwarn_replica(
            "receive bulk load request with wrong version, remote ballot={}, local ballot={}",
            request.ballot,
            get_ballot());
        response.err = ERR_INVALID_STATE;
        return;
    }

    ddebug_replica(
        "receive bulk load request, remote provider = {}, cluster_name = {}, app_name = {}, "
        "meta_bulk_load_status = {}, local bulk_load_status = {}",
        request.remote_provider_name,
        request.cluster_name,
        request.app_name,
        enum_to_string(request.meta_bulk_load_status),
        enum_to_string(_status));

    error_code ec = do_bulk_load(request.app_name,
                                 request.meta_bulk_load_status,
                                 request.cluster_name,
                                 request.remote_provider_name);
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
        dwarn_replica("has learners, skip broadcast group bulk load request");
        return;
    }

    if (!_replica->_primary_states.group_bulk_load_pending_replies.empty()) {
        dwarn_replica("{} group bulk_load replies are still pending, cancel it firstly",
                      _replica->_primary_states.group_bulk_load_pending_replies.size());
        for (auto &kv : _replica->_primary_states.group_bulk_load_pending_replies) {
            CLEANUP_TASK_ALWAYS(kv.second);
        }
        _replica->_primary_states.group_bulk_load_pending_replies.clear();
    }

    ddebug_replica("start to broadcast group bulk load");

    for (const auto &addr : _replica->_primary_states.membership.secondaries) {
        if (addr == _stub->_primary_address)
            continue;

        auto request = make_unique<group_bulk_load_request>();
        request->app_name = _replica->_app_info.app_name;
        request->target_address = addr;
        _replica->_primary_states.get_replica_config(partition_status::PS_SECONDARY,
                                                     request->config);
        request->cluster_name = meta_req.cluster_name;
        request->provider_name = meta_req.remote_provider_name;
        request->meta_bulk_load_status = meta_req.meta_bulk_load_status;

        ddebug_replica("send group_bulk_load_request to {}", addr.to_string());

        group_bulk_load_rpc rpc(
            std::move(request), RPC_GROUP_BULK_LOAD, 0_ms, 0, get_gpid().thread_hash());
        auto callback_task = rpc.call(addr, tracker(), [this, rpc](error_code err) mutable {
            on_group_bulk_load_reply(err, rpc.request(), rpc.response());
        });
        _replica->_primary_states.group_bulk_load_pending_replies[addr] = callback_task;
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
        dwarn_replica(
            "receive outdated group_bulk_load request, request ballot({}) VS local ballot({})",
            request.config.ballot,
            get_ballot());
        return;
    }
    if (request.config.ballot > get_ballot()) {
        response.err = ERR_INVALID_STATE;
        dwarn_replica("receive group_bulk_load request, local ballot is outdated, request "
                      "ballot({}) VS local ballot({})",
                      request.config.ballot,
                      get_ballot());
        return;
    }
    if (status() != request.config.status) {
        response.err = ERR_INVALID_STATE;
        dwarn_replica("status changed, status should be {}, but {}",
                      enum_to_string(request.config.status),
                      enum_to_string(status()));
        return;
    }

    ddebug_replica("receive group_bulk_load request, primary address = {}, ballot = {}, "
                   "meta bulk_load_status = {}, local bulk_load_status = {}",
                   request.config.primary.to_string(),
                   request.config.ballot,
                   enum_to_string(request.meta_bulk_load_status),
                   enum_to_string(_status));

    error_code ec = do_bulk_load(request.app_name,
                                 request.meta_bulk_load_status,
                                 request.cluster_name,
                                 request.provider_name);
    if (ec != ERR_OK) {
        response.err = ec;
        response.status = _status;
        return;
    }

    report_bulk_load_states_to_primary(request.meta_bulk_load_status, response);
}

void replica_bulk_loader::on_group_bulk_load_reply(error_code err,
                                                   const group_bulk_load_request &req,
                                                   const group_bulk_load_response &resp)
{
    _replica->_checker.only_one_thread_access();

    if (partition_status::PS_PRIMARY != status()) {
        derror_replica("replica status={}, should be {}",
                       enum_to_string(status()),
                       enum_to_string(partition_status::PS_PRIMARY));
        return;
    }

    _replica->_primary_states.group_bulk_load_pending_replies.erase(req.target_address);

    // TODO(heyuchen): TBD
    // if error happened, reset secondary bulk_load_state
    // otherwise, set secondary bulk_load_states from resp
}

// ThreadPool: THREAD_POOL_REPLICATION
error_code replica_bulk_loader::do_bulk_load(const std::string &app_name,
                                             bulk_load_status::type meta_status,
                                             const std::string &cluster_name,
                                             const std::string &provider_name)
{
    // TODO(heyuchen): TBD
    return ERR_OK;
}

// ThreadPool: THREAD_POOL_REPLICATION
error_code replica_bulk_loader::bulk_load_start_download(const std::string &app_name,
                                                         const std::string &cluster_name,
                                                         const std::string &provider_name)
{
    if (_stub->_bulk_load_downloading_count.load() >=
        _stub->_max_concurrent_bulk_load_downloading_count) {
        dwarn_replica("node[{}] already has {} replica downloading, wait for next round",
                      _stub->_primary_address_str,
                      _stub->_bulk_load_downloading_count.load());
        return ERR_BUSY;
    }

    // reset local bulk load context and state
    if (status() == partition_status::PS_PRIMARY) {
        _replica->_primary_states.cleanup_bulk_load_states();
    }
    clear_bulk_load_states();

    _status = bulk_load_status::BLS_DOWNLOADING;
    ++_stub->_bulk_load_downloading_count;
    ddebug_replica("node[{}] has {} replica executing downloading",
                   _stub->_primary_address_str,
                   _stub->_bulk_load_downloading_count.load());
    // TODO(heyuchen): add perf-counter

    // start download
    ddebug_replica("start to download sst files");
    error_code err = download_sst_files(app_name, cluster_name, provider_name);
    if (err != ERR_OK) {
        try_decrease_bulk_load_download_count();
    }
    return err;
}

// ThreadPool: THREAD_POOL_REPLICATION
error_code replica_bulk_loader::download_sst_files(const std::string &app_name,
                                                   const std::string &cluster_name,
                                                   const std::string &provider_name)
{
    FAIL_POINT_INJECT_F("replica_bulk_loader_download_sst_files",
                        [](string_view) -> error_code { return ERR_OK; });

    // create local bulk load dir
    if (!utils::filesystem::directory_exists(_replica->_dir)) {
        derror_replica("_dir({}) is not existed", _replica->_dir);
        return ERR_FILE_OPERATION_FAILED;
    }
    const std::string local_dir = utils::filesystem::path_combine(
        _replica->_dir, bulk_load_constant::BULK_LOAD_LOCAL_ROOT_DIR);
    if (!utils::filesystem::directory_exists(local_dir) &&
        !utils::filesystem::create_directory(local_dir)) {
        derror_replica("create bulk_load_dir({}) failed", local_dir);
        return ERR_FILE_OPERATION_FAILED;
    }

    const std::string remote_dir =
        get_remote_bulk_load_dir(app_name, cluster_name, get_gpid().get_partition_index());
    dist::block_service::block_filesystem *fs =
        _stub->_block_service_manager.get_block_filesystem(provider_name);

    // download metadata file synchronously
    uint64_t file_size = 0;
    error_code err = _replica->do_download(
        remote_dir, local_dir, bulk_load_constant::BULK_LOAD_METADATA, fs, file_size);
    if (err != ERR_OK) {
        derror_replica("download bulk load metadata file failed, error = {}", err.to_string());
        return err;
    }

    // parse metadata
    const std::string &local_metadata_file_name =
        utils::filesystem::path_combine(local_dir, bulk_load_constant::BULK_LOAD_METADATA);
    err = parse_bulk_load_metadata(local_metadata_file_name, _metadata);
    if (err != ERR_OK) {
        derror_replica("parse bulk load metadata failed, error = {}", err.to_string());
        return err;
    }

    // download sst files asynchronously
    for (const auto &f_meta : _metadata.files) {
        auto bulk_load_download_task = tasking::enqueue(
            LPC_BACKGROUND_BULK_LOAD, tracker(), [this, remote_dir, local_dir, f_meta, fs]() {
                uint64_t f_size = 0;
                error_code ec =
                    _replica->do_download(remote_dir, local_dir, f_meta.name, fs, f_size);
                if (ec == ERR_OK && !verify_sst_files(f_meta, local_dir)) {
                    ec = ERR_CORRUPTION;
                }
                if (ec != ERR_OK) {
                    try_decrease_bulk_load_download_count();
                    _download_status.store(ec);
                    derror_replica(
                        "failed to download file({}), error = {}", f_meta.name, ec.to_string());
                    // TODO(heyuchen): add perf-counter
                    return;
                }
                // download file succeed, update progress
                update_bulk_load_download_progress(f_size, f_meta.name);
                // TODO(heyuchen): add perf-counter
            });
        _download_task[f_meta.name] = bulk_load_download_task;
    }
    return err;
}

// ThreadPool: THREAD_POOL_REPLICATION
error_code replica_bulk_loader::parse_bulk_load_metadata(const std::string &fname,
                                                         /*out*/ bulk_load_metadata &meta)
{
    // TODO(heyuchen): TBD
    // read file and parse file content as bulk_load_metadata
    return ERR_OK;
}

// ThreadPool: THREAD_POOL_REPLICATION_LONG
bool replica_bulk_loader::verify_sst_files(const file_meta &f_meta, const std::string &local_dir)
{
    // TODO(heyuchen): TBD
    // compare sst file metadata calculated by file and parsed by metadata
    return true;
}

// ThreadPool: THREAD_POOL_REPLICATION_LONG
void replica_bulk_loader::update_bulk_load_download_progress(uint64_t file_size,
                                                             const std::string &file_name)
{
    // TODO(heyuchen): TBD
    // update download progress after downloading sst files succeed
}

// ThreadPool: THREAD_POOL_REPLICATION, THREAD_POOL_REPLICATION_LONG
void replica_bulk_loader::try_decrease_bulk_load_download_count()
{
    --_stub->_bulk_load_downloading_count;
    ddebug_replica("node[{}] has {} replica executing downloading",
                   _stub->_primary_address_str,
                   _stub->_bulk_load_downloading_count.load());
}

void replica_bulk_loader::clear_bulk_load_states()
{
    // TODO(heyuchen): TBD
    // clear replica bulk load states and cleanup bulk load context
}

// ThreadPool: THREAD_POOL_REPLICATION
void replica_bulk_loader::report_bulk_load_states_to_meta(bulk_load_status::type remote_status,
                                                          bool report_metadata,
                                                          /*out*/ bulk_load_response &response)
{
    // TODO(heyuchen): TBD
}

void replica_bulk_loader::report_bulk_load_states_to_primary(
    bulk_load_status::type remote_status,
    /*out*/ group_bulk_load_response &response)
{
    // TODO(heyuchen): TBD
}

} // namespace replication
} // namespace dsn
