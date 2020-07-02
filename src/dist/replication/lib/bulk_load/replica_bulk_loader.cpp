// Copyright (c) 2017-present, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "replica_bulk_loader.h"

#include <dsn/dist/block_service.h>
#include <dsn/dist/fmt_logging.h>
#include <dsn/dist/replication/replication_app_base.h>
#include <dsn/utility/fail_point.h>
#include <dsn/utility/filesystem.h>

namespace dsn {
namespace replication {

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
    if (status() != partition_status::PS_PRIMARY && status() != partition_status::PS_SECONDARY) {
        return ERR_INVALID_STATE;
    }

    bulk_load_status::type local_status = _status;
    error_code ec = validate_bulk_load_status(meta_status, local_status);
    if (ec != ERR_OK) {
        derror_replica("invalid bulk load status, remote = {}, local = {}",
                       enum_to_string(meta_status),
                       enum_to_string(local_status));
        return ec;
    }

    switch (meta_status) {
    case bulk_load_status::BLS_DOWNLOADING:
        // TODO(heyuchen): add restart downloading status check
        if (local_status == bulk_load_status::BLS_INVALID) {
            ec = start_download(app_name, cluster_name, provider_name);
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
        if (local_status == bulk_load_status::BLS_INGESTING) {
            handle_bulk_load_succeed();
        } else if (local_status == bulk_load_status::BLS_SUCCEED) {
            handle_bulk_load_finish(meta_status);
        }
        break;
    case bulk_load_status::BLS_PAUSING:
        pause_bulk_load();
        break;
    // TODO(heyuchen): add other bulk load status
    default:
        break;
    }
    return ec;
}

error_code replica_bulk_loader::validate_bulk_load_status(bulk_load_status::type meta_status,
                                                          bulk_load_status::type local_status)
{
    // TODO(heyuchen): TBD
    return ERR_OK;
}

// ThreadPool: THREAD_POOL_REPLICATION
error_code replica_bulk_loader::start_download(const std::string &app_name,
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
    error_code err = _stub->_block_service_manager.download_file(
        remote_dir, local_dir, bulk_load_constant::BULK_LOAD_METADATA, fs, file_size);
    if (err != ERR_OK) {
        derror_replica("download bulk load metadata file failed, error = {}", err.to_string());
        return err;
    }

    // parse metadata
    const std::string &local_metadata_file_name =
        utils::filesystem::path_combine(local_dir, bulk_load_constant::BULK_LOAD_METADATA);
    err = parse_bulk_load_metadata(local_metadata_file_name);
    if (err != ERR_OK) {
        derror_replica("parse bulk load metadata failed, error = {}", err.to_string());
        return err;
    }

    // download sst files asynchronously
    for (const auto &f_meta : _metadata.files) {
        auto bulk_load_download_task = tasking::enqueue(
            LPC_BACKGROUND_BULK_LOAD, tracker(), [this, remote_dir, local_dir, f_meta, fs]() {
                uint64_t f_size = 0;
                error_code ec = _stub->_block_service_manager.download_file(
                    remote_dir, local_dir, f_meta.name, fs, f_size);
                const std::string &file_name =
                    utils::filesystem::path_combine(local_dir, f_meta.name);
                if (ec == ERR_OK &&
                    !utils::filesystem::verify_file(file_name, f_meta.md5, f_meta.size)) {
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
error_code replica_bulk_loader::parse_bulk_load_metadata(const std::string &fname)
{
    std::string buf;
    error_code ec = utils::filesystem::read_file(fname, buf);
    if (ec != ERR_OK) {
        derror_replica("read file {} failed, error = {}", fname, ec);
        return ec;
    }

    blob bb = blob::create_from_bytes(std::move(buf));
    if (!json::json_forwarder<bulk_load_metadata>::decode(bb, _metadata)) {
        derror_replica("file({}) is damaged", fname);
        return ERR_CORRUPTION;
    }

    if (_metadata.file_total_size <= 0) {
        derror_replica("bulk_load_metadata has invalid file_total_size({})",
                       _metadata.file_total_size);
        return ERR_CORRUPTION;
    }

    return ERR_OK;
}

// ThreadPool: THREAD_POOL_REPLICATION_LONG
void replica_bulk_loader::update_bulk_load_download_progress(uint64_t file_size,
                                                             const std::string &file_name)
{
    if (_metadata.file_total_size <= 0) {
        derror_replica("bulk_load_metadata has invalid file_total_size({})",
                       _metadata.file_total_size);
        return;
    }

    ddebug_replica("update progress after downloading file({})", file_name);
    _cur_downloaded_size.fetch_add(file_size);
    auto total_size = static_cast<double>(_metadata.file_total_size);
    auto cur_downloaded_size = static_cast<double>(_cur_downloaded_size.load());
    auto cur_progress = static_cast<int32_t>((cur_downloaded_size / total_size) * 100);
    _download_progress.store(cur_progress);
    ddebug_replica("total_size = {}, cur_downloaded_size = {}, progress = {}",
                   total_size,
                   cur_downloaded_size,
                   cur_progress);

    tasking::enqueue(LPC_REPLICATION_COMMON,
                     tracker(),
                     std::bind(&replica_bulk_loader::check_download_finish, this),
                     get_gpid().thread_hash());
}

// ThreadPool: THREAD_POOL_REPLICATION, THREAD_POOL_REPLICATION_LONG
void replica_bulk_loader::try_decrease_bulk_load_download_count()
{
    --_stub->_bulk_load_downloading_count;
    ddebug_replica("node[{}] has {} replica executing downloading",
                   _stub->_primary_address_str,
                   _stub->_bulk_load_downloading_count.load());
}

// ThreadPool: THREAD_POOL_REPLICATION
void replica_bulk_loader::check_download_finish()
{
    if (_download_progress.load() == bulk_load_constant::PROGRESS_FINISHED &&
        _status == bulk_load_status::BLS_DOWNLOADING) {
        ddebug_replica("download all files succeed");
        _status = bulk_load_status::BLS_DOWNLOADED;
        try_decrease_bulk_load_download_count();
        cleanup_download_task();
    }
}

// ThreadPool: THREAD_POOL_REPLICATION
void replica_bulk_loader::start_ingestion()
{
    _status = bulk_load_status::BLS_INGESTING;
    // TODO(heyuchen): add perf-counter
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
        mu->add_client_request(RPC_REPLICATION_WRITE_EMPTY, nullptr);
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
    // TODO(heyuchen): add perf-counter
}

// ThreadPool: THREAD_POOL_REPLICATION
void replica_bulk_loader::handle_bulk_load_finish(bulk_load_status::type new_status)
{
    if (is_cleaned_up()) {
        ddebug_replica("bulk load states have been cleaned up");
        return;
    }

    if (status() == partition_status::PS_PRIMARY) {
        // TODO(heyuchen): cleanup _primary_states.secondary_bulk_load_states
    }

    ddebug_replica("bulk load finished, old_status = {}, new_status = {}",
                   enum_to_string(_status),
                   enum_to_string(new_status));

    // remove local bulk load dir
    std::string bulk_load_dir = utils::filesystem::path_combine(
        _replica->_dir, bulk_load_constant::BULK_LOAD_LOCAL_ROOT_DIR);
    error_code err = remove_local_bulk_load_dir(bulk_load_dir);
    if (err != ERR_OK) {
        tasking::enqueue(
            LPC_REPLICATION_COMMON,
            &_replica->_tracker,
            std::bind(&replica_bulk_loader::remove_local_bulk_load_dir, this, bulk_load_dir),
            get_gpid().thread_hash());
    }

    clear_bulk_load_states();
}

// ThreadPool: THREAD_POOL_REPLICATION
error_code replica_bulk_loader::remove_local_bulk_load_dir(const std::string &bulk_load_dir)
{
    if (!utils::filesystem::directory_exists(bulk_load_dir) ||
        !utils::filesystem::remove_path(bulk_load_dir)) {
        derror_replica("remove bulk_load dir({}) failed", bulk_load_dir);
        return ERR_FILE_OPERATION_FAILED;
    }
    return ERR_OK;
}

// ThreadPool: THREAD_POOL_REPLICATION
void replica_bulk_loader::cleanup_download_task()
{
    for (auto &kv : _download_task) {
        CLEANUP_TASK_ALWAYS(kv.second)
    }
    _download_task.clear();
}

// ThreadPool: THREAD_POOL_REPLICATION
void replica_bulk_loader::clear_bulk_load_states()
{
    if (_status == bulk_load_status::BLS_DOWNLOADING) {
        try_decrease_bulk_load_download_count();
    }

    cleanup_download_task();
    _metadata.files.clear();
    _metadata.file_total_size = 0;
    _cur_downloaded_size.store(0);
    _download_progress.store(0);
    _download_status.store(ERR_OK);

    _replica->_is_bulk_load_ingestion = false;
    _replica->_app->set_ingestion_status(ingestion_status::IS_INVALID);

    // TODO(heyuchen): clear other states

    _status = bulk_load_status::BLS_INVALID;
}

// ThreadPool: THREAD_POOL_REPLICATION
bool replica_bulk_loader::is_cleaned_up()
{
    if (_status != bulk_load_status::BLS_INVALID) {
        return false;
    }
    // download context not cleaned up
    if (_cur_downloaded_size.load() != 0 || _download_progress.load() != 0 ||
        _download_status.load() != ERR_OK || _download_task.size() != 0 ||
        _metadata.files.size() != 0 || _metadata.file_total_size != 0) {
        return false;
    }
    // ingestion context not cleaned up
    if (_replica->_is_bulk_load_ingestion ||
        _replica->_app->get_ingestion_status() != ingestion_status::IS_INVALID) {
        return false;
    }
    return true;
}

// ThreadPool: THREAD_POOL_REPLICATION
void replica_bulk_loader::pause_bulk_load()
{
    if (_status == bulk_load_status::BLS_PAUSED) {
        ddebug_replica("bulk load has been paused");
        return;
    }
    if (_status == bulk_load_status::BLS_DOWNLOADING) {
        cleanup_download_task();
        try_decrease_bulk_load_download_count();
    }
    _status = bulk_load_status::BLS_PAUSED;
    ddebug_replica("bulk load is paused");
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

    if (report_metadata && !_metadata.files.empty()) {
        response.__set_metadata(_metadata);
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
        report_group_cleaned_up(response);
        break;
    case bulk_load_status::BLS_PAUSING:
        report_group_is_paused(response);
        break;
    // TODO(heyuchen): add other status
    default:
        break;
    }

    response.primary_bulk_load_status = _status;
}

// ThreadPool: THREAD_POOL_REPLICATION
void replica_bulk_loader::report_group_download_progress(/*out*/ bulk_load_response &response)
{
    if (status() != partition_status::PS_PRIMARY) {
        dwarn_replica("replica status={}, should be {}",
                      enum_to_string(status()),
                      enum_to_string(partition_status::PS_PRIMARY));
        response.err = ERR_INVALID_STATE;
        return;
    }

    partition_bulk_load_state primary_state;
    primary_state.__set_download_progress(_download_progress.load());
    primary_state.__set_download_status(_download_status.load());
    response.group_bulk_load_state[_replica->_primary_states.membership.primary] = primary_state;
    ddebug_replica("primary = {}, download progress = {}%, status = {}",
                   _replica->_primary_states.membership.primary.to_string(),
                   primary_state.download_progress,
                   primary_state.download_status);

    int32_t total_progress = primary_state.download_progress;
    for (const auto &target_address : _replica->_primary_states.membership.secondaries) {
        const auto &secondary_state =
            _replica->_primary_states.secondary_bulk_load_states[target_address];
        int32_t s_progress =
            secondary_state.__isset.download_progress ? secondary_state.download_progress : 0;
        error_code s_status =
            secondary_state.__isset.download_status ? secondary_state.download_status : ERR_OK;
        ddebug_replica("secondary = {}, download progress = {}%, status={}",
                       target_address.to_string(),
                       s_progress,
                       s_status);
        response.group_bulk_load_state[target_address] = secondary_state;
        total_progress += s_progress;
    }

    total_progress /= _replica->_primary_states.membership.max_replica_count;
    ddebug_replica("total download progress = {}%", total_progress);
    response.__set_total_download_progress(total_progress);
}

// ThreadPool: THREAD_POOL_REPLICATION
void replica_bulk_loader::report_group_ingestion_status(/*out*/ bulk_load_response &response)
{
    if (status() != partition_status::PS_PRIMARY) {
        dwarn_replica("replica status={}, should be {}",
                      enum_to_string(status()),
                      enum_to_string(partition_status::PS_PRIMARY));
        response.err = ERR_INVALID_STATE;
        return;
    }

    partition_bulk_load_state primary_state;
    primary_state.__set_ingest_status(_replica->_app->get_ingestion_status());
    response.group_bulk_load_state[_replica->_primary_states.membership.primary] = primary_state;
    ddebug_replica("primary = {}, ingestion status = {}",
                   _replica->_primary_states.membership.primary.to_string(),
                   enum_to_string(primary_state.ingest_status));

    bool is_group_ingestion_finish =
        (primary_state.ingest_status == ingestion_status::IS_SUCCEED) &&
        (_replica->_primary_states.membership.secondaries.size() + 1 ==
         _replica->_primary_states.membership.max_replica_count);
    for (const auto &target_address : _replica->_primary_states.membership.secondaries) {
        const auto &secondary_state =
            _replica->_primary_states.secondary_bulk_load_states[target_address];
        ingestion_status::type ingest_status = secondary_state.__isset.ingest_status
                                                   ? secondary_state.ingest_status
                                                   : ingestion_status::IS_INVALID;
        ddebug_replica("secondary = {}, ingestion status={}",
                       target_address.to_string(),
                       enum_to_string(ingest_status));
        response.group_bulk_load_state[target_address] = secondary_state;
        is_group_ingestion_finish &= (ingest_status == ingestion_status::IS_SUCCEED);
    }
    response.__set_is_group_ingestion_finished(is_group_ingestion_finish);

    // if group ingestion finish, recover wirte immediately
    if (is_group_ingestion_finish) {
        ddebug_replica("finish ingestion, recover write");
        _replica->_is_bulk_load_ingestion = false;
        // TODO(heyuchen): reset perf-counter
    }
}

// ThreadPool: THREAD_POOL_REPLICATION
void replica_bulk_loader::report_group_cleaned_up(bulk_load_response &response)
{
    if (status() != partition_status::PS_PRIMARY) {
        dwarn_replica("replica status={}, should be {}",
                      enum_to_string(status()),
                      enum_to_string(partition_status::PS_PRIMARY));
        response.err = ERR_INVALID_STATE;
        return;
    }

    partition_bulk_load_state primary_state;
    primary_state.__set_is_cleaned_up(is_cleaned_up());
    response.group_bulk_load_state[_replica->_primary_states.membership.primary] = primary_state;
    ddebug_replica("primary = {}, bulk load states cleaned_up = {}",
                   _replica->_primary_states.membership.primary.to_string(),
                   primary_state.is_cleaned_up);

    bool group_flag = (primary_state.is_cleaned_up) &&
                      (_replica->_primary_states.membership.secondaries.size() + 1 ==
                       _replica->_primary_states.membership.max_replica_count);
    for (const auto &target_address : _replica->_primary_states.membership.secondaries) {
        const auto &secondary_state =
            _replica->_primary_states.secondary_bulk_load_states[target_address];
        bool is_cleaned_up =
            secondary_state.__isset.is_cleaned_up ? secondary_state.is_cleaned_up : false;
        ddebug_replica("secondary = {}, bulk load states cleaned_up = {}",
                       target_address.to_string(),
                       is_cleaned_up);
        response.group_bulk_load_state[target_address] = secondary_state;
        group_flag &= is_cleaned_up;
    }
    ddebug_replica("group bulk load states cleaned_up = {}", group_flag);
    response.__set_is_group_bulk_load_context_cleaned_up(group_flag);
}

// ThreadPool: THREAD_POOL_REPLICATION
void replica_bulk_loader::report_group_is_paused(bulk_load_response &response)
{
    if (status() != partition_status::PS_PRIMARY) {
        dwarn_replica("replica status={}, should be {}",
                      enum_to_string(status()),
                      enum_to_string(partition_status::PS_PRIMARY));
        response.err = ERR_INVALID_STATE;
        return;
    }

    partition_bulk_load_state primary_state;
    primary_state.__set_is_paused(_status == bulk_load_status::BLS_PAUSED);
    response.group_bulk_load_state[_replica->_primary_states.membership.primary] = primary_state;
    ddebug_replica("primary = {}, bulk_load is_paused = {}",
                   _replica->_primary_states.membership.primary.to_string(),
                   primary_state.is_paused);

    bool group_is_paused =
        primary_state.is_paused && (_replica->_primary_states.membership.secondaries.size() + 1 ==
                                    _replica->_primary_states.membership.max_replica_count);
    for (const auto &target_address : _replica->_primary_states.membership.secondaries) {
        partition_bulk_load_state secondary_state =
            _replica->_primary_states.secondary_bulk_load_states[target_address];
        bool is_paused = secondary_state.__isset.is_paused ? secondary_state.is_paused : false;
        ddebug_replica(
            "secondary = {}, bulk_load is_paused = {}", target_address.to_string(), is_paused);
        response.group_bulk_load_state[target_address] = secondary_state;
        group_is_paused &= is_paused;
    }
    ddebug_replica("group bulk load is_paused = {}", group_is_paused);
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
    case bulk_load_status::BLS_DOWNLOADED:
        bulk_load_state.__set_download_progress(_download_progress.load());
        bulk_load_state.__set_download_status(_download_status.load());
        break;
    case bulk_load_status::BLS_INGESTING:
        bulk_load_state.__set_ingest_status(_replica->_app->get_ingestion_status());
        break;
    case bulk_load_status::BLS_SUCCEED:
        bulk_load_state.__set_is_cleaned_up(is_cleaned_up());
        break;
    case bulk_load_status::BLS_PAUSING:
        bulk_load_state.__set_is_paused(local_status == bulk_load_status::BLS_PAUSED);
        break;
    // TODO(heyuchen): add other status
    default:
        break;
    }

    response.status = local_status;
    response.bulk_load_state = bulk_load_state;
}

} // namespace replication
} // namespace dsn
