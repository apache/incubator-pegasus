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

#include "utils/fmt_logging.h"
#include "common/replica_envs.h"
#include "utils/fail_point.h"

#include "meta_bulk_load_service.h"

namespace dsn {
namespace replication {

DSN_DEFINE_uint32("meta_server",
                  bulk_load_max_rollback_times,
                  10,
                  "if bulk load rollback time "
                  "exceed this value, meta won't "
                  "rollback bulk load process to "
                  "downloading, but turn it into "
                  "failed");
DSN_TAG_VARIABLE(bulk_load_max_rollback_times, FT_MUTABLE);

DSN_DEFINE_bool("meta_server",
                bulk_load_verify_before_ingest,
                false,
                "verify files according to metadata before ingest");
DSN_TAG_VARIABLE(bulk_load_verify_before_ingest, FT_MUTABLE);

DSN_DEFINE_bool("meta_server",
                enable_concurrent_bulk_load,
                false,
                "whether to enable different apps to execute bulk load at the same time");
DSN_TAG_VARIABLE(enable_concurrent_bulk_load, FT_MUTABLE);

bulk_load_service::bulk_load_service(meta_service *meta_svc, const std::string &bulk_load_dir)
    : _meta_svc(meta_svc), _state(meta_svc->get_server_state()), _bulk_load_root(bulk_load_dir)
{
}

// ThreadPool: THREAD_POOL_META_SERVER
void bulk_load_service::initialize_bulk_load_service()
{
    _sync_bulk_load_storage =
        make_unique<mss::meta_storage>(_meta_svc->get_remote_storage(), &_sync_tracker);
    _ingestion_context = make_unique<ingestion_context>();

    create_bulk_load_root_dir();
    _sync_tracker.wait_outstanding_tasks();

    try_to_continue_bulk_load();
}

// ThreadPool: THREAD_POOL_META_SERVER
void bulk_load_service::on_start_bulk_load(start_bulk_load_rpc rpc)
{
    FAIL_POINT_INJECT_F("meta_on_start_bulk_load",
                        [=](dsn::string_view) { rpc.response().err = ERR_OK; });

    const auto &request = rpc.request();
    auto &response = rpc.response();
    response.err = ERR_OK;

    if (!FLAGS_enable_concurrent_bulk_load &&
        !_meta_svc->try_lock_meta_op_status(meta_op_status::BULKLOAD)) {
        response.hint_msg = "meta server is busy now, please wait";
        LOG_ERROR_F("{}", response.hint_msg);
        response.err = ERR_BUSY;
        return;
    }

    std::shared_ptr<app_state> app = get_app(request.app_name);
    if (app == nullptr || app->status != app_status::AS_AVAILABLE) {
        response.err = app == nullptr ? ERR_APP_NOT_EXIST : ERR_APP_DROPPED;
        response.hint_msg = fmt::format(
            "app {} is ", response.err == ERR_APP_NOT_EXIST ? "not existed" : "not available");
        LOG_ERROR_F("{}", response.hint_msg);
        _meta_svc->unlock_meta_op_status();
        return;
    }
    if (app->is_bulk_loading) {
        response.err = ERR_BUSY;
        response.hint_msg = fmt::format("app({}) is already executing bulk load", app->app_name);
        LOG_ERROR_F("{}", response.hint_msg);
        _meta_svc->unlock_meta_op_status();
        return;
    }

    std::string hint_msg;
    error_code e = check_bulk_load_request_params(
        request, app->app_id, app->partition_count, app->envs, hint_msg);
    if (e != ERR_OK) {
        response.err = e;
        response.hint_msg = hint_msg;
        _meta_svc->unlock_meta_op_status();
        return;
    }

    LOG_INFO_F("app({}) start bulk load, cluster_name = {}, provider = {}, remote root path = {}, "
               "ingest_behind = {}",
               request.app_name,
               request.cluster_name,
               request.file_provider_type,
               request.remote_root_path,
               request.ingest_behind);

    // clear old bulk load result
    reset_local_bulk_load_states(app->app_id, app->app_name, true);
    // avoid possible load balancing
    _meta_svc->set_function_level(meta_function_level::fl_steady);

    tasking::enqueue(LPC_META_STATE_NORMAL,
                     _meta_svc->tracker(),
                     [this, rpc, app]() { do_start_app_bulk_load(std::move(app), std::move(rpc)); },
                     server_state::sStateHash);
}

// ThreadPool: THREAD_POOL_META_SERVER
error_code
bulk_load_service::check_bulk_load_request_params(const start_bulk_load_request &request,
                                                  const int32_t app_id,
                                                  const int32_t partition_count,
                                                  const std::map<std::string, std::string> &envs,
                                                  std::string &hint_msg)
{
    FAIL_POINT_INJECT_F("meta_check_bulk_load_request_params",
                        [](dsn::string_view) -> error_code { return ERR_OK; });

    if (!validate_ingest_behind(envs, request.ingest_behind)) {
        hint_msg = fmt::format("inconsistent ingestion behind option");
        LOG_ERROR_F("{}", hint_msg);
        return ERR_INCONSISTENT_STATE;
    }

    auto file_provider = request.file_provider_type;
    // check file provider
    dsn::dist::block_service::block_filesystem *blk_fs =
        _meta_svc->get_block_service_manager().get_or_create_block_filesystem(file_provider);
    if (blk_fs == nullptr) {
        LOG_ERROR_F("invalid remote file provider type: {}", file_provider);
        hint_msg = "invalid file_provider";
        return ERR_INVALID_PARAMETERS;
    }

    // sync get bulk_load_info file_handler
    const std::string remote_path =
        get_bulk_load_info_path(request.app_name, request.cluster_name, request.remote_root_path);
    dsn::dist::block_service::create_file_request cf_req;
    cf_req.file_name = remote_path;
    cf_req.ignore_metadata = true;
    error_code err = ERR_OK;
    dsn::dist::block_service::block_file_ptr file_handler = nullptr;
    blk_fs
        ->create_file(
            cf_req,
            TASK_CODE_EXEC_INLINED,
            [&err, &file_handler](const dsn::dist::block_service::create_file_response &resp) {
                err = resp.err;
                file_handler = resp.file_handle;
            })
        ->wait();
    if (err != ERR_OK || file_handler == nullptr) {
        LOG_ERROR_F(
            "failed to get file({}) handler on remote provider({})", remote_path, file_provider);
        hint_msg = "file_provider error";
        return ERR_FILE_OPERATION_FAILED;
    }

    // sync read bulk_load_info on file provider
    dsn::dist::block_service::read_response r_resp;
    file_handler
        ->read(dsn::dist::block_service::read_request{0, -1},
               TASK_CODE_EXEC_INLINED,
               [&r_resp](const dsn::dist::block_service::read_response &resp) { r_resp = resp; })
        ->wait();
    if (r_resp.err != ERR_OK) {
        LOG_ERROR_F("failed to read file({}) on remote provider({}), error = {}",
                    remote_path,
                    file_provider,
                    r_resp.err.to_string());
        hint_msg = "read bulk_load_info failed";
        return r_resp.err;
    }

    bulk_load_info bl_info;
    if (!::dsn::json::json_forwarder<bulk_load_info>::decode(r_resp.buffer, bl_info)) {
        LOG_ERROR_F("file({}) is damaged on remote file provider({})", remote_path, file_provider);
        hint_msg = "bulk_load_info damaged";
        return ERR_CORRUPTION;
    }

    if (bl_info.app_id != app_id || bl_info.partition_count != partition_count) {
        LOG_ERROR_F("app({}) information is inconsistent, local app_id({}) VS remote app_id({}), "
                    "local partition_count({}) VS remote partition_count({})",
                    request.app_name,
                    app_id,
                    bl_info.app_id,
                    partition_count,
                    bl_info.partition_count);
        hint_msg = "app_id or partition_count is inconsistent";
        return ERR_INCONSISTENT_STATE;
    }

    return ERR_OK;
}

// ThreadPool: THREAD_POOL_META_STATE
void bulk_load_service::do_start_app_bulk_load(std::shared_ptr<app_state> app,
                                               start_bulk_load_rpc rpc)
{
    app_info info = *app;
    info.__set_is_bulk_loading(true);

    blob value = dsn::json::json_forwarder<app_info>::encode(info);
    _meta_svc->get_meta_storage()->set_data(
        _state->get_app_path(*app), std::move(value), [app, rpc, this]() {
            {
                zauto_write_lock l(app_lock());
                app->is_bulk_loading = true;
            }
            {
                zauto_write_lock l(_lock);
                _bulk_load_app_id.insert(app->app_id);
                _apps_in_progress_count[app->app_id] = app->partition_count;
            }
            create_app_bulk_load_dir(
                app->app_name, app->app_id, app->partition_count, std::move(rpc));
        });
}

// ThreadPool: THREAD_POOL_META_STATE
void bulk_load_service::create_app_bulk_load_dir(const std::string &app_name,
                                                 int32_t app_id,
                                                 int32_t partition_count,
                                                 start_bulk_load_rpc rpc)
{
    const auto &req = rpc.request();

    app_bulk_load_info ainfo;
    ainfo.app_id = app_id;
    ainfo.app_name = app_name;
    ainfo.partition_count = partition_count;
    ainfo.status = bulk_load_status::BLS_DOWNLOADING;
    ainfo.cluster_name = req.cluster_name;
    ainfo.file_provider_type = req.file_provider_type;
    ainfo.remote_root_path = req.remote_root_path;
    ainfo.ingest_behind = req.ingest_behind;
    ainfo.is_ever_ingesting = false;
    ainfo.bulk_load_err = ERR_OK;

    _meta_svc->get_meta_storage()->delete_node_recursively(
        get_app_bulk_load_path(app_id), [this, rpc, ainfo]() {
            std::string bulk_load_path = get_app_bulk_load_path(ainfo.app_id);
            LOG_INFO_F("remove app({}) bulk load dir {} succeed", ainfo.app_name, bulk_load_path);

            blob value = dsn::json::json_forwarder<app_bulk_load_info>::encode(ainfo);
            _meta_svc->get_meta_storage()->create_node(
                std::move(bulk_load_path), std::move(value), [this, rpc, ainfo]() {
                    LOG_DEBUG_F("create app({}) bulk load dir", ainfo.app_name);
                    {
                        zauto_write_lock l(_lock);
                        _app_bulk_load_info[ainfo.app_id] = ainfo;
                        _apps_pending_sync_flag[ainfo.app_id] = false;
                        _apps_rollback_count[ainfo.app_id] = 0;
                    }
                    for (int32_t i = 0; i < ainfo.partition_count; ++i) {
                        create_partition_bulk_load_dir(ainfo.app_name,
                                                       gpid(ainfo.app_id, i),
                                                       ainfo.partition_count,
                                                       std::move(rpc));
                    }
                });
        });
}

// ThreadPool: THREAD_POOL_META_STATE
void bulk_load_service::create_partition_bulk_load_dir(const std::string &app_name,
                                                       const gpid &pid,
                                                       int32_t partition_count,
                                                       start_bulk_load_rpc rpc)
{
    partition_bulk_load_info pinfo;
    pinfo.status = bulk_load_status::BLS_DOWNLOADING;
    pinfo.ever_ingest_succeed = false;
    blob value = dsn::json::json_forwarder<partition_bulk_load_info>::encode(pinfo);

    _meta_svc->get_meta_storage()->create_node(
        get_partition_bulk_load_path(pid),
        std::move(value),
        [app_name, pid, partition_count, rpc, pinfo, this]() {
            LOG_DEBUG_F("app({}) create partition({}) bulk_load_info", app_name, pid.to_string());
            {
                zauto_write_lock l(_lock);
                _partition_bulk_load_info[pid] = pinfo;
                _partitions_pending_sync_flag[pid] = false;
                if (--_apps_in_progress_count[pid.get_app_id()] == 0) {
                    LOG_INFO_F("app({}) start bulk load succeed", app_name);
                    _apps_in_progress_count[pid.get_app_id()] = partition_count;
                    rpc.response().err = ERR_OK;
                }
            }
            // start send bulk load to replica servers
            partition_bulk_load(app_name, pid);
        });
}

// ThreadPool: THREAD_POOL_META_STATE
bool bulk_load_service::check_partition_status(
    const std::string &app_name,
    const gpid &pid,
    bool always_unhealthy_check,
    const std::function<void(const std::string &, const gpid &)> &retry_function,
    /*out*/ partition_configuration &pconfig)
{
    std::shared_ptr<app_state> app = get_app(pid.get_app_id());
    if (app == nullptr || app->status != app_status::AS_AVAILABLE) {
        LOG_WARNING_F(
            "app(name={}, id={}, status={}) is not existed or not available, set bulk load failed",
            app_name,
            pid.get_app_id(),
            app ? dsn::enum_to_string(app->status) : "-");
        handle_app_unavailable(pid.get_app_id(), app_name);
        return false;
    }

    pconfig = app->partitions[pid.get_partition_index()];
    if (pconfig.primary.is_invalid()) {
        LOG_WARNING_F("app({}) partition({}) primary is invalid, try it later", app_name, pid);
        tasking::enqueue(LPC_META_STATE_NORMAL,
                         _meta_svc->tracker(),
                         [retry_function, app_name, pid]() { retry_function(app_name, pid); },
                         0,
                         std::chrono::seconds(1));
        return false;
    }

    if (pconfig.secondaries.size() < pconfig.max_replica_count - 1) {
        bulk_load_status::type p_status;
        {
            zauto_read_lock l(_lock);
            p_status = get_partition_bulk_load_status_unlocked(pid);
        }
        // rollback to downloading, pause,cancel,failed bulk load should always send to replica
        // server
        if (!always_unhealthy_check && (p_status == bulk_load_status::BLS_DOWNLOADING ||
                                        p_status == bulk_load_status::BLS_PAUSING ||
                                        p_status == bulk_load_status::BLS_CANCELED ||
                                        p_status == bulk_load_status::BLS_FAILED)) {
            return true;
        }
        LOG_WARNING_F("app({}) partition({}) is unhealthy, status({}), try it later",
                      app_name,
                      pid,
                      dsn::enum_to_string(p_status));
        tasking::enqueue(LPC_META_STATE_NORMAL,
                         _meta_svc->tracker(),
                         [retry_function, app_name, pid]() { retry_function(app_name, pid); },
                         0,
                         std::chrono::seconds(1));
        return false;
    }
    return true;
}

// ThreadPool: THREAD_POOL_META_STATE
void bulk_load_service::partition_bulk_load(const std::string &app_name, const gpid &pid)
{
    FAIL_POINT_INJECT_F("meta_bulk_load_partition_bulk_load", [](dsn::string_view) {});

    partition_configuration pconfig;
    if (!check_partition_status(app_name,
                                pid,
                                false,
                                std::bind(&bulk_load_service::partition_bulk_load,
                                          this,
                                          std::placeholders::_1,
                                          std::placeholders::_2),
                                pconfig)) {
        return;
    }

    rpc_address primary_addr = pconfig.primary;
    auto req = make_unique<bulk_load_request>();
    {
        zauto_read_lock l(_lock);
        const app_bulk_load_info &ainfo = _app_bulk_load_info[pid.get_app_id()];
        req->pid = pid;
        req->app_name = app_name;
        req->primary_addr = primary_addr;
        req->remote_provider_name = ainfo.file_provider_type;
        req->cluster_name = ainfo.cluster_name;
        req->meta_bulk_load_status = get_partition_bulk_load_status_unlocked(pid);
        req->ballot = pconfig.ballot;
        req->query_bulk_load_metadata = is_partition_metadata_not_updated_unlocked(pid);
        req->remote_root_path = ainfo.remote_root_path;
    }

    LOG_INFO_F("send bulk load request to node({}), app({}), partition({}), partition "
               "status = {}, remote provider = {}, cluster_name = {}, remote_root_path = {}",
               primary_addr.to_string(),
               app_name,
               pid,
               dsn::enum_to_string(req->meta_bulk_load_status),
               req->remote_provider_name,
               req->cluster_name,
               req->remote_root_path);

    bulk_load_rpc rpc(std::move(req), RPC_BULK_LOAD, 0_ms, 0, pid.thread_hash());
    rpc.call(primary_addr, _meta_svc->tracker(), [this, rpc](error_code err) mutable {
        on_partition_bulk_load_reply(err, rpc.request(), rpc.response());
    });
}

// ThreadPool: THREAD_POOL_META_STATE
void bulk_load_service::on_partition_bulk_load_reply(error_code err,
                                                     const bulk_load_request &request,
                                                     const bulk_load_response &response)
{
    const std::string &app_name = request.app_name;
    const gpid &pid = request.pid;
    const rpc_address &primary_addr = request.primary_addr;

    if (err != ERR_OK) {
        LOG_ERROR_F(
            "app({}), partition({}) failed to receive bulk load response from node({}), error = {}",
            app_name,
            pid,
            primary_addr.to_string(),
            err.to_string());
        try_rollback_to_downloading(app_name, pid);
        try_resend_bulk_load_request(app_name, pid);
        return;
    }

    if (response.err == ERR_OBJECT_NOT_FOUND || response.err == ERR_INVALID_STATE) {
        LOG_ERROR_F(
            "app({}), partition({}) doesn't exist or has invalid state on node({}), error = {}",
            app_name,
            pid,
            primary_addr.to_string(),
            response.err.to_string());
        try_rollback_to_downloading(app_name, pid);
        try_resend_bulk_load_request(app_name, pid);
        return;
    }

    if (response.err == ERR_BUSY) {
        LOG_WARNING_F(
            "node({}) has enough replicas downloading, wait for next round to send bulk load "
            "request for app({}), partition({})",
            primary_addr.to_string(),
            app_name,
            pid);
        try_resend_bulk_load_request(app_name, pid);
        return;
    }

    if (response.err != ERR_OK) {
        LOG_ERROR_F(
            "app({}), partition({}) from node({}) handle bulk load response failed, error = "
            "{}, primary status = {}",
            app_name,
            pid,
            primary_addr.to_string(),
            response.err.to_string(),
            dsn::enum_to_string(response.primary_bulk_load_status));
        handle_bulk_load_failed(pid.get_app_id(), response.err);
        try_resend_bulk_load_request(app_name, pid);
        return;
    }

    // response.err = ERR_OK
    std::shared_ptr<app_state> app = get_app(pid.get_app_id());
    if (app == nullptr || app->status != app_status::AS_AVAILABLE) {
        LOG_WARNING_F(
            "app(name={}, id={}) is not existed, set bulk load failed", app_name, pid.get_app_id());
        handle_app_unavailable(pid.get_app_id(), app_name);
        return;
    }
    ballot current_ballot = app->partitions[pid.get_partition_index()].ballot;
    if (request.ballot < current_ballot) {
        LOG_WARNING_F(
            "receive out-date response from node({}), app({}), partition({}), request ballot = "
            "{}, current ballot= {}",
            primary_addr.to_string(),
            app_name,
            pid,
            request.ballot,
            current_ballot);
        try_rollback_to_downloading(app_name, pid);
        try_resend_bulk_load_request(app_name, pid);
        return;
    }

    // handle bulk load states reported from primary replica
    bulk_load_status::type app_status = get_app_bulk_load_status(response.pid.get_app_id());
    switch (app_status) {
    case bulk_load_status::BLS_DOWNLOADING:
        handle_app_downloading(response, primary_addr);
        break;
    case bulk_load_status::BLS_DOWNLOADED:
        update_partition_info_on_remote_storage(
            response.app_name, response.pid, bulk_load_status::BLS_INGESTING);
        // when app status is downloaded or ingesting, send request frequently
        break;
    case bulk_load_status::BLS_INGESTING:
        handle_app_ingestion(response, primary_addr);
        break;
    case bulk_load_status::BLS_SUCCEED:
    case bulk_load_status::BLS_FAILED:
    case bulk_load_status::BLS_CANCELED:
        handle_bulk_load_finish(response, primary_addr);
        break;
    case bulk_load_status::BLS_PAUSING:
        handle_app_pausing(response, primary_addr);
        break;
    case bulk_load_status::BLS_PAUSED:
        // paused not send request to replica servers
        return;
    default:
        // do nothing in other status
        break;
    }

    try_resend_bulk_load_request(app_name, pid);
}

// ThreadPool: THREAD_POOL_META_STATE
void bulk_load_service::try_resend_bulk_load_request(const std::string &app_name, const gpid &pid)
{
    FAIL_POINT_INJECT_F("meta_bulk_load_resend_request", [](dsn::string_view) {});
    zauto_read_lock l(_lock);
    if (is_app_bulk_loading_unlocked(pid.get_app_id())) {
        tasking::enqueue(LPC_META_STATE_NORMAL,
                         _meta_svc->tracker(),
                         std::bind(&bulk_load_service::partition_bulk_load, this, app_name, pid),
                         0,
                         std::chrono::seconds(bulk_load_constant::BULK_LOAD_REQUEST_INTERVAL));
    }
}

// ThreadPool: THREAD_POOL_META_STATE
void bulk_load_service::handle_app_downloading(const bulk_load_response &response,
                                               const rpc_address &primary_addr)
{
    const std::string &app_name = response.app_name;
    const gpid &pid = response.pid;

    if (!response.__isset.total_download_progress) {
        LOG_WARNING_F(
            "receive bulk load response from node({}) app({}), partition({}), primary_status({}), "
            "but total_download_progress is not set",
            primary_addr.to_string(),
            app_name,
            pid,
            dsn::enum_to_string(response.primary_bulk_load_status));
        return;
    }

    for (const auto &kv : response.group_bulk_load_state) {
        const auto &bulk_load_states = kv.second;
        if (!bulk_load_states.__isset.download_progress ||
            !bulk_load_states.__isset.download_status) {
            LOG_WARNING_F("receive bulk load response from node({}) app({}), partition({}), "
                          "primary_status({}), but node({}) progress or status is not set",
                          primary_addr.to_string(),
                          app_name,
                          pid,
                          dsn::enum_to_string(response.primary_bulk_load_status),
                          kv.first.to_string());
            return;
        }
        // check partition download status
        if (bulk_load_states.download_status != ERR_OK) {
            LOG_ERROR_F("app({}) partition({}) on node({}) meet unrecoverable error during "
                        "downloading files, error = {}",
                        app_name,
                        pid,
                        kv.first.to_string(),
                        bulk_load_states.download_status);

            error_code err = ERR_UNKNOWN;
            // ERR_FILE_OPERATION_FAILED: local file system error
            // ERR_FS_INTERNAL: remote file system error
            // ERR_CORRUPTION: file not exist or damaged
            if (ERR_FILE_OPERATION_FAILED == bulk_load_states.download_status ||
                ERR_FS_INTERNAL == bulk_load_states.download_status ||
                ERR_CORRUPTION == bulk_load_states.download_status) {
                err = bulk_load_states.download_status;
            }
            handle_bulk_load_failed(pid.get_app_id(), err);
            return;
        }
    }

    // if replica report metadata, update metadata on remote storage
    if (response.__isset.metadata && is_partition_metadata_not_updated(pid)) {
        update_partition_metadata_on_remote_storage(app_name, pid, response.metadata);
    }

    // update download progress
    int32_t total_progress = response.total_download_progress;
    LOG_INFO_F(
        "receive bulk load response from node({}) app({}) partition({}), primary_status({}), "
        "total_download_progress = {}",
        primary_addr.to_string(),
        app_name,
        pid,
        dsn::enum_to_string(response.primary_bulk_load_status),
        total_progress);
    {
        zauto_write_lock l(_lock);
        _partitions_total_download_progress[pid] = total_progress;
        _partitions_bulk_load_state[pid] = response.group_bulk_load_state;
    }

    // update partition status to `downloaded` if all replica downloaded
    if (total_progress >= bulk_load_constant::PROGRESS_FINISHED) {
        LOG_INFO_F(
            "app({}) partirion({}) download all files from remote provider succeed", app_name, pid);
        update_partition_info_on_remote_storage(app_name, pid, bulk_load_status::BLS_DOWNLOADED);
    }
}

// ThreadPool: THREAD_POOL_META_STATE
void bulk_load_service::handle_app_ingestion(const bulk_load_response &response,
                                             const rpc_address &primary_addr)
{
    const std::string &app_name = response.app_name;
    const gpid &pid = response.pid;

    if (!response.__isset.is_group_ingestion_finished) {
        LOG_WARNING_F("receive bulk load response from node({}) app({}) partition({}), "
                      "primary_status({}), but is_group_ingestion_finished is not set",
                      primary_addr.to_string(),
                      app_name,
                      pid,
                      dsn::enum_to_string(response.primary_bulk_load_status));
        return;
    }

    for (const auto &kv : response.group_bulk_load_state) {
        const auto &bulk_load_states = kv.second;
        if (!bulk_load_states.__isset.ingest_status) {
            LOG_WARNING_F("receive bulk load response from node({}) app({}) partition({}), "
                          "primary_status({}), but node({}) ingestion_status is not set",
                          primary_addr.to_string(),
                          app_name,
                          pid,
                          dsn::enum_to_string(response.primary_bulk_load_status),
                          kv.first.to_string());
            return;
        }

        if (bulk_load_states.ingest_status == ingestion_status::IS_FAILED) {
            LOG_ERROR_F("app({}) partition({}) on node({}) ingestion failed",
                        app_name,
                        pid,
                        kv.first.to_string());
            finish_ingestion(pid);
            handle_bulk_load_failed(pid.get_app_id(), ERR_INGESTION_FAILED);
            return;
        }
    }

    LOG_INFO_F(
        "receive bulk load response from node({}) app({}) partition({}), primary_status({}), "
        "is_group_ingestion_finished = {}",
        primary_addr.to_string(),
        app_name,
        pid,
        dsn::enum_to_string(response.primary_bulk_load_status),
        response.is_group_ingestion_finished);
    {
        zauto_write_lock l(_lock);
        _partitions_bulk_load_state[pid] = response.group_bulk_load_state;
    }

    if (response.is_group_ingestion_finished) {
        LOG_INFO_F("app({}) partition({}) ingestion files succeed", app_name, pid);
        finish_ingestion(pid);
        update_partition_info_on_remote_storage(app_name, pid, bulk_load_status::BLS_SUCCEED);
    }
}

// ThreadPool: THREAD_POOL_META_STATE
void bulk_load_service::handle_bulk_load_finish(const bulk_load_response &response,
                                                const rpc_address &primary_addr)
{
    const std::string &app_name = response.app_name;
    const gpid &pid = response.pid;

    if (!response.__isset.is_group_bulk_load_context_cleaned_up) {
        LOG_WARNING_F("receive bulk load response from node({}) app({}) partition({}), "
                      "primary_status({}), but is_group_bulk_load_context_cleaned_up is not set",
                      primary_addr.to_string(),
                      app_name,
                      pid,
                      dsn::enum_to_string(response.primary_bulk_load_status));
        return;
    }

    for (const auto &kv : response.group_bulk_load_state) {
        if (!kv.second.__isset.is_cleaned_up) {
            LOG_WARNING_F("receive bulk load response from node({}) app({}), partition({}), "
                          "primary_status({}), but node({}) is_cleaned_up is not set",
                          primary_addr.to_string(),
                          app_name,
                          pid,
                          dsn::enum_to_string(response.primary_bulk_load_status),
                          kv.first.to_string());
            return;
        }
    }

    {
        zauto_read_lock l(_lock);
        if (_partitions_cleaned_up[pid]) {
            LOG_WARNING_F(
                "receive bulk load response from node({}) app({}) partition({}), current partition "
                "has already been cleaned up",
                primary_addr.to_string(),
                app_name,
                pid);
            return;
        }
    }

    // The replicas have cleaned up their bulk load states and removed temporary sst files
    bool group_cleaned_up = response.is_group_bulk_load_context_cleaned_up;
    LOG_INFO_F(
        "receive bulk load response from node({}) app({}) partition({}), primary status = {}, "
        "is_group_bulk_load_context_cleaned_up = {}",
        primary_addr.to_string(),
        app_name,
        pid,
        dsn::enum_to_string(response.primary_bulk_load_status),
        group_cleaned_up);
    {
        zauto_write_lock l(_lock);
        _partitions_cleaned_up[pid] = group_cleaned_up;
        _partitions_bulk_load_state[pid] = response.group_bulk_load_state;
    }

    if (group_cleaned_up) {
        int32_t count = 0;
        {
            zauto_write_lock l(_lock);
            count = --_apps_in_progress_count[pid.get_app_id()];
        }
        if (count == 0) {
            std::shared_ptr<app_state> app = get_app(pid.get_app_id());
            if (app == nullptr || app->status != app_status::AS_AVAILABLE) {
                LOG_WARNING_F("app(name={}, id={}) is not existed, remove bulk load dir on remote "
                              "storage",
                              app_name,
                              pid.get_app_id());
                remove_bulk_load_dir_on_remote_storage(pid.get_app_id(), app_name);
                return;
            }
            LOG_INFO_F("app({}) update app to not bulk loading", app_name);
            update_app_not_bulk_loading_on_remote_storage(std::move(app));
            reset_local_bulk_load_states(pid.get_app_id(), app_name, false);
        }
    }
}

// ThreadPool: THREAD_POOL_META_STATE
void bulk_load_service::handle_app_pausing(const bulk_load_response &response,
                                           const rpc_address &primary_addr)
{
    const std::string &app_name = response.app_name;
    const gpid &pid = response.pid;

    if (!response.__isset.is_group_bulk_load_paused) {
        LOG_WARNING_F("receive bulk load response from node({}) app({}) partition({}), "
                      "primary_status({}), but is_group_bulk_load_paused is not set",
                      primary_addr.to_string(),
                      app_name,
                      pid,
                      dsn::enum_to_string(response.primary_bulk_load_status));
        return;
    }

    for (const auto &kv : response.group_bulk_load_state) {
        if (!kv.second.__isset.is_paused) {
            LOG_WARNING_F("receive bulk load response from node({}) app({}), partition({}), "
                          "primary_status({}), but node({}) is_paused is not set",
                          primary_addr.to_string(),
                          app_name,
                          pid,
                          dsn::enum_to_string(response.primary_bulk_load_status),
                          kv.first.to_string());
            return;
        }
    }

    bool is_group_paused = response.is_group_bulk_load_paused;
    LOG_INFO_F(
        "receive bulk load response from node({}) app({}) partition({}), primary status = {}, "
        "is_group_bulk_load_paused = {}",
        primary_addr.to_string(),
        app_name,
        pid,
        dsn::enum_to_string(response.primary_bulk_load_status),
        is_group_paused);
    {
        zauto_write_lock l(_lock);
        _partitions_bulk_load_state[pid] = response.group_bulk_load_state;
    }

    if (is_group_paused) {
        LOG_INFO_F("app({}) partirion({}) pause bulk load succeed", response.app_name, pid);
        update_partition_info_on_remote_storage(
            response.app_name, pid, bulk_load_status::BLS_PAUSED);
    }
}

// ThreadPool: THREAD_POOL_META_STATE
void bulk_load_service::try_rollback_to_downloading(const std::string &app_name, const gpid &pid)
{
    zauto_write_lock l(_lock);

    const auto app_status = get_app_bulk_load_status_unlocked(pid.get_app_id());
    if (app_status != bulk_load_status::BLS_DOWNLOADING &&
        app_status != bulk_load_status::BLS_DOWNLOADED &&
        app_status != bulk_load_status::BLS_INGESTING) {
        LOG_INFO_F("app({}) status={}, no need to rollback to downloading, wait for next round",
                   app_name,
                   dsn::enum_to_string(app_status));
        return;
    }

    if (_apps_rolling_back[pid.get_app_id()]) {
        LOG_WARNING_F("app({}) is rolling back to downloading, ignore this request", app_name);
        return;
    }
    if (_apps_rollback_count[pid.get_app_id()] >= FLAGS_bulk_load_max_rollback_times) {
        LOG_WARNING_F(
            "app({}) has been rollback to downloading for {} times, make bulk load process failed",
            app_name,
            _apps_rollback_count[pid.get_app_id()]);

        update_app_status_on_remote_storage_unlocked(
            pid.get_app_id(),
            bulk_load_status::BLS_FAILED,
            _app_bulk_load_info[pid.get_app_id()].is_ever_ingesting ? ERR_INGESTION_FAILED
                                                                    : ERR_RETRY_EXHAUSTED);
        return;
    }
    LOG_INFO_F("app({}) will rolling back from {} to {}, current rollback_count = {}",
               app_name,
               dsn::enum_to_string(app_status),
               dsn::enum_to_string(bulk_load_status::BLS_DOWNLOADING),
               _apps_rollback_count[pid.get_app_id()]);
    _apps_rolling_back[pid.get_app_id()] = true;
    _apps_rollback_count[pid.get_app_id()]++;
    update_app_status_on_remote_storage_unlocked(pid.get_app_id(),
                                                 bulk_load_status::type::BLS_DOWNLOADING);
}

// ThreadPool: THREAD_POOL_META_STATE
void bulk_load_service::handle_bulk_load_failed(int32_t app_id, error_code err)
{
    zauto_write_lock l(_lock);
    if (!_apps_cleaning_up[app_id]) {
        _apps_cleaning_up[app_id] = true;
        update_app_status_on_remote_storage_unlocked(app_id, bulk_load_status::BLS_FAILED, err);
    }
}

// ThreadPool: THREAD_POOL_META_STATE
void bulk_load_service::handle_app_unavailable(int32_t app_id, const std::string &app_name)
{
    zauto_write_lock l(_lock);
    if (is_app_bulk_loading_unlocked(app_id) && !_apps_cleaning_up[app_id]) {
        _apps_cleaning_up[app_id] = true;
        reset_local_bulk_load_states_unlocked(app_id, app_name, false);
    }
}

// ThreadPool: THREAD_POOL_META_STATE
void bulk_load_service::update_partition_metadata_on_remote_storage(
    const std::string &app_name, const gpid &pid, const bulk_load_metadata &metadata)
{
    zauto_read_lock l(_lock);
    partition_bulk_load_info pinfo = _partition_bulk_load_info[pid];
    pinfo.metadata = metadata;
    blob value = json::json_forwarder<partition_bulk_load_info>::encode(pinfo);

    _meta_svc->get_meta_storage()->set_data(
        get_partition_bulk_load_path(pid), std::move(value), [this, app_name, pid, pinfo]() {
            zauto_write_lock l(_lock);
            _partition_bulk_load_info[pid] = pinfo;
            LOG_INFO_F(
                "app({}) update partition({}) bulk load metadata, file count = {}, file size = {}",
                app_name,
                pid,
                pinfo.metadata.files.size(),
                pinfo.metadata.file_total_size);
        });
}

// ThreadPool: THREAD_POOL_META_STATE
void bulk_load_service::update_partition_info_on_remote_storage(const std::string &app_name,
                                                                const gpid &pid,
                                                                bulk_load_status::type new_status,
                                                                bool should_send_request)
{
    zauto_write_lock l(_lock);
    partition_bulk_load_info pinfo = _partition_bulk_load_info[pid];
    if (pinfo.status == new_status && new_status != bulk_load_status::BLS_DOWNLOADING) {
        LOG_WARNING_F("app({}) partition({}) old status:{} VS new status:{}, ignore it",
                      app_name,
                      pid,
                      dsn::enum_to_string(pinfo.status),
                      dsn::enum_to_string(new_status));
        return;
    }

    if (_partitions_pending_sync_flag[pid]) {
        if (_apps_rolling_back[pid.get_app_id()] &&
            new_status == bulk_load_status::BLS_DOWNLOADING) {
            LOG_WARNING_F(
                "app({}) partition({}) has already sync bulk load status, current_status = {}, "
                "wait and retry to set status as {}",
                app_name,
                pid,
                dsn::enum_to_string(pinfo.status),
                dsn::enum_to_string(new_status));
            tasking::enqueue(LPC_META_STATE_NORMAL,
                             _meta_svc->tracker(),
                             std::bind(&bulk_load_service::update_partition_info_on_remote_storage,
                                       this,
                                       app_name,
                                       pid,
                                       new_status,
                                       should_send_request),
                             0,
                             std::chrono::seconds(1));
        } else {
            LOG_INFO_F("app({}) partition({}) has already sync bulk load status, current_status = "
                       "{}, new_status = {}, wait for next round",
                       app_name,
                       pid,
                       dsn::enum_to_string(pinfo.status),
                       dsn::enum_to_string(new_status));
        }
        return;
    }

    _partitions_pending_sync_flag[pid] = true;
    update_partition_info_unlock(pid, new_status, pinfo);

    blob value = json::json_forwarder<partition_bulk_load_info>::encode(pinfo);
    _meta_svc->get_meta_storage()->set_data(
        get_partition_bulk_load_path(pid),
        std::move(value),
        std::bind(&bulk_load_service::update_partition_info_on_remote_storage_reply,
                  this,
                  app_name,
                  pid,
                  pinfo,
                  should_send_request));
}

// ThreadPool: THREAD_POOL_META_STATE
void bulk_load_service::update_partition_info_unlock(const gpid &pid,
                                                     bulk_load_status::type new_status,
                                                     /*out*/ partition_bulk_load_info &pinfo)
{
    auto old_status = pinfo.status;
    pinfo.status = new_status;
    if (old_status != bulk_load_status::BLS_INGESTING ||
        new_status != bulk_load_status::BLS_SUCCEED ||
        _partitions_bulk_load_state.find(pid) == _partitions_bulk_load_state.end()) {
        // no need to update other field of partition_bulk_load_info
        return;
    }
    pinfo.addresses.clear();
    const auto &state = _partitions_bulk_load_state[pid];
    for (const auto &kv : state) {
        pinfo.addresses.emplace_back(kv.first);
    }
    pinfo.ever_ingest_succeed = true;
}

// ThreadPool: THREAD_POOL_META_STATE
void bulk_load_service::update_partition_info_on_remote_storage_reply(
    const std::string &app_name,
    const gpid &pid,
    const partition_bulk_load_info &new_info,
    bool should_send_request)
{
    {
        zauto_write_lock l(_lock);
        auto old_status = _partition_bulk_load_info[pid].status;
        auto new_status = new_info.status;
        _partition_bulk_load_info[pid] = new_info;
        _partitions_pending_sync_flag[pid] = false;

        LOG_INFO_F("app({}) update partition({}) status from {} to {}",
                   app_name,
                   pid,
                   dsn::enum_to_string(old_status),
                   dsn::enum_to_string(new_status));

        switch (new_status) {
        case bulk_load_status::BLS_DOWNLOADED:
        case bulk_load_status::BLS_INGESTING:
        case bulk_load_status::BLS_SUCCEED:
        case bulk_load_status::BLS_PAUSED:
            if (old_status != new_status && !_apps_rolling_back[pid.get_app_id()] &&
                --_apps_in_progress_count[pid.get_app_id()] == 0) {
                update_app_status_on_remote_storage_unlocked(pid.get_app_id(), new_status);
            }
            break;
        case bulk_load_status::BLS_DOWNLOADING: {
            _partitions_bulk_load_state.erase(pid);
            _partitions_total_download_progress[pid] = 0;
            _partitions_cleaned_up[pid] = false;

            if (--_apps_in_progress_count[pid.get_app_id()] == 0) {
                _apps_in_progress_count[pid.get_app_id()] =
                    _app_bulk_load_info[pid.get_app_id()].partition_count;
                _apps_rolling_back[pid.get_app_id()] = false;
                LOG_INFO_F("app({}) restart to bulk load", app_name);
            }
        } break;
        default:
            // do nothing in other status
            break;
        }
    }
    if (should_send_request) {
        partition_bulk_load(app_name, pid);
    }
}

// ThreadPool: THREAD_POOL_META_STATE
void bulk_load_service::update_app_status_on_remote_storage_unlocked(
    int32_t app_id, bulk_load_status::type new_status, error_code err, bool should_send_request)
{
    FAIL_POINT_INJECT_F("meta_update_app_status_on_remote_storage_unlocked",
                        [](dsn::string_view) {});

    app_bulk_load_info ainfo = _app_bulk_load_info[app_id];
    auto old_status = ainfo.status;

    if (old_status == new_status && new_status != bulk_load_status::BLS_DOWNLOADING) {
        LOG_WARNING_F("app({}) old status:{} VS new status:{}, ignore it",
                      ainfo.app_name,
                      dsn::enum_to_string(old_status),
                      dsn::enum_to_string(new_status));
        return;
    }

    if (_apps_pending_sync_flag[app_id]) {
        LOG_INFO_F(
            "app({}) has already sync bulk load status, wait and retry, current status = {}, "
            "new status = {}",
            ainfo.app_name,
            dsn::enum_to_string(old_status),
            dsn::enum_to_string(new_status));
        tasking::enqueue(LPC_META_STATE_NORMAL,
                         _meta_svc->tracker(),
                         std::bind(&bulk_load_service::update_app_status_on_remote_storage_unlocked,
                                   this,
                                   app_id,
                                   new_status,
                                   err,
                                   should_send_request),
                         0,
                         std::chrono::seconds(1));
        return;
    }

    _apps_pending_sync_flag[app_id] = true;

    if (bulk_load_status::BLS_INGESTING == new_status) {
        ainfo.is_ever_ingesting = true;
    }
    ainfo.status = new_status;
    ainfo.bulk_load_err = err;
    blob value = dsn::json::json_forwarder<app_bulk_load_info>::encode(ainfo);

    _meta_svc->get_meta_storage()->set_data(
        get_app_bulk_load_path(app_id),
        std::move(value),
        std::bind(&bulk_load_service::update_app_status_on_remote_storage_reply,
                  this,
                  ainfo,
                  old_status,
                  new_status,
                  should_send_request));
}

// ThreadPool: THREAD_POOL_META_STATE
void bulk_load_service::update_app_status_on_remote_storage_reply(const app_bulk_load_info &ainfo,
                                                                  bulk_load_status::type old_status,
                                                                  bulk_load_status::type new_status,
                                                                  bool should_send_request)
{
    int32_t app_id = ainfo.app_id;
    int32_t partition_count = ainfo.partition_count;
    {
        zauto_write_lock l(_lock);
        _app_bulk_load_info[app_id] = ainfo;
        _apps_pending_sync_flag[app_id] = false;
        _apps_in_progress_count[app_id] = partition_count;
        // when rollback from ingesting, ingesting_count should be reset
        if (old_status == bulk_load_status::BLS_INGESTING &&
            new_status == bulk_load_status::BLS_DOWNLOADING) {
            reset_app_ingestion(app_id);
        }
    }

    LOG_INFO_F("update app({}) status from {} to {}",
               ainfo.app_name,
               dsn::enum_to_string(old_status),
               dsn::enum_to_string(new_status));

    if (new_status == bulk_load_status::BLS_INGESTING) {
        for (auto i = 0; i < partition_count; ++i) {
            partition_ingestion(ainfo.app_name, gpid(app_id, i));
        }
    }

    if (new_status == bulk_load_status::BLS_PAUSING ||
        new_status == bulk_load_status::BLS_DOWNLOADING ||
        new_status == bulk_load_status::BLS_CANCELED ||
        new_status == bulk_load_status::BLS_FAILED) {
        for (int i = 0; i < ainfo.partition_count; ++i) {
            update_partition_info_on_remote_storage(
                ainfo.app_name, gpid(app_id, i), new_status, should_send_request);
        }
    }
}

// ThreadPool: THREAD_POOL_META_STATE
bool bulk_load_service::check_ever_ingestion_succeed(const partition_configuration &config,
                                                     const std::string &app_name,
                                                     const gpid &pid)
{
    partition_bulk_load_info pinfo;
    {
        zauto_read_lock l(_lock);
        pinfo = _partition_bulk_load_info[pid];
    }

    if (!pinfo.ever_ingest_succeed) {
        return false;
    }

    std::vector<rpc_address> current_nodes;
    current_nodes.emplace_back(config.primary);
    for (const auto &secondary : config.secondaries) {
        current_nodes.emplace_back(secondary);
    }

    std::sort(pinfo.addresses.begin(), pinfo.addresses.end());
    std::sort(current_nodes.begin(), current_nodes.end());
    if (current_nodes == pinfo.addresses) {
        LOG_INFO_F("app({}) partition({}) has already executed ingestion succeed", app_name, pid);
        update_partition_info_on_remote_storage(app_name, pid, bulk_load_status::BLS_SUCCEED);
        return true;
    }

    LOG_WARNING_F("app({}) partition({}) configuration changed, should executed ingestion again",
                  app_name,
                  pid);
    return false;
}

// ThreadPool: THREAD_POOL_META_STATE
void bulk_load_service::partition_ingestion(const std::string &app_name, const gpid &pid)
{
    FAIL_POINT_INJECT_F("meta_bulk_load_partition_ingestion", [](dsn::string_view) {});

    auto app_status = get_app_bulk_load_status(pid.get_app_id());
    if (app_status != bulk_load_status::BLS_INGESTING) {
        LOG_WARNING_F("app({}) current status is {}, partition({}), ignore it",
                      app_name,
                      dsn::enum_to_string(app_status),
                      pid);
        return;
    }

    if (is_partition_metadata_not_updated(pid)) {
        LOG_ERROR_F("app({}) partition({}) doesn't have bulk load metadata, set bulk load failed",
                    app_name,
                    pid);
        handle_bulk_load_failed(pid.get_app_id(), ERR_CORRUPTION);
        return;
    }

    partition_configuration pconfig;
    if (!check_partition_status(app_name,
                                pid,
                                true,
                                std::bind(&bulk_load_service::partition_ingestion,
                                          this,
                                          std::placeholders::_1,
                                          std::placeholders::_2),
                                pconfig)) {
        return;
    }

    if (check_ever_ingestion_succeed(pconfig, app_name, pid)) {
        return;
    }

    auto app = get_app(pid.get_app_id());
    if (!try_partition_ingestion(pconfig, app->helpers->contexts[pid.get_partition_index()])) {
        LOG_WARNING_F(
            "app({}) partition({}) couldn't execute ingestion, wait and try later", app_name, pid);
        tasking::enqueue(LPC_META_STATE_NORMAL,
                         _meta_svc->tracker(),
                         std::bind(&bulk_load_service::partition_ingestion, this, app_name, pid),
                         pid.thread_hash(),
                         std::chrono::seconds(5));
        return;
    }

    rpc_address primary_addr = pconfig.primary;
    ballot meta_ballot = pconfig.ballot;
    tasking::enqueue(LPC_BULK_LOAD_INGESTION,
                     _meta_svc->tracker(),
                     std::bind(&bulk_load_service::send_ingestion_request,
                               this,
                               app_name,
                               pid,
                               primary_addr,
                               meta_ballot),
                     0,
                     std::chrono::seconds(bulk_load_constant::BULK_LOAD_REQUEST_INTERVAL));
}

// ThreadPool: THREAD_POOL_DEFAULT
void bulk_load_service::send_ingestion_request(const std::string &app_name,
                                               const gpid &pid,
                                               const rpc_address &primary_addr,
                                               const ballot &meta_ballot)
{
    ingestion_request req;
    req.app_name = app_name;
    req.ballot = meta_ballot;
    req.verify_before_ingest = FLAGS_bulk_load_verify_before_ingest;
    {
        zauto_read_lock l(_lock);
        req.metadata = _partition_bulk_load_info[pid].metadata;
        req.ingest_behind = _app_bulk_load_info[pid.get_app_id()].ingest_behind;
    }
    // create a client request, whose gpid field in header should be pid
    message_ex *msg = message_ex::create_request(dsn::apps::RPC_RRDB_RRDB_BULK_LOAD,
                                                 0,
                                                 pid.thread_hash(),
                                                 static_cast<uint64_t>(pid.get_partition_index()));
    auto &hdr = *msg->header;
    hdr.gpid = pid;
    dsn::marshall(msg, req);
    dsn::rpc_response_task_ptr rpc_callback = rpc::create_rpc_response_task(
        msg,
        _meta_svc->tracker(),
        [this, app_name, pid, primary_addr](error_code err, ingestion_response &&resp) {
            on_partition_ingestion_reply(err, std::move(resp), app_name, pid, primary_addr);
        });
    _meta_svc->send_request(msg, primary_addr, rpc_callback);
    LOG_INFO_F("send ingest_request to node({}), app({}) partition({})",
               primary_addr.to_string(),
               app_name,
               pid);
}

// ThreadPool: THREAD_POOL_DEFAULT
void bulk_load_service::on_partition_ingestion_reply(error_code err,
                                                     const ingestion_response &&resp,
                                                     const std::string &app_name,
                                                     const gpid &pid,
                                                     const rpc_address &primary_addr)
{
    if (err != ERR_OK || resp.err != ERR_OK || resp.rocksdb_error != ERR_OK) {
        finish_ingestion(pid);
    }

    if (err == ERR_NO_NEED_OPERATE) {
        LOG_WARNING_F(
            "app({}) partition({}) on node({}) has already executing ingestion, ignore this "
            "repeated request",
            app_name,
            pid,
            primary_addr.to_string());
        return;
    }

    // if meet 2pc error, ingesting will rollback to downloading, no need to retry here
    if (err != ERR_OK) {
        LOG_ERROR_F("app({}) partition({}) on node({}) ingestion files failed, error = {}",
                    app_name,
                    pid,
                    primary_addr.to_string(),
                    err);
        tasking::enqueue(
            LPC_META_STATE_NORMAL,
            _meta_svc->tracker(),
            std::bind(&bulk_load_service::try_rollback_to_downloading, this, app_name, pid));
        return;
    }

    if (resp.err == ERR_TRY_AGAIN && resp.rocksdb_error != 0) {
        LOG_ERROR_F("app({}) partition({}) on node({}) ingestion files failed while empty write, "
                    "rocksdb error = "
                    "{}, retry it later",
                    app_name,
                    pid,
                    primary_addr.to_string(),
                    resp.rocksdb_error);
        tasking::enqueue(LPC_BULK_LOAD_INGESTION,
                         _meta_svc->tracker(),
                         std::bind(&bulk_load_service::partition_ingestion, this, app_name, pid),
                         0,
                         std::chrono::milliseconds(10));
        return;
    }

    // some unexpected errors happened, such as write empty write failed but rocksdb_error is ok
    // stop bulk load process with failed
    if (resp.err != ERR_OK || resp.rocksdb_error != 0) {
        LOG_ERROR_F(
            "app({}) partition({}) on node({}) failed to ingestion files, error = {}, rocksdb "
            "error = {}",
            app_name,
            pid,
            primary_addr.to_string(),
            resp.err,
            resp.rocksdb_error);

        tasking::enqueue(LPC_META_STATE_NORMAL,
                         _meta_svc->tracker(),
                         std::bind(&bulk_load_service::handle_bulk_load_failed,
                                   this,
                                   pid.get_app_id(),
                                   ERR_INGESTION_FAILED));
        return;
    }

    LOG_INFO_F("app({}) partition({}) receive ingestion response from node({}) succeed",
               app_name,
               pid,
               primary_addr.to_string());
}

// ThreadPool: THREAD_POOL_META_STATE
void bulk_load_service::remove_bulk_load_dir_on_remote_storage(int32_t app_id,
                                                               const std::string &app_name)
{
    std::string bulk_load_path = get_app_bulk_load_path(app_id);
    _meta_svc->get_meta_storage()->delete_node_recursively(
        std::move(bulk_load_path), [this, app_id, app_name, bulk_load_path]() {
            LOG_INFO_F("remove app({}) bulk load dir {} succeed", app_name, bulk_load_path);
            reset_local_bulk_load_states(app_id, app_name, true);
        });
}

// ThreadPool: THREAD_POOL_META_STATE
void bulk_load_service::remove_bulk_load_dir_on_remote_storage(std::shared_ptr<app_state> app,
                                                               bool set_app_not_bulk_loading)
{
    std::string bulk_load_path = get_app_bulk_load_path(app->app_id);
    _meta_svc->get_meta_storage()->delete_node_recursively(
        std::move(bulk_load_path), [this, app, set_app_not_bulk_loading, bulk_load_path]() {
            LOG_INFO_F("remove app({}) bulk load dir {} succeed", app->app_name, bulk_load_path);
            reset_local_bulk_load_states(app->app_id, app->app_name, true);
            if (set_app_not_bulk_loading) {
                update_app_not_bulk_loading_on_remote_storage(std::move(app));
            }
        });
}

// ThreadPool: THREAD_POOL_META_STATE
template <typename T>
inline void erase_map_elem_by_id(int32_t app_id, std::unordered_map<gpid, T> &mymap)
{
    for (auto iter = mymap.begin(); iter != mymap.end();) {
        if (iter->first.get_app_id() == app_id) {
            mymap.erase(iter++);
        } else {
            iter++;
        }
    }
}

// ThreadPool: THREAD_POOL_META_STATE
void bulk_load_service::reset_local_bulk_load_states_unlocked(int32_t app_id,
                                                              const std::string &app_name,
                                                              bool is_reset_result)
{
    _apps_in_progress_count.erase(app_id);
    _apps_pending_sync_flag.erase(app_id);
    erase_map_elem_by_id(app_id, _partitions_pending_sync_flag);
    erase_map_elem_by_id(app_id, _partitions_total_download_progress);
    _apps_rolling_back.erase(app_id);
    _apps_rollback_count.erase(app_id);
    reset_app_ingestion(app_id);
    _bulk_load_app_id.erase(app_id);

    if (is_reset_result) {
        _app_bulk_load_info.erase(app_id);
        erase_map_elem_by_id(app_id, _partitions_bulk_load_state);
        erase_map_elem_by_id(app_id, _partition_bulk_load_info);
        erase_map_elem_by_id(app_id, _partitions_cleaned_up);
        _apps_cleaning_up.erase(app_id);
    }

    LOG_INFO_F(
        "reset local app({}) bulk load context, is_reset_result({})", app_name, is_reset_result);
}

// ThreadPool: THREAD_POOL_META_STATE
void bulk_load_service::reset_local_bulk_load_states(int32_t app_id,
                                                     const std::string &app_name,
                                                     bool is_reset_result)
{
    zauto_write_lock l(_lock);
    reset_local_bulk_load_states_unlocked(app_id, app_name, is_reset_result);
}

// ThreadPool: THREAD_POOL_META_STATE
void bulk_load_service::update_app_not_bulk_loading_on_remote_storage(
    std::shared_ptr<app_state> app)
{
    app_info info = *app;
    info.__set_is_bulk_loading(false);

    blob value = dsn::json::json_forwarder<app_info>::encode(info);
    _meta_svc->get_meta_storage()->set_data(
        _state->get_app_path(*app), std::move(value), [app, this]() {
            zauto_write_lock l(app_lock());
            app->is_bulk_loading = false;
            LOG_INFO_F("app({}) update app is_bulk_loading to false", app->app_name);
            _meta_svc->unlock_meta_op_status();
        });
}

// ThreadPool: THREAD_POOL_META_STATE
void bulk_load_service::on_control_bulk_load(control_bulk_load_rpc rpc)
{
    const std::string &app_name = rpc.request().app_name;
    const auto &control_type = rpc.request().type;
    auto &response = rpc.response();
    response.err = ERR_OK;

    std::shared_ptr<app_state> app = get_app(app_name);
    if (app == nullptr || app->status != app_status::AS_AVAILABLE) {
        LOG_ERROR_F("app({}) is not existed or not available", app_name);
        response.err = app == nullptr ? ERR_APP_NOT_EXIST : ERR_APP_DROPPED;
        response.__set_hint_msg(fmt::format("app({}) is not existed or not available", app_name));
        return;
    }

    if (!app->is_bulk_loading) {
        LOG_ERROR_F("app({}) is not executing bulk load", app_name);
        response.err = ERR_INACTIVE_STATE;
        response.__set_hint_msg(fmt::format("app({}) is not executing bulk load", app_name));
        return;
    }
    int32_t app_id = app->app_id;

    zauto_write_lock l(_lock);
    const auto &app_status = get_app_bulk_load_status_unlocked(app_id);
    switch (control_type) {
    case bulk_load_control_type::BLC_PAUSE: {
        if (app_status != bulk_load_status::BLS_DOWNLOADING) {
            auto hint_msg = fmt::format("can not pause bulk load for app({}) with status({})",
                                        app_name,
                                        dsn::enum_to_string(app_status));
            LOG_ERROR_F("{}", hint_msg);
            response.err = ERR_INVALID_STATE;
            response.__set_hint_msg(hint_msg);
            return;
        }
        LOG_INFO_F("app({}) start to pause bulk load", app_name);
        update_app_status_on_remote_storage_unlocked(app_id, bulk_load_status::BLS_PAUSING);
    } break;
    case bulk_load_control_type::BLC_RESTART: {
        if (app_status != bulk_load_status::BLS_PAUSED) {
            auto hint_msg = fmt::format("can not restart bulk load for app({}) with status({})",
                                        app_name,
                                        dsn::enum_to_string(app_status));
            LOG_ERROR_F("{}", hint_msg);
            response.err = ERR_INVALID_STATE;
            response.__set_hint_msg(hint_msg);
            return;
        }
        LOG_INFO_F("app({}) restart bulk load", app_name);
        update_app_status_on_remote_storage_unlocked(
            app_id, bulk_load_status::BLS_DOWNLOADING, ERR_OK, true);
    } break;
    case bulk_load_control_type::BLC_CANCEL:
        if (app_status != bulk_load_status::BLS_DOWNLOADING &&
            app_status != bulk_load_status::BLS_PAUSED) {
            auto hint_msg = fmt::format("can not cancel bulk load for app({}) with status({})",
                                        app_name,
                                        dsn::enum_to_string(app_status));
            LOG_ERROR_F("{}", hint_msg);
            response.err = ERR_INVALID_STATE;
            response.__set_hint_msg(hint_msg);
            return;
        }
    case bulk_load_control_type::BLC_FORCE_CANCEL: {
        LOG_INFO_F("app({}) start to {} cancel bulk load, original status = {}",
                   app_name,
                   control_type == bulk_load_control_type::BLC_FORCE_CANCEL ? "force" : "",
                   dsn::enum_to_string(app_status));
        update_app_status_on_remote_storage_unlocked(app_id,
                                                     bulk_load_status::BLS_CANCELED,
                                                     ERR_OK,
                                                     app_status == bulk_load_status::BLS_PAUSED);
    } break;
    default:
        break;
    }
}

// ThreadPool: THREAD_POOL_META_SERVER
void bulk_load_service::on_query_bulk_load_status(query_bulk_load_rpc rpc)
{
    const auto &request = rpc.request();
    const std::string &app_name = request.app_name;

    query_bulk_load_response &response = rpc.response();
    response.err = ERR_OK;
    response.app_name = app_name;

    std::shared_ptr<app_state> app = get_app(app_name);
    if (app == nullptr || app->status != app_status::AS_AVAILABLE) {
        auto hint_msg = fmt::format("app({}) is not existed or not available", app_name);
        LOG_ERROR_F("{}", hint_msg);
        response.err = (app == nullptr) ? ERR_APP_NOT_EXIST : ERR_APP_DROPPED;
        response.__set_hint_msg(hint_msg);
        return;
    }

    if (!app->is_bulk_loading) {
        auto hint_msg =
            fmt::format("app({}) is not during bulk load, return last time result", app_name);
        LOG_WARNING_F("{}", hint_msg);
        response.__set_hint_msg(hint_msg);
    }

    int32_t app_id = app->app_id;
    int32_t partition_count = app->partition_count;

    zauto_read_lock l(_lock);
    response.max_replica_count = app->max_replica_count;
    response.app_status = get_app_bulk_load_status_unlocked(app_id);

    response.partitions_status.resize(partition_count);
    for (const auto &kv : _partition_bulk_load_info) {
        if (kv.first.get_app_id() == app_id) {
            response.partitions_status[kv.first.get_partition_index()] = kv.second.status;
        }
    }

    response.bulk_load_states.resize(partition_count);
    for (const auto &kv : _partitions_bulk_load_state) {
        if (kv.first.get_app_id() == app_id) {
            response.bulk_load_states[kv.first.get_partition_index()] = kv.second;
        }
    }

    response.__set_is_bulk_loading(app->is_bulk_loading);

    if (!app->is_bulk_loading && bulk_load_status::BLS_FAILED == response.app_status) {
        response.err = get_app_bulk_load_err_unlocked(app_id);
    }

    LOG_INFO_F("query app({}) bulk_load_status({}) succeed",
               app_name,
               dsn::enum_to_string(response.app_status));
}

void bulk_load_service::on_clear_bulk_load(clear_bulk_load_rpc rpc)
{
    const auto &request = rpc.request();
    const std::string &app_name = request.app_name;
    clear_bulk_load_state_response &response = rpc.response();

    std::shared_ptr<app_state> app = get_app(app_name);
    if (app == nullptr || app->status != app_status::AS_AVAILABLE) {
        response.err = (app == nullptr) ? ERR_APP_NOT_EXIST : ERR_APP_DROPPED;
        response.hint_msg = fmt::format("app({}) is not existed or not available", app_name);
        LOG_ERROR_F("{}", response.hint_msg);
        return;
    }

    if (app->is_bulk_loading) {
        response.err = ERR_INVALID_STATE;
        response.hint_msg = fmt::format("app({}) is executing bulk load", app_name);
        LOG_ERROR_F("{}", response.hint_msg);
        return;
    }

    do_clear_app_bulk_load_result(app->app_id, rpc);
}

void bulk_load_service::do_clear_app_bulk_load_result(int32_t app_id, clear_bulk_load_rpc rpc)
{
    FAIL_POINT_INJECT_F("meta_do_clear_app_bulk_load_result",
                        [rpc](dsn::string_view) { rpc.response().err = ERR_OK; });
    std::string bulk_load_path = get_app_bulk_load_path(app_id);
    _meta_svc->get_meta_storage()->delete_node_recursively(
        std::move(bulk_load_path), [this, app_id, bulk_load_path, rpc]() {
            clear_bulk_load_state_response &response = rpc.response();
            response.err = ERR_OK;
            response.hint_msg =
                fmt::format("clear app({}) bulk load result succeed, remove bulk load dir succeed",
                            rpc.request().app_name);
            reset_local_bulk_load_states(app_id, rpc.request().app_name, true);
            LOG_INFO_F("{}", response.hint_msg);
        });
}

// ThreadPool: THREAD_POOL_META_SERVER
void bulk_load_service::create_bulk_load_root_dir()
{
    blob value = blob();
    std::string path = _bulk_load_root;
    _sync_bulk_load_storage->create_node(std::move(path), std::move(value), [this]() {
        LOG_INFO_F("create bulk load root({}) succeed", _bulk_load_root);
        sync_apps_from_remote_storage();
    });
}

// ThreadPool: THREAD_POOL_META_STATE
void bulk_load_service::sync_apps_from_remote_storage()
{
    std::string path = _bulk_load_root;
    _sync_bulk_load_storage->get_children(
        std::move(path), [this](bool flag, const std::vector<std::string> &children) {
            if (flag && children.size() > 0) {
                LOG_INFO_F("There are {} apps need to sync bulk load status", children.size());
                for (const auto &elem : children) {
                    int32_t app_id = boost::lexical_cast<int32_t>(elem);
                    LOG_INFO_F("start to sync app({}) bulk load status", app_id);
                    do_sync_app(app_id);
                }
            }
        });
}

// ThreadPool: THREAD_POOL_META_STATE
void bulk_load_service::do_sync_app(int32_t app_id)
{
    std::string app_path = get_app_bulk_load_path(app_id);
    _sync_bulk_load_storage->get_data(std::move(app_path), [this, app_id](const blob &value) {
        app_bulk_load_info ainfo;
        dsn::json::json_forwarder<app_bulk_load_info>::decode(value, ainfo);
        {
            zauto_write_lock l(_lock);
            _bulk_load_app_id.insert(app_id);
            _app_bulk_load_info[app_id] = ainfo;
        }
        sync_partitions_from_remote_storage(ainfo.app_id, ainfo.app_name);
    });
}

// ThreadPool: THREAD_POOL_META_STATE
void bulk_load_service::sync_partitions_from_remote_storage(int32_t app_id,
                                                            const std::string &app_name)
{
    std::string app_path = get_app_bulk_load_path(app_id);
    _sync_bulk_load_storage->get_children(
        std::move(app_path),
        [this, app_path, app_id, app_name](bool flag, const std::vector<std::string> &children) {
            LOG_INFO_F("app(name={},app_id={}) has {} partition bulk load info to be synced",
                       app_name,
                       app_id,
                       children.size());
            for (const auto &child_pidx : children) {
                int32_t pidx = boost::lexical_cast<int32_t>(child_pidx);
                std::string partition_path = get_partition_bulk_load_path(app_path, pidx);
                do_sync_partition(gpid(app_id, pidx), partition_path);
            }
        });
}

// ThreadPool: THREAD_POOL_META_STATE
void bulk_load_service::do_sync_partition(const gpid &pid, std::string &partition_path)
{
    _sync_bulk_load_storage->get_data(std::move(partition_path), [this, pid](const blob &value) {
        partition_bulk_load_info pinfo;
        dsn::json::json_forwarder<partition_bulk_load_info>::decode(value, pinfo);
        {
            zauto_write_lock l(_lock);
            _partition_bulk_load_info[pid] = pinfo;
        }
    });
}

// ThreadPool: THREAD_POOL_META_SERVER
void bulk_load_service::try_to_continue_bulk_load()
{
    FAIL_POINT_INJECT_F("meta_try_to_continue_bulk_load", [](dsn::string_view) {});
    zauto_read_lock l(_lock);
    for (const auto app_id : _bulk_load_app_id) {
        app_bulk_load_info ainfo = _app_bulk_load_info[app_id];
        // <partition_index, partition_bulk_load_info>
        std::unordered_map<int32_t, partition_bulk_load_info> pinfo_map;
        for (const auto &kv : _partition_bulk_load_info) {
            if (kv.first.get_app_id() == app_id) {
                pinfo_map[kv.first.get_partition_index()] = kv.second;
            }
        }
        try_to_continue_app_bulk_load(ainfo, pinfo_map);
    }
}

// ThreadPool: THREAD_POOL_META_SERVER
void bulk_load_service::try_to_continue_app_bulk_load(
    const app_bulk_load_info &ainfo,
    const std::unordered_map<int32_t, partition_bulk_load_info> &pinfo_map)
{
    std::shared_ptr<app_state> app = get_app(ainfo.app_name);
    // if app is not available, remove bulk load dir
    if (app == nullptr || app->status != app_status::AS_AVAILABLE) {
        LOG_ERROR_F(
            "app(name={},app_id={}) is not existed or not available", ainfo.app_name, ainfo.app_id);
        if (app == nullptr) {
            remove_bulk_load_dir_on_remote_storage(ainfo.app_id, ainfo.app_name);
        } else {
            remove_bulk_load_dir_on_remote_storage(std::move(app), true);
        }
        return;
    }

    // check app bulk load info
    if (!validate_app(app->app_id, app->partition_count, app->envs, ainfo, pinfo_map.size())) {
        remove_bulk_load_dir_on_remote_storage(std::move(app), true);
        return;
    }

    // index of the partition whose bulk load status is different from app's bulk load status
    std::unordered_set<int32_t> different_status_pidx_set;
    for (const auto &kv : pinfo_map) {
        if (kv.second.status != ainfo.status) {
            different_status_pidx_set.insert(kv.first);
        }
    }

    // check partition bulk load info
    if (!validate_partition(ainfo, pinfo_map, different_status_pidx_set.size())) {
        remove_bulk_load_dir_on_remote_storage(std::move(app), true);
        return;
    }

    tasking::enqueue(LPC_META_STATE_NORMAL,
                     _meta_svc->tracker(),
                     std::bind(&bulk_load_service::do_continue_app_bulk_load,
                               this,
                               ainfo,
                               pinfo_map,
                               different_status_pidx_set));
}

// ThreadPool: THREAD_POOL_META_SERVER
/*static*/ bool
bulk_load_service::validate_ingest_behind(const std::map<std::string, std::string> &envs,
                                          bool ingest_behind)
{
    bool app_allow_ingest_behind = false;
    const auto &iter = envs.find(replica_envs::ROCKSDB_ALLOW_INGEST_BEHIND);
    if (iter != envs.end()) {
        if (!buf2bool(iter->second, app_allow_ingest_behind)) {
            LOG_WARNING_F("can not convert {} to bool", iter->second);
            app_allow_ingest_behind = false;
        }
    }
    if (ingest_behind && !app_allow_ingest_behind) {
        return false;
    }
    return true;
}

// ThreadPool: THREAD_POOL_META_SERVER
/*static*/ bool bulk_load_service::validate_app(int32_t app_id,
                                                int32_t partition_count,
                                                const std::map<std::string, std::string> &envs,
                                                const app_bulk_load_info &ainfo,
                                                int32_t pinfo_count)
{
    // app id and partition from `app_bulk_load_info` is inconsistent with current app_info
    if (app_id != ainfo.app_id || partition_count != ainfo.partition_count) {
        LOG_ERROR_F("app({}) has different app_id or partition_count, bulk load app_id = {}, "
                    "partition_count = {}, current app_id = {}, partition_count = {}",
                    ainfo.app_name,
                    ainfo.app_id,
                    ainfo.partition_count,
                    app_id,
                    partition_count);
        return false;
    }

    // partition_bulk_load_info count should not be greater than partition_count
    if (partition_count < pinfo_count) {
        LOG_ERROR_F("app({}) has invalid count, app partition_count = {}, remote "
                    "partition_bulk_load_info count = {}",
                    ainfo.app_name,
                    partition_count,
                    pinfo_count);
        return false;
    }

    // partition_bulk_load_info count is not equal to partition_count can only be happended when app
    // status is downloading, consider the following condition:
    // when starting bulk load, meta server will create app_bulk_load_dir and
    // partition_bulk_load_dir on remote storage
    // however, meta server crash when create app directory and part of partition directory
    // when meta server recover, partition directory count is less than partition_count
    if (pinfo_count != partition_count && ainfo.status != bulk_load_status::BLS_DOWNLOADING) {
        LOG_ERROR_F("app({}) bulk_load_status = {}, but there are {} partitions lack "
                    "partition_bulk_load dir",
                    ainfo.app_name,
                    dsn::enum_to_string(ainfo.status),
                    partition_count - pinfo_count);
        return false;
    }

    if (!validate_ingest_behind(envs, ainfo.ingest_behind)) {
        LOG_ERROR_F("app({}) has inconsistent ingest_behind option", ainfo.app_name);
        return false;
    }

    return true;
}

// ThreadPool: THREAD_POOL_META_SERVER
/*static*/ bool bulk_load_service::validate_partition(
    const app_bulk_load_info &ainfo,
    const std::unordered_map<int32_t, partition_bulk_load_info> &pinfo_map,
    const int32_t different_status_count)
{
    const auto app_status = ainfo.status;
    bool is_valid = true;

    switch (app_status) {
    case bulk_load_status::BLS_DOWNLOADING:
        // if app status is downloading, partition status has no limit, because when bulk load meet
        // recoverable errors, will rollback to downloading
        // partition directory count is allowed less than partition_count, but it is impossible with
        // some partition bulk load status is not downloading and some partition directroy is
        // missing on remote storage
        if (ainfo.partition_count - pinfo_map.size() > 0 && different_status_count > 0) {
            LOG_ERROR_F(
                "app({}) bulk_load_status = {}, there are {} partitions status is different "
                "from app, and {} partitions not existed, this is invalid",
                ainfo.app_name,
                dsn::enum_to_string(app_status),
                different_status_count,
                ainfo.partition_count - pinfo_map.size());
            is_valid = false;
        }
        break;
    case bulk_load_status::BLS_DOWNLOADED:
    case bulk_load_status::BLS_INGESTING: {
        // if app status is downloaded, valid partition status is downloaded or ingesting
        // if app status is ingesting, valid partition status is ingesting or succeed
        const auto other_valid_status = (app_status == bulk_load_status::BLS_DOWNLOADED)
                                            ? bulk_load_status::BLS_INGESTING
                                            : bulk_load_status::BLS_SUCCEED;
        for (const auto &kv : pinfo_map) {
            if (kv.second.status != app_status && kv.second.status != other_valid_status) {
                LOG_ERROR_F(
                    "app({}) bulk_load_status = {}, but partition[{}] bulk_load_status = {}, "
                    "only {} and {} is valid",
                    ainfo.app_name,
                    app_status,
                    kv.first,
                    dsn::enum_to_string(kv.second.status),
                    dsn::enum_to_string(app_status),
                    dsn::enum_to_string(other_valid_status));
                is_valid = false;
                break;
            }
        }
    } break;
    case bulk_load_status::BLS_SUCCEED:
    case bulk_load_status::BLS_PAUSED:
        // if app status is succeed or paused, all partitions' status should not be different from
        // app's
        if (different_status_count > 0) {
            LOG_ERROR_F(
                "app({}) bulk_load_status = {}, {} partitions status is different from app, "
                "this is invalid",
                ainfo.app_name,
                dsn::enum_to_string(app_status),
                different_status_count);
            is_valid = false;
        }
        break;
    default:
        // for other status, partition status has no limit
        break;
    }

    return is_valid;
}

// ThreadPool: THREAD_POOL_META_STATE
void bulk_load_service::do_continue_app_bulk_load(
    const app_bulk_load_info &ainfo,
    const std::unordered_map<int32_t, partition_bulk_load_info> &pinfo_map,
    const std::unordered_set<int32_t> &different_status_pidx_set)
{
    const int32_t app_id = ainfo.app_id;
    const int32_t partition_count = ainfo.partition_count;
    const auto app_status = ainfo.status;
    const int32_t different_count = different_status_pidx_set.size();
    const int32_t same_count = pinfo_map.size() - different_count;
    const int32_t invalid_count = partition_count - pinfo_map.size();

    if (!FLAGS_enable_concurrent_bulk_load &&
        !_meta_svc->try_lock_meta_op_status(meta_op_status::BULKLOAD)) {
        LOG_ERROR_F("fatal, the op status of meta server must be meta_op_status::FREE");
        return;
    }
    LOG_INFO_F(
        "app({}) continue bulk load, app_id = {}, partition_count = {}, status = {}, there are {} "
        "partitions have bulk_load_info, {} partitions have same status with app, {} "
        "partitions different",
        ainfo.app_name,
        app_id,
        partition_count,
        dsn::enum_to_string(app_status),
        pinfo_map.size(),
        same_count,
        different_count);

    // _apps_in_progress_count is used for updating app bulk load, when _apps_in_progress_count = 0
    // means app bulk load status can transfer to next stage, for example, when app status is
    // downloaded, and _apps_in_progress_count = 0, app status can turn to ingesting
    // see more in function `update_partition_info_on_remote_storage_reply`
    int32_t in_progress_partition_count = partition_count;
    if (app_status == bulk_load_status::BLS_DOWNLOADING) {
        if (invalid_count > 0) {
            // create missing partition, so the in_progress_count should be invalid_count
            in_progress_partition_count = invalid_count;
        } else if (different_count > 0) {
            // it is hard to distinguish that bulk load is normal downloading or rollback to
            // downloading before meta server crash, when app status is downloading, we consider
            // bulk load as rolling back to downloading for convenience, for partitions whose status
            // is not downloading, update them to downloading, so the in_progress_count should be
            // different_count
            in_progress_partition_count = different_count;
        }
    } else if (app_status == bulk_load_status::BLS_DOWNLOADED ||
               app_status == bulk_load_status::BLS_INGESTING ||
               app_status == bulk_load_status::BLS_SUCCEED) {
        // for app status is downloaded, when all partitions turn to ingesting, app partition will
        // turn to ingesting, so the in_progress_count should be same_count, ingesting and succeed
        // are same
        in_progress_partition_count = same_count;
    } // for other cases, in_progress_count should be partition_count
    {
        zauto_write_lock l(_lock);
        _apps_in_progress_count[app_id] = in_progress_partition_count;
        _apps_rollback_count[app_id] = 0;
    }

    // if app is paused, no need to send bulk_load_request, just return
    if (app_status == bulk_load_status::BLS_PAUSED) {
        return;
    }

    // create all missing partitions then send request to all partitions
    if (app_status == bulk_load_status::BLS_DOWNLOADING && invalid_count > 0) {
        for (auto i = 0; i < partition_count; ++i) {
            if (pinfo_map.find(i) == pinfo_map.end()) {
                create_missing_partition_dir(ainfo.app_name, gpid(app_id, i), partition_count);
            }
        }
        return;
    }

    // update all partition status to app_status
    if ((app_status == bulk_load_status::BLS_FAILED ||
         app_status == bulk_load_status::BLS_CANCELED ||
         app_status == bulk_load_status::BLS_PAUSING ||
         app_status == bulk_load_status::BLS_DOWNLOADING) &&
        different_count > 0) {
        for (auto pidx : different_status_pidx_set) {
            update_partition_info_on_remote_storage(ainfo.app_name, gpid(app_id, pidx), app_status);
        }
    }

    // send bulk_load_request to all partitions
    for (auto i = 0; i < partition_count; ++i) {
        gpid pid = gpid(app_id, i);
        partition_bulk_load(ainfo.app_name, pid);
        if (app_status == bulk_load_status::BLS_INGESTING) {
            partition_ingestion(ainfo.app_name, pid);
        }
    }
}

// ThreadPool: THREAD_POOL_META_STATE
void bulk_load_service::create_missing_partition_dir(const std::string &app_name,
                                                     const gpid &pid,
                                                     int32_t partition_count)
{
    partition_bulk_load_info pinfo;
    pinfo.status = bulk_load_status::BLS_DOWNLOADING;
    blob value = dsn::json::json_forwarder<partition_bulk_load_info>::encode(pinfo);

    _meta_svc->get_meta_storage()->create_node(
        get_partition_bulk_load_path(pid),
        std::move(value),
        [app_name, pid, partition_count, pinfo, this]() {
            const int32_t app_id = pid.get_app_id();
            bool send_request = false;
            LOG_INFO_F("app({}) create partition({}) bulk_load_info", app_name, pid);
            {
                zauto_write_lock l(_lock);
                _partition_bulk_load_info[pid] = pinfo;

                if (--_apps_in_progress_count[app_id] == 0) {
                    _apps_in_progress_count[app_id] = partition_count;
                    send_request = true;
                }
            }
            if (send_request) {
                LOG_INFO_F("app({}) start to bulk load", app_name);
                for (auto i = 0; i < partition_count; ++i) {
                    partition_bulk_load(app_name, gpid(app_id, i));
                }
            }
        });
}

// ThreadPool: THREAD_POOL_META_SERVER
void bulk_load_service::check_app_bulk_load_states(std::shared_ptr<app_state> app,
                                                   bool is_app_bulk_loading)
{
    std::string app_path = get_app_bulk_load_path(app->app_id);
    _meta_svc->get_remote_storage()->node_exist(
        app_path, LPC_META_CALLBACK, [this, app_path, app, is_app_bulk_loading](error_code err) {
            if (err != ERR_OK && err != ERR_OBJECT_NOT_FOUND) {
                LOG_WARNING_F("check app({}) bulk load dir({}) failed, error = {}, try later",
                              app->app_name,
                              app_path,
                              err);
                tasking::enqueue(LPC_META_CALLBACK,
                                 nullptr,
                                 std::bind(&bulk_load_service::check_app_bulk_load_states,
                                           this,
                                           app,
                                           is_app_bulk_loading),
                                 0,
                                 std::chrono::seconds(1));
                return;
            }

            if (err == ERR_OBJECT_NOT_FOUND && is_app_bulk_loading) {
                LOG_ERROR_F("app({}): bulk load dir({}) not exist, but is_bulk_loading = {}, reset "
                            "app is_bulk_loading flag",
                            app->app_name,
                            app_path,
                            is_app_bulk_loading);
                update_app_not_bulk_loading_on_remote_storage(std::move(app));
                return;
            }

            // Normal cases:
            // err = ERR_OBJECT_NOT_FOUND, is_app_bulk_load = false: app is not executing bulk load
            // err = ERR_OK, is_app_bulk_load = true: app used to be executing bulk load
        });
}

} // namespace replication
} // namespace dsn
