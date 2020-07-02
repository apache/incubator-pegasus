// Copyright (c) 2017-present, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include <dsn/dist/fmt_logging.h>
#include <dsn/utility/fail_point.h>

#include "meta_bulk_load_service.h"

namespace dsn {
namespace replication {

bulk_load_service::bulk_load_service(meta_service *meta_svc, const std::string &bulk_load_dir)
    : _meta_svc(meta_svc), _bulk_load_root(bulk_load_dir)
{
    _state = _meta_svc->get_server_state();
}

// ThreadPool: THREAD_POOL_META_STATE
void bulk_load_service::initialize_bulk_load_service()
{
    task_tracker tracker;
    error_code err = ERR_OK;

    create_bulk_load_root_dir(err, tracker);
    tracker.wait_outstanding_tasks();

    if (err == ERR_OK) {
        try_to_continue_bulk_load();
    }
}

// ThreadPool: THREAD_POOL_META_STATE
void bulk_load_service::on_start_bulk_load(start_bulk_load_rpc rpc)
{
    const auto &request = rpc.request();
    auto &response = rpc.response();
    response.err = ERR_OK;

    std::shared_ptr<app_state> app;
    {
        zauto_read_lock l(app_lock());

        app = _state->get_app(request.app_name);
        if (app == nullptr || app->status != app_status::AS_AVAILABLE) {
            derror_f("app({}) is not existed or not available", request.app_name);
            response.err = app == nullptr ? ERR_APP_NOT_EXIST : ERR_APP_DROPPED;
            response.hint_msg = fmt::format(
                "app {}", response.err == ERR_APP_NOT_EXIST ? "not existed" : "dropped");
            return;
        }

        if (app->is_bulk_loading) {
            derror_f("app({}) is already executing bulk load, please wait", app->app_name);
            response.err = ERR_BUSY;
            response.hint_msg = "app is already executing bulk load";
            return;
        }
    }

    std::string hint_msg;
    error_code e = check_bulk_load_request_params(request.app_name,
                                                  request.cluster_name,
                                                  request.file_provider_type,
                                                  app->app_id,
                                                  app->partition_count,
                                                  hint_msg);
    if (e != ERR_OK) {
        response.err = e;
        response.hint_msg = hint_msg;
        return;
    }

    ddebug_f("app({}) start bulk load, cluster_name = {}, provider = {}",
             request.app_name,
             request.cluster_name,
             request.file_provider_type);

    // avoid possible load balancing
    _meta_svc->set_function_level(meta_function_level::fl_steady);

    do_start_app_bulk_load(std::move(app), std::move(rpc));
}

// ThreadPool: THREAD_POOL_META_STATE
error_code bulk_load_service::check_bulk_load_request_params(const std::string &app_name,
                                                             const std::string &cluster_name,
                                                             const std::string &file_provider,
                                                             const int32_t app_id,
                                                             const int32_t partition_count,
                                                             std::string &hint_msg)
{
    FAIL_POINT_INJECT_F("meta_check_bulk_load_request_params",
                        [](dsn::string_view) -> error_code { return ERR_OK; });

    // check file provider
    dsn::dist::block_service::block_filesystem *blk_fs =
        _meta_svc->get_block_service_manager().get_block_filesystem(file_provider);
    if (blk_fs == nullptr) {
        derror_f("invalid remote file provider type: {}", file_provider);
        hint_msg = "invalid file_provider";
        return ERR_INVALID_PARAMETERS;
    }

    // sync get bulk_load_info file_handler
    const std::string remote_path = get_bulk_load_info_path(app_name, cluster_name);
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
        derror_f(
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
        derror_f("failed to read file({}) on remote provider({}), error = {}",
                 file_provider,
                 remote_path,
                 r_resp.err.to_string());
        hint_msg = "read bulk_load_info failed";
        return r_resp.err;
    }

    bulk_load_info bl_info;
    if (!::dsn::json::json_forwarder<bulk_load_info>::decode(r_resp.buffer, bl_info)) {
        derror_f("file({}) is damaged on remote file provider({})", remote_path, file_provider);
        hint_msg = "bulk_load_info damaged";
        return ERR_CORRUPTION;
    }

    if (bl_info.app_id != app_id || bl_info.partition_count != partition_count) {
        derror_f("app({}) information is inconsistent, local app_id({}) VS remote app_id({}), "
                 "local partition_count({}) VS remote partition_count({})",
                 app_name,
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
    blob value = dsn::json::json_forwarder<app_bulk_load_info>::encode(ainfo);

    _meta_svc->get_meta_storage()->create_node(
        get_app_bulk_load_path(app_id), std::move(value), [rpc, ainfo, this]() {
            dinfo_f("create app({}) bulk load dir", ainfo.app_name);
            {
                zauto_write_lock l(_lock);
                _app_bulk_load_info[ainfo.app_id] = ainfo;
                _apps_pending_sync_flag[ainfo.app_id] = false;
            }
            for (int32_t i = 0; i < ainfo.partition_count; ++i) {
                create_partition_bulk_load_dir(
                    ainfo.app_name, gpid(ainfo.app_id, i), ainfo.partition_count, std::move(rpc));
            }
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
    blob value = dsn::json::json_forwarder<partition_bulk_load_info>::encode(pinfo);

    _meta_svc->get_meta_storage()->create_node(
        get_partition_bulk_load_path(pid),
        std::move(value),
        [app_name, pid, partition_count, rpc, pinfo, this]() {
            dinfo_f("app({}) create partition({}) bulk_load_info", app_name, pid.to_string());
            {
                zauto_write_lock l(_lock);
                _partition_bulk_load_info[pid] = pinfo;
                _partitions_pending_sync_flag[pid] = false;
                if (--_apps_in_progress_count[pid.get_app_id()] == 0) {
                    ddebug_f("app({}) start bulk load succeed", app_name);
                    _apps_in_progress_count[pid.get_app_id()] = partition_count;
                    rpc.response().err = ERR_OK;
                }
            }
            // start send bulk load to replica servers
            partition_bulk_load(app_name, pid);
        });
}

// ThreadPool: THREAD_POOL_META_STATE
void bulk_load_service::partition_bulk_load(const std::string &app_name, const gpid &pid)
{
    FAIL_POINT_INJECT_F("meta_bulk_load_partition_bulk_load", [](dsn::string_view) {});

    rpc_address primary_addr;
    ballot b;
    {
        zauto_read_lock l(app_lock());
        std::shared_ptr<app_state> app = _state->get_app(pid.get_app_id());
        if (app == nullptr || app->status != app_status::AS_AVAILABLE) {
            dwarn_f("app(name={}, id={}) is not existed, set bulk load failed",
                    app_name,
                    pid.get_app_id());
            handle_app_unavailable(pid.get_app_id(), app_name);
            return;
        }
        primary_addr = app->partitions[pid.get_partition_index()].primary;
        b = app->partitions[pid.get_partition_index()].ballot;
    }

    if (primary_addr.is_invalid()) {
        dwarn_f("app({}) partition({}) primary is invalid, try it later", app_name, pid);
        tasking::enqueue(LPC_META_STATE_NORMAL,
                         _meta_svc->tracker(),
                         std::bind(&bulk_load_service::partition_bulk_load, this, app_name, pid),
                         0,
                         std::chrono::seconds(1));
        return;
    }

    zauto_read_lock l(_lock);
    const app_bulk_load_info &ainfo = _app_bulk_load_info[pid.get_app_id()];
    auto req = make_unique<bulk_load_request>();
    req->pid = pid;
    req->app_name = app_name;
    req->primary_addr = primary_addr;
    req->remote_provider_name = ainfo.file_provider_type;
    req->cluster_name = ainfo.cluster_name;
    req->meta_bulk_load_status = get_partition_bulk_load_status_unlocked(pid);
    req->ballot = b;
    req->query_bulk_load_metadata = is_partition_metadata_not_updated_unlocked(pid);

    ddebug_f("send bulk load request to node({}), app({}), partition({}), partition "
             "status = {}, remote provider = {}, cluster_name = {}",
             primary_addr.to_string(),
             app_name,
             pid,
             dsn::enum_to_string(req->meta_bulk_load_status),
             req->remote_provider_name,
             req->cluster_name);

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
    int32_t interval = bulk_load_constant::BULK_LOAD_REQUEST_INTERVAL;

    if (err != ERR_OK) {
        derror_f("app({}), partition({}) failed to receive bulk load response, error = {}",
                 app_name,
                 pid,
                 err.to_string());
        try_rollback_to_downloading(app_name, pid);
        try_resend_bulk_load_request(app_name, pid, interval);
        return;
    }

    if (response.err == ERR_OBJECT_NOT_FOUND || response.err == ERR_INVALID_STATE) {
        derror_f(
            "app({}), partition({}) doesn't exist or has invalid state on node({}), error = {}",
            app_name,
            pid,
            primary_addr.to_string(),
            response.err.to_string());
        try_rollback_to_downloading(app_name, pid);
        try_resend_bulk_load_request(app_name, pid, interval);
        return;
    }

    if (response.err == ERR_BUSY) {
        dwarn_f("node({}) has enough replicas downloading, wait for next round to send bulk load "
                "request for app({}), partition({})",
                primary_addr.to_string(),
                app_name,
                pid);
        try_resend_bulk_load_request(app_name, pid, interval);
        return;
    }

    if (response.err != ERR_OK) {
        derror_f("app({}), partition({}) handle bulk load response failed, error = {}, primary "
                 "status = {}",
                 app_name,
                 pid,
                 response.err.to_string(),
                 dsn::enum_to_string(response.primary_bulk_load_status));
        handle_bulk_load_failed(pid.get_app_id());
        try_resend_bulk_load_request(app_name, pid, interval);
        return;
    }

    // response.err = ERR_OK
    ballot current_ballot;
    {
        zauto_read_lock l(app_lock());
        std::shared_ptr<app_state> app = _state->get_app(pid.get_app_id());
        if (app == nullptr || app->status != app_status::AS_AVAILABLE) {
            dwarn_f("app(name={}, id={}) is not existed, set bulk load failed",
                    app_name,
                    pid.get_app_id());
            handle_app_unavailable(pid.get_app_id(), app_name);
            return;
        }
        current_ballot = app->partitions[pid.get_partition_index()].ballot;
    }
    if (request.ballot < current_ballot) {
        dwarn_f("receive out-date response, app({}), partition({}), request ballot = {}, "
                "current ballot= {}",
                app_name,
                pid,
                request.ballot,
                current_ballot);
        try_rollback_to_downloading(app_name, pid);
        try_resend_bulk_load_request(app_name, pid, interval);
        return;
    }

    // handle bulk load states reported from primary replica
    bulk_load_status::type app_status = get_app_bulk_load_status(response.pid.get_app_id());
    switch (app_status) {
    case bulk_load_status::BLS_DOWNLOADING:
        handle_app_downloading(response, primary_addr);
        break;
    case bulk_load_status::BLS_DOWNLOADED:
        update_partition_status_on_remote_storage(
            response.app_name, response.pid, bulk_load_status::BLS_INGESTING);
        // when app status is downloaded or ingesting, send request frequently
        interval = bulk_load_constant::BULK_LOAD_REQUEST_SHORT_INTERVAL;
        break;
    case bulk_load_status::BLS_INGESTING:
        handle_app_ingestion(response, primary_addr);
        interval = bulk_load_constant::BULK_LOAD_REQUEST_SHORT_INTERVAL;
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

    try_resend_bulk_load_request(app_name, pid, interval);
}

// ThreadPool: THREAD_POOL_META_STATE
void bulk_load_service::try_resend_bulk_load_request(const std::string &app_name,
                                                     const gpid &pid,
                                                     const int32_t interval)
{
    FAIL_POINT_INJECT_F("meta_bulk_load_resend_request", [](dsn::string_view) {});
    zauto_read_lock l(_lock);
    if (is_app_bulk_loading_unlocked(pid.get_app_id())) {
        tasking::enqueue(LPC_META_STATE_NORMAL,
                         _meta_svc->tracker(),
                         std::bind(&bulk_load_service::partition_bulk_load, this, app_name, pid),
                         0,
                         std::chrono::seconds(interval));
    }
}

// ThreadPool: THREAD_POOL_META_STATE
void bulk_load_service::handle_app_downloading(const bulk_load_response &response,
                                               const rpc_address &primary_addr)
{
    const std::string &app_name = response.app_name;
    const gpid &pid = response.pid;

    if (!response.__isset.total_download_progress) {
        dwarn_f(
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
            dwarn_f("receive bulk load response from node({}) app({}), partition({}), "
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
            derror_f("app({}) partition({}) on node({}) meet unrecoverable error during "
                     "downloading files, error = {}",
                     app_name,
                     pid,
                     kv.first.to_string(),
                     bulk_load_states.download_status);
            handle_bulk_load_failed(pid.get_app_id());
            return;
        }
    }

    // if replica report metadata, update metadata on remote storage
    if (response.__isset.metadata && is_partition_metadata_not_updated(pid)) {
        update_partition_metadata_on_remote_stroage(app_name, pid, response.metadata);
    }

    // update download progress
    int32_t total_progress = response.total_download_progress;
    ddebug_f("receive bulk load response from node({}) app({}) partition({}), primary_status({}), "
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
        ddebug_f(
            "app({}) partirion({}) download all files from remote provider succeed", app_name, pid);
        update_partition_status_on_remote_storage(app_name, pid, bulk_load_status::BLS_DOWNLOADED);
    }
}

// ThreadPool: THREAD_POOL_META_STATE
void bulk_load_service::handle_app_ingestion(const bulk_load_response &response,
                                             const rpc_address &primary_addr)
{
    const std::string &app_name = response.app_name;
    const gpid &pid = response.pid;

    if (!response.__isset.is_group_ingestion_finished) {
        dwarn_f("receive bulk load response from node({}) app({}) partition({}), "
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
            dwarn_f("receive bulk load response from node({}) app({}) partition({}), "
                    "primary_status({}), but node({}) ingestion_status is not set",
                    primary_addr.to_string(),
                    app_name,
                    pid,
                    dsn::enum_to_string(response.primary_bulk_load_status),
                    kv.first.to_string());
            return;
        }

        if (bulk_load_states.ingest_status == ingestion_status::IS_FAILED) {
            derror_f("app({}) partition({}) on node({}) ingestion failed",
                     app_name,
                     pid,
                     kv.first.to_string());
            handle_bulk_load_failed(pid.get_app_id());
            return;
        }
    }

    ddebug_f("receive bulk load response from node({}) app({}) partition({}), primary_status({}), "
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
        ddebug_f("app({}) partition({}) ingestion files succeed", app_name, pid);
        update_partition_status_on_remote_storage(app_name, pid, bulk_load_status::BLS_SUCCEED);
    }
}

// ThreadPool: THREAD_POOL_META_STATE
void bulk_load_service::handle_bulk_load_finish(const bulk_load_response &response,
                                                const rpc_address &primary_addr)
{
    const std::string &app_name = response.app_name;
    const gpid &pid = response.pid;

    dassert_f(
        response.__isset.is_group_bulk_load_context_cleaned_up,
        "receive bulk load response from node({}) app({}), partition({}), primary_status({}), "
        "but is_group_bulk_load_context_cleaned_up is not set",
        primary_addr.to_string(),
        app_name,
        pid,
        dsn::enum_to_string(response.primary_bulk_load_status));

    for (const auto &kv : response.group_bulk_load_state) {
        dassert_f(kv.second.__isset.is_cleaned_up,
                  "receive bulk load response from node({}) app({}), partition({}), "
                  "primary_status({}), but node({}) is_cleaned_up is not set",
                  primary_addr.to_string(),
                  app_name,
                  pid,
                  dsn::enum_to_string(response.primary_bulk_load_status),
                  kv.first.to_string());
    }

    {
        zauto_read_lock l(_lock);
        if (_partitions_cleaned_up[pid]) {
            dwarn_f(
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
    ddebug_f("receive bulk load response from node({}) app({}) partition({}), primary status = {}, "
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
            std::shared_ptr<app_state> app;
            {
                zauto_read_lock l(app_lock());
                app = _state->get_app(pid.get_app_id());
                if (app == nullptr || app->status != app_status::AS_AVAILABLE) {
                    dwarn_f("app(name={}, id={}) is not existed, remove bulk load dir on remote "
                            "storage",
                            app_name,
                            pid.get_app_id());
                    remove_bulk_load_dir_on_remote_storage(pid.get_app_id(), app_name);
                    return;
                }
            }
            ddebug_f("app({}) all partitions cleanup bulk load context", app_name);
            remove_bulk_load_dir_on_remote_storage(std::move(app), true);
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
        dwarn_f("receive bulk load response from node({}) app({}) partition({}), "
                "primary_status({}), but is_group_bulk_load_paused is not set",
                primary_addr.to_string(),
                app_name,
                pid,
                dsn::enum_to_string(response.primary_bulk_load_status));
        return;
    }

    for (const auto &kv : response.group_bulk_load_state) {
        if (!kv.second.__isset.is_paused) {
            dwarn_f("receive bulk load response from node({}) app({}), partition({}), "
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
    ddebug_f("receive bulk load response from node({}) app({}) partition({}), primary status = {}, "
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
        ddebug_f("app({}) partirion({}) pause bulk load succeed", response.app_name, pid);
        update_partition_status_on_remote_storage(
            response.app_name, pid, bulk_load_status::BLS_PAUSED);
    }
}

// ThreadPool: THREAD_POOL_META_STATE
void bulk_load_service::try_rollback_to_downloading(const std::string &app_name, const gpid &pid)
{
    // TODO(heyuchen): TBD
    // replica meets error during bulk load, rollback to downloading to retry bulk load
}

// ThreadPool: THREAD_POOL_META_STATE
void bulk_load_service::handle_bulk_load_failed(int32_t app_id)
{
    // TODO(heyuchen): TBD
    // replica meets serious error during bulk load, such as file on remote storage is damaged
    // should stop bulk load process, set bulk load failed
}

// ThreadPool: THREAD_POOL_META_STATE
void bulk_load_service::handle_app_unavailable(int32_t app_id, const std::string &app_name)
{
    // TODO(heyuchen): TBD
}

// ThreadPool: THREAD_POOL_META_STATE
void bulk_load_service::update_partition_metadata_on_remote_stroage(
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
            ddebug_f(
                "app({}) update partition({}) bulk load metadata, file count = {}, file size = {}",
                app_name,
                pid,
                pinfo.metadata.files.size(),
                pinfo.metadata.file_total_size);
        });
}

// ThreadPool: THREAD_POOL_META_STATE
void bulk_load_service::update_partition_status_on_remote_storage(const std::string &app_name,
                                                                  const gpid &pid,
                                                                  bulk_load_status::type new_status,
                                                                  bool should_send_request)
{
    zauto_read_lock l(_lock);
    partition_bulk_load_info pinfo = _partition_bulk_load_info[pid];

    if (pinfo.status == new_status) {
        return;
    }

    if (_partitions_pending_sync_flag[pid]) {
        ddebug_f("app({}) partition({}) has already sync bulk load status, wait for next round",
                 app_name,
                 pid);
        return;
    }

    _partitions_pending_sync_flag[pid] = true;
    pinfo.status = new_status;
    blob value = json::json_forwarder<partition_bulk_load_info>::encode(pinfo);

    _meta_svc->get_meta_storage()->set_data(
        get_partition_bulk_load_path(pid),
        std::move(value),
        std::bind(&bulk_load_service::update_partition_status_on_remote_storage_reply,
                  this,
                  app_name,
                  pid,
                  new_status,
                  should_send_request));
}

// ThreadPool: THREAD_POOL_META_STATE
void bulk_load_service::update_partition_status_on_remote_storage_reply(
    const std::string &app_name,
    const gpid &pid,
    bulk_load_status::type new_status,
    bool should_send_request)
{
    {
        zauto_write_lock l(_lock);
        auto old_status = _partition_bulk_load_info[pid].status;
        _partition_bulk_load_info[pid].status = new_status;
        _partitions_pending_sync_flag[pid] = false;

        ddebug_f("app({}) update partition({}) status from {} to {}",
                 app_name,
                 pid,
                 dsn::enum_to_string(old_status),
                 dsn::enum_to_string(new_status));

        // TODO(heyuchen): add other status
        switch (new_status) {
        case bulk_load_status::BLS_DOWNLOADED:
        case bulk_load_status::BLS_INGESTING:
        case bulk_load_status::BLS_SUCCEED:
        case bulk_load_status::BLS_PAUSED:
            if (old_status != new_status && --_apps_in_progress_count[pid.get_app_id()] == 0) {
                update_app_status_on_remote_storage_unlocked(pid.get_app_id(), new_status);
            }
            break;
        default:
            break;
        }
    }
    if (should_send_request) {
        partition_bulk_load(app_name, pid);
    }
}

// ThreadPool: THREAD_POOL_META_STATE
void bulk_load_service::update_app_status_on_remote_storage_unlocked(
    int32_t app_id, bulk_load_status::type new_status, bool should_send_request)
{
    FAIL_POINT_INJECT_F("meta_update_app_status_on_remote_storage_unlocked",
                        [](dsn::string_view) {});

    app_bulk_load_info ainfo = _app_bulk_load_info[app_id];
    auto old_status = ainfo.status;

    if (old_status == new_status && new_status != bulk_load_status::BLS_DOWNLOADING) {
        dwarn_f("app({}) old status:{} VS new status:{}, ignore it",
                ainfo.app_name,
                dsn::enum_to_string(old_status),
                dsn::enum_to_string(new_status));
        return;
    }

    if (_apps_pending_sync_flag[app_id]) {
        ddebug_f("app({}) has already sync bulk load status, wait and retry, current status = {}, "
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
                                   should_send_request),
                         0,
                         std::chrono::seconds(1));
        return;
    }

    _apps_pending_sync_flag[app_id] = true;
    ainfo.status = new_status;
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
    }

    ddebug_f("update app({}) status from {} to {}",
             ainfo.app_name,
             dsn::enum_to_string(old_status),
             dsn::enum_to_string(new_status));

    if (new_status == bulk_load_status::BLS_INGESTING) {
        for (int i = 0; i < partition_count; ++i) {
            tasking::enqueue(LPC_BULK_LOAD_INGESTION,
                             _meta_svc->tracker(),
                             std::bind(&bulk_load_service::partition_ingestion,
                                       this,
                                       ainfo.app_name,
                                       gpid(app_id, i)));
        }
    }

    // TODO(heyuchen): add other status
    if (new_status == bulk_load_status::BLS_PAUSING) {
        for (int i = 0; i < ainfo.partition_count; ++i) {
            update_partition_status_on_remote_storage(
                ainfo.app_name, gpid(app_id, i), new_status, should_send_request);
        }
    }
}

// ThreadPool: THREAD_POOL_DEFAULT
void bulk_load_service::partition_ingestion(const std::string &app_name, const gpid &pid)
{
    FAIL_POINT_INJECT_F("meta_bulk_load_partition_ingestion", [](dsn::string_view) {});

    rpc_address primary_addr;
    {
        zauto_read_lock l(app_lock());
        std::shared_ptr<app_state> app = _state->get_app(pid.get_app_id());
        if (app == nullptr || app->status != app_status::AS_AVAILABLE) {
            dwarn_f("app(name={}, id={}) is not existed, set bulk load failed",
                    app_name,
                    pid.get_app_id());
            handle_app_unavailable(pid.get_app_id(), app_name);
            return;
        }
        primary_addr = app->partitions[pid.get_partition_index()].primary;
    }

    if (primary_addr.is_invalid()) {
        dwarn_f("app({}) partition({}) primary is invalid, try it later", app_name, pid);
        tasking::enqueue(LPC_BULK_LOAD_INGESTION,
                         _meta_svc->tracker(),
                         std::bind(&bulk_load_service::partition_ingestion, this, app_name, pid),
                         pid.thread_hash(),
                         std::chrono::seconds(1));
        return;
    }

    if (is_partition_metadata_not_updated(pid)) {
        derror_f("app({}) partition({}) doesn't have bulk load metadata, set bulk load failed",
                 app_name,
                 pid);
        handle_bulk_load_failed(pid.get_app_id());
        return;
    }

    ingestion_request req;
    req.app_name = app_name;
    {
        zauto_read_lock l(_lock);
        req.metadata = _partition_bulk_load_info[pid].metadata;
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
        [this, app_name, pid](error_code err, ingestion_response &&resp) {
            on_partition_ingestion_reply(err, std::move(resp), app_name, pid);
        });
    ddebug_f("send ingest_request to node({}), app({}) partition({})",
             primary_addr.to_string(),
             app_name,
             pid);
    _meta_svc->send_request(msg, primary_addr, rpc_callback);
}

// ThreadPool: THREAD_POOL_DEFAULT
void bulk_load_service::on_partition_ingestion_reply(error_code err,
                                                     const ingestion_response &&resp,
                                                     const std::string &app_name,
                                                     const gpid &pid)
{
    // if meet 2pc error, ingesting will rollback to downloading, no need to retry here
    if (err != ERR_OK) {
        derror_f("app({}) partition({}) ingestion files failed, error = {}", app_name, pid, err);
        tasking::enqueue(
            LPC_META_STATE_NORMAL,
            _meta_svc->tracker(),
            std::bind(&bulk_load_service::try_rollback_to_downloading, this, app_name, pid));
        return;
    }

    if (resp.err == ERR_TRY_AGAIN && resp.rocksdb_error != 0) {
        derror_f("app({}) partition({}) ingestion files failed while empty write, rocksdb error = "
                 "{}, retry it later",
                 app_name,
                 pid,
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
        derror_f("app({}) partition({}) failed to ingestion files, error = {}, rocksdb error = {}",
                 app_name,
                 pid,
                 resp.err,
                 resp.rocksdb_error);
        tasking::enqueue(
            LPC_META_STATE_NORMAL,
            _meta_svc->tracker(),
            std::bind(&bulk_load_service::handle_bulk_load_failed, this, pid.get_app_id()));
        return;
    }

    ddebug_f("app({}) partition({}) receive ingestion response succeed", app_name, pid);
}

// ThreadPool: THREAD_POOL_META_STATE
void bulk_load_service::remove_bulk_load_dir_on_remote_storage(int32_t app_id,
                                                               const std::string &app_name)
{
    std::string bulk_load_path = get_app_bulk_load_path(app_id);
    _meta_svc->get_meta_storage()->delete_node_recursively(
        std::move(bulk_load_path), [this, app_id, app_name, bulk_load_path]() {
            ddebug_f("remove app({}) bulk load dir {} succeed", app_name, bulk_load_path);
            reset_local_bulk_load_states(app_id, app_name);
        });
}

// ThreadPool: THREAD_POOL_META_STATE
void bulk_load_service::remove_bulk_load_dir_on_remote_storage(std::shared_ptr<app_state> app,
                                                               bool set_app_not_bulk_loading)
{
    std::string bulk_load_path = get_app_bulk_load_path(app->app_id);
    _meta_svc->get_meta_storage()->delete_node_recursively(
        std::move(bulk_load_path), [this, app, set_app_not_bulk_loading, bulk_load_path]() {
            ddebug_f("remove app({}) bulk load dir {} succeed", app->app_name, bulk_load_path);
            reset_local_bulk_load_states(app->app_id, app->app_name);
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
void bulk_load_service::reset_local_bulk_load_states(int32_t app_id, const std::string &app_name)
{
    zauto_write_lock l(_lock);
    _app_bulk_load_info.erase(app_id);
    _apps_in_progress_count.erase(app_id);
    _apps_pending_sync_flag.erase(app_id);
    erase_map_elem_by_id(app_id, _partitions_pending_sync_flag);
    erase_map_elem_by_id(app_id, _partitions_bulk_load_state);
    erase_map_elem_by_id(app_id, _partition_bulk_load_info);
    erase_map_elem_by_id(app_id, _partitions_total_download_progress);
    erase_map_elem_by_id(app_id, _partitions_cleaned_up);
    // TODO(heyuchen): add other varieties
    _bulk_load_app_id.erase(app_id);
    ddebug_f("reset local app({}) bulk load context", app_name);
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
            ddebug_f("app({}) update app is_bulk_loading to false", app->app_name);
        });
}

// ThreadPool: THREAD_POOL_META_SERVER
void bulk_load_service::on_control_bulk_load(control_bulk_load_rpc rpc)
{
    const std::string &app_name = rpc.request().app_name;
    const auto &control_type = rpc.request().type;
    auto &response = rpc.response();
    response.err = ERR_OK;

    int32_t app_id;
    {
        zauto_read_lock l(app_lock());
        std::shared_ptr<app_state> app = _state->get_app(app_name);
        if (app == nullptr || app->status != app_status::AS_AVAILABLE) {
            derror_f("app({}) is not existed or not available", app_name);
            response.err = app == nullptr ? ERR_APP_NOT_EXIST : ERR_APP_DROPPED;
            response.__set_hint_msg(
                fmt::format("app({}) is not existed or not available", app_name));
            return;
        }

        if (!app->is_bulk_loading) {
            derror_f("app({}) is not executing bulk load", app_name);
            response.err = ERR_INACTIVE_STATE;
            response.__set_hint_msg(fmt::format("app({}) is not executing bulk load", app_name));
            return;
        }
        app_id = app->app_id;
    }

    zauto_write_lock l(_lock);
    const auto &app_status = get_app_bulk_load_status_unlocked(app_id);
    switch (control_type) {
    case bulk_load_control_type::BLC_PAUSE: {
        if (app_status != bulk_load_status::BLS_DOWNLOADING) {
            derror_f("pause bulk load for app({}) failed, can not pause bulk load with status({})",
                     app_name,
                     dsn::enum_to_string(app_status));
            response.err = ERR_INVALID_STATE;
            response.__set_hint_msg(
                fmt::format("app({}) status={}", app_name, dsn::enum_to_string(app_status)));
            return;
        }
        ddebug_f("app({}) start to pause bulk load", app_name);
        tasking::enqueue(LPC_META_STATE_NORMAL,
                         _meta_svc->tracker(),
                         std::bind(&bulk_load_service::update_app_status_on_remote_storage_unlocked,
                                   this,
                                   app_id,
                                   bulk_load_status::BLS_PAUSING,
                                   false));
    } break;
    case bulk_load_control_type::BLC_RESTART:
    // TODO(heyuchen): TBD
    case bulk_load_control_type::BLC_CANCEL:
    // TODO(heyuchen): TBD
    case bulk_load_control_type::BLC_FORCE_CANCEL:
    // TODO(heyuchen): TBD
    default:
        break;
    }
}

// ThreadPool: THREAD_POOL_META_STATE
void bulk_load_service::create_bulk_load_root_dir(error_code &err, task_tracker &tracker)
{
    blob value = blob();
    _meta_svc->get_remote_storage()->create_node(
        _bulk_load_root,
        LPC_META_CALLBACK,
        [this, &err, &tracker](error_code ec) {
            if (ERR_OK == ec || ERR_NODE_ALREADY_EXIST == ec) {
                ddebug_f("create bulk load root({}) succeed", _bulk_load_root);
                sync_apps_bulk_load_from_remote_stroage(err, tracker);
            } else if (ERR_TIMEOUT == ec) {
                dwarn_f("create bulk load root({}) failed, retry later", _bulk_load_root);
                tasking::enqueue(
                    LPC_META_STATE_NORMAL,
                    _meta_svc->tracker(),
                    std::bind(&bulk_load_service::create_bulk_load_root_dir, this, err, tracker),
                    0,
                    std::chrono::seconds(1));
            } else {
                err = ec;
                dfatal_f(
                    "create bulk load root({}) failed, error={}", _bulk_load_root, ec.to_string());
            }
        },
        value,
        &tracker);
}

void bulk_load_service::sync_apps_bulk_load_from_remote_stroage(error_code &err,
                                                                task_tracker &tracker)
{
    // TODO(heyuchen): TBD
}

void bulk_load_service::try_to_continue_bulk_load()
{
    // TODO(heyuchen): TBD
}

} // namespace replication
} // namespace dsn
