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
    const auto req = rpc.request();

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
                    auto response = rpc.response();
                    response.err = ERR_OK;
                }
            }
            // start send bulk load to replica servers
            partition_bulk_load(app_name, pid);
        });
}

// ThreadPool: THREAD_POOL_META_STATE
void bulk_load_service::partition_bulk_load(const std::string &app_name, const gpid &pid)
{
    // TODO(heyuchen): TBD
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
