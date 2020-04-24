/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015 Microsoft Corporation
 *
 * -=- Robust Distributed System Nucleus (rDSN) -=-
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

#include <dsn/dist/fmt_logging.h>

#include "dist/replication/meta_server/meta_split_service.h"
#include "dist/replication/meta_server/meta_state_service_utils.h"

namespace dsn {
namespace replication {

meta_split_service::meta_split_service(meta_service *meta_srv)
{
    _meta_svc = meta_srv;
    _state = meta_srv->get_server_state();
}

void meta_split_service::app_partition_split(app_partition_split_rpc rpc)
{
    const auto &request = rpc.request();
    auto &response = rpc.response();
    response.err = ERR_OK;

    std::shared_ptr<app_state> app;
    {
        zauto_write_lock l(app_lock());

        app = _state->get_app(request.app_name);
        if (!app || app->status != app_status::AS_AVAILABLE) {
            response.err = ERR_APP_NOT_EXIST;
            dwarn_f("client({}) sent split request with invalid app({}), app is not existed or "
                    "unavailable",
                    rpc.remote_address().to_string(),
                    request.app_name);
            return;
        }

        response.app_id = app->app_id;
        response.partition_count = app->partition_count;

        // new_partition_count != old_partition_count*2
        if (request.new_partition_count != app->partition_count * 2) {
            response.err = ERR_INVALID_PARAMETERS;
            dwarn_f("client({}) sent split request with wrong partition count: app({}), partition "
                    "count({}),"
                    "new_partition_count({})",
                    rpc.remote_address().to_string(),
                    request.app_name,
                    app->partition_count,
                    request.new_partition_count);
            return;
        }

        for (const auto &partition_config : app->partitions) {
            // partition already during split
            if (partition_config.ballot < 0) {
                response.err = ERR_BUSY;
                dwarn_f("app is already during partition split, client({}) sent repeated split "
                        "request: app({}), new_partition_count({})",
                        rpc.remote_address().to_string(),
                        request.app_name,
                        request.new_partition_count);
                return;
            }
        }
    }

    ddebug_f("app({}) start to partition split, new_partition_count={}",
             request.app_name,
             request.new_partition_count);

    do_app_partition_split(std::move(app), std::move(rpc));
}

void meta_split_service::do_app_partition_split(std::shared_ptr<app_state> app,
                                                app_partition_split_rpc rpc)
{
    auto on_write_storage_complete = [app, rpc, this]() {
        ddebug_f("app({}) update partition count on remote storage, new partition count is {}",
                 app->app_name.c_str(),
                 app->partition_count * 2);

        zauto_write_lock l(app_lock());
        app->partition_count *= 2;
        app->helpers->contexts.resize(app->partition_count);
        app->partitions.resize(app->partition_count);

        for (int i = 0; i < app->partition_count; ++i) {
            app->helpers->contexts[i].config_owner = &app->partitions[i];
            if (i >= app->partition_count / 2) {
                app->partitions[i].ballot = invalid_ballot;
                app->partitions[i].pid = gpid(app->app_id, i);
            }
        }

        auto &response = rpc.response();
        response.err = ERR_OK;
        response.partition_count = app->partition_count;
    };

    if (app->init_partition_count <= 0) {
        app->init_partition_count = app->partition_count;
    }
    auto copy = *app;
    copy.partition_count *= 2;
    blob value = dsn::json::json_forwarder<app_info>::encode(copy);

    _meta_svc->get_meta_storage()->set_data(
        _state->get_app_path(*app), std::move(value), on_write_storage_complete);
}

void meta_split_service::register_child_on_meta(register_child_rpc rpc)
{
    const auto &request = rpc.request();
    auto &response = rpc.response();
    response.err = ERR_IO_PENDING;

    zauto_write_lock(app_lock());
    std::shared_ptr<app_state> app = _state->get_app(request.app.app_id);
    dassert_f(app != nullptr, "app is not existed, id({})", request.app.app_id);
    dassert_f(app->is_stateful, "app is stateless currently, id({})", request.app.app_id);

    dsn::gpid parent_gpid = request.parent_config.pid;
    dsn::gpid child_gpid = request.child_config.pid;
    const partition_configuration &parent_config =
        app->partitions[parent_gpid.get_partition_index()];
    const partition_configuration &child_config = app->partitions[child_gpid.get_partition_index()];
    config_context &parent_context = app->helpers->contexts[parent_gpid.get_partition_index()];

    if (request.parent_config.ballot < parent_config.ballot) {
        dwarn_f("partition({}) register child failed, request is out-dated, request ballot = {}, "
                "meta ballot = {}",
                parent_gpid,
                request.parent_config.ballot,
                parent_config.ballot);
        response.err = ERR_INVALID_VERSION;
        return;
    }

    if (child_config.ballot != invalid_ballot) {
        dwarn_f(
            "duplicated register child request, child({}) has already been registered, ballot = {}",
            child_gpid,
            child_config.ballot);
        response.err = ERR_CHILD_REGISTERED;
        return;
    }

    if (parent_context.stage == config_status::pending_proposal ||
        parent_context.stage == config_status::pending_remote_sync) {
        dwarn_f("another request is syncing with remote storage, ignore this request");
        return;
    }

    ddebug_f("parent({}) will register child({})", parent_gpid, child_gpid);
    parent_context.stage = config_status::pending_remote_sync;
    parent_context.msg = rpc.dsn_request();
    parent_context.pending_sync_task = add_child_on_remote_storage(rpc, true);
}

dsn::task_ptr meta_split_service::add_child_on_remote_storage(register_child_rpc rpc,
                                                              bool create_new)
{
    const auto &request = rpc.request();
    const std::string &partition_path = _state->get_partition_path(request.child_config.pid);
    blob value = dsn::json::json_forwarder<partition_configuration>::encode(request.child_config);
    if (create_new) {
        return _meta_svc->get_remote_storage()->create_node(
            partition_path,
            LPC_META_STATE_HIGH,
            std::bind(&meta_split_service::on_add_child_on_remote_storage_reply,
                      this,
                      std::placeholders::_1,
                      rpc,
                      create_new),
            value);
    } else {
        return _meta_svc->get_remote_storage()->set_data(
            partition_path,
            value,
            LPC_META_STATE_HIGH,
            std::bind(&meta_split_service::on_add_child_on_remote_storage_reply,
                      this,
                      std::placeholders::_1,
                      rpc,
                      create_new),
            _meta_svc->tracker());
    }
}

void meta_split_service::on_add_child_on_remote_storage_reply(error_code ec,
                                                              register_child_rpc rpc,
                                                              bool create_new)
{
    zauto_write_lock(app_lock());

    const auto &request = rpc.request();
    auto &response = rpc.response();

    std::shared_ptr<app_state> app = _state->get_app(request.app.app_id);
    dassert_f(app != nullptr, "app is not existed, id({})", request.app.app_id);
    dassert_f(app->status == app_status::AS_AVAILABLE || app->status == app_status::AS_DROPPING,
              "app is not available now, id({})",
              request.app.app_id);

    dsn::gpid parent_gpid = request.parent_config.pid;
    dsn::gpid child_gpid = request.child_config.pid;
    config_context &parent_context = app->helpers->contexts[parent_gpid.get_partition_index()];

    if (ec == ERR_TIMEOUT ||
        (ec == ERR_NODE_ALREADY_EXIST && create_new)) { // retry register child on remote storage
        bool retry_create_new = (ec == ERR_TIMEOUT) ? create_new : false;
        int delay = (ec == ERR_TIMEOUT) ? 1 : 0;
        parent_context.pending_sync_task =
            tasking::enqueue(LPC_META_STATE_HIGH,
                             nullptr,
                             [this, parent_context, rpc, retry_create_new]() mutable {
                                 parent_context.pending_sync_task =
                                     add_child_on_remote_storage(rpc, retry_create_new);
                             },
                             0,
                             std::chrono::seconds(delay));
        return;
    }
    dassert_f(ec == ERR_OK, "we can't handle this right now, err = {}", ec.to_string());

    ddebug_f("parent({}) resgiter child({}) on remote storage succeed", parent_gpid, child_gpid);

    // update local child partition configuration
    std::shared_ptr<configuration_update_request> update_child_request =
        std::make_shared<configuration_update_request>();
    update_child_request->config = request.child_config;
    update_child_request->info = *app;
    update_child_request->type = config_type::CT_REGISTER_CHILD;
    update_child_request->node = request.primary_address;

    partition_configuration child_config = app->partitions[child_gpid.get_partition_index()];
    child_config.secondaries = request.child_config.secondaries;
    _state->update_configuration_locally(*app, update_child_request);

    parent_context.pending_sync_task = nullptr;
    parent_context.stage = config_status::not_pending;
    if (parent_context.msg) {
        response.err = ERR_OK;
        response.app = *app;
        response.parent_config = app->partitions[parent_gpid.get_partition_index()];
        response.child_config = app->partitions[child_gpid.get_partition_index()];
        parent_context.msg = nullptr;
    }
}

} // namespace replication
} // namespace dsn
