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
#include <dsn/utility/fail_point.h>

#include "meta_split_service.h"
#include "meta_state_service_utils.h"

namespace dsn {
namespace replication {

meta_split_service::meta_split_service(meta_service *meta_srv)
{
    _meta_svc = meta_srv;
    _state = meta_srv->get_server_state();
}

void meta_split_service::start_partition_split(start_split_rpc rpc)
{
    const auto &request = rpc.request();
    auto &response = rpc.response();
    response.err = ERR_OK;

    std::shared_ptr<app_state> app;
    {
        zauto_write_lock l(app_lock());

        app = _state->get_app(request.app_name);
        if (app == nullptr || app->status != app_status::AS_AVAILABLE) {
            derror_f("app({}) is not existed or not available", request.app_name);
            response.err = app == nullptr ? ERR_APP_NOT_EXIST : ERR_APP_DROPPED;
            response.hint_msg = fmt::format(
                "app {}", response.err == ERR_APP_NOT_EXIST ? "not existed" : "dropped");

            return;
        }

        // new_partition_count != old_partition_count*2
        if (request.new_partition_count != app->partition_count * 2) {
            response.err = ERR_INVALID_PARAMETERS;
            derror_f("wrong partition count: app({}), partition count({}), new_partition_count({})",
                     request.app_name,
                     app->partition_count,
                     request.new_partition_count);
            response.hint_msg =
                fmt::format("wrong partition_count, should be {}", app->partition_count * 2);
            return;
        }

        if (app->helpers->split_states.splitting_count > 0) {
            response.err = ERR_BUSY;
            auto err_msg =
                fmt::format("app({}) is already executing partition split", request.app_name);
            derror_f("{}", err_msg);
            response.hint_msg = err_msg;
            return;
        }
    }

    ddebug_f("app({}) start to partition split, new_partition_count={}",
             request.app_name,
             request.new_partition_count);

    do_start_partition_split(std::move(app), std::move(rpc));
}

void meta_split_service::do_start_partition_split(std::shared_ptr<app_state> app,
                                                  start_split_rpc rpc)
{
    auto on_write_storage_complete = [app, rpc, this]() {
        ddebug_f("app({}) update partition count on remote storage, new partition_count = {}",
                 app->app_name,
                 app->partition_count * 2);

        zauto_write_lock l(app_lock());
        app->helpers->split_states.splitting_count = app->partition_count;
        app->partition_count *= 2;
        app->helpers->contexts.resize(app->partition_count);
        app->partitions.resize(app->partition_count);

        for (int i = 0; i < app->partition_count; ++i) {
            app->helpers->contexts[i].config_owner = &app->partitions[i];
            if (i >= app->partition_count / 2) { // child partitions
                app->partitions[i].ballot = invalid_ballot;
                app->partitions[i].pid = gpid(app->app_id, i);
            } else { // parent partitions
                app->helpers->split_states.status[i] = split_status::SPLITTING;
            }
        }

        auto &response = rpc.response();
        response.err = ERR_OK;
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

// TODO(heyuchen): refactor this function
void meta_split_service::register_child_on_meta(register_child_rpc rpc)
{
    const auto &request = rpc.request();
    const std::string &app_name = request.app.app_name;
    auto &response = rpc.response();
    response.err = ERR_IO_PENDING;

    zauto_write_lock l(app_lock());
    std::shared_ptr<app_state> app = _state->get_app(app_name);
    dassert_f(app != nullptr, "app({}) is not existed", app_name);
    dassert_f(app->is_stateful, "app({}) is stateless currently", app_name);

    const gpid &parent_gpid = request.parent_config.pid;
    const gpid &child_gpid = request.child_config.pid;
    const auto &parent_config = app->partitions[parent_gpid.get_partition_index()];
    if (request.parent_config.ballot != parent_config.ballot) {
        derror_f("app({}) partition({}) register child({}) failed, request is outdated, request "
                 "parent ballot = {}, local parent ballot = {}",
                 app_name,
                 parent_gpid,
                 child_gpid,
                 request.parent_config.ballot,
                 parent_config.ballot);
        response.err = ERR_INVALID_VERSION;
        response.parent_config = parent_config;
        return;
    }

    config_context &parent_context = app->helpers->contexts[parent_gpid.get_partition_index()];
    if (parent_context.stage == config_status::pending_proposal ||
        parent_context.stage == config_status::pending_remote_sync) {
        dwarn_f("app({}) partition({}): another request is syncing with remote storage, ignore "
                "this request",
                app_name,
                parent_gpid);
        return;
    }

    // TODO(heyuchen): pause/cancel split check

    auto iter = app->helpers->split_states.status.find(parent_gpid.get_partition_index());
    if (iter == app->helpers->split_states.status.end()) {
        derror_f(
            "duplicated register request, app({}) child partition({}) has already been registered",
            app_name,
            child_gpid);
        const auto &child_config = app->partitions[child_gpid.get_partition_index()];
        dassert_f(child_config.ballot > 0,
                  "app({}) partition({}) should have been registered",
                  app_name,
                  child_gpid);
        response.err = ERR_CHILD_REGISTERED;
        response.parent_config = parent_config;
        return;
    }

    if (iter->second != split_status::SPLITTING) {
        derror_f(
            "app({}) partition({}) register child({}) failed, current partition split_status = {}",
            app_name,
            parent_gpid,
            child_gpid,
            dsn::enum_to_string(iter->second));
        response.err = ERR_INVALID_STATE;
        return;
    }

    app->helpers->split_states.status.erase(parent_gpid.get_partition_index());
    app->helpers->split_states.splitting_count--;
    ddebug_f("app({}) parent({}) will register child({})", app_name, parent_gpid, child_gpid);

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
    const auto &request = rpc.request();
    auto &response = rpc.response();
    const std::string &app_name = request.app.app_name;

    zauto_write_lock l(app_lock());
    std::shared_ptr<app_state> app = _state->get_app(app_name);
    dassert_f(app != nullptr, "app({}) is not existed", app_name);
    dassert_f(app->is_stateful, "app({}) is stateless currently", app_name);

    const gpid &parent_gpid = request.parent_config.pid;
    const gpid &child_gpid = request.child_config.pid;
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
    dassert_f(ec == ERR_OK, "we can't handle this right now, err = {}", ec);

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

    if (parent_context.msg) {
        response.err = ERR_OK;
        response.app = *app;
        response.parent_config = app->partitions[parent_gpid.get_partition_index()];
        response.child_config = app->partitions[child_gpid.get_partition_index()];
        parent_context.msg = nullptr;
    }
    parent_context.pending_sync_task = nullptr;
    parent_context.stage = config_status::not_pending;
}

} // namespace replication
} // namespace dsn
