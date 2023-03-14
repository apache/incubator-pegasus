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
#include <chrono>
#include <cstdint>
#include <functional>
#include <map>
#include <utility>
#include <vector>

#include "common/gpid.h"
#include "common/json_helper.h"
#include "common/replica_envs.h"
#include "common/replication.codes.h"
#include "common/replication_enums.h"
#include "common/replication_other_types.h"
#include "dsn.layer2_types.h"
#include "meta/meta_data.h"
#include "meta/meta_service.h"
#include "meta/meta_state_service.h"
#include "meta/server_state.h"
#include "meta_admin_types.h"
#include "meta_split_service.h"
#include "meta_state_service_utils.h"
#include "metadata_types.h"
#include "runtime/rpc/rpc_address.h"
#include "runtime/rpc/rpc_holder.h"
#include "runtime/task/async_calls.h"
#include "utils/blob.h"
#include "utils/error_code.h"
#include "utils/fmt_logging.h"
#include "utils/zlocks.h"

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
            LOG_ERROR("app({}) is not existed or not available", request.app_name);
            response.err = app == nullptr ? ERR_APP_NOT_EXIST : ERR_APP_DROPPED;
            response.hint_msg = fmt::format(
                "app {}", response.err == ERR_APP_NOT_EXIST ? "not existed" : "dropped");
            return;
        }

        // new_partition_count != old_partition_count*2
        if (request.new_partition_count != app->partition_count * 2) {
            response.err = ERR_INVALID_PARAMETERS;
            LOG_ERROR(
                "wrong partition count: app({}), partition count({}), new_partition_count({})",
                request.app_name,
                app->partition_count,
                request.new_partition_count);
            response.hint_msg =
                fmt::format("wrong partition_count, should be {}", app->partition_count * 2);
            return;
        }

        if (app->splitting()) {
            response.err = ERR_BUSY;
            auto err_msg =
                fmt::format("app({}) is already executing partition split", request.app_name);
            LOG_ERROR("{}", err_msg);
            response.hint_msg = err_msg;
            return;
        }
    }

    LOG_INFO("app({}) start to partition split, new_partition_count={}",
             request.app_name,
             request.new_partition_count);

    do_start_partition_split(std::move(app), std::move(rpc));
}

void meta_split_service::do_start_partition_split(std::shared_ptr<app_state> app,
                                                  start_split_rpc rpc)
{
    auto on_write_storage_complete = [app, rpc, this]() {
        LOG_INFO("app({}) update partition count on remote storage, new partition_count = {}",
                 app->app_name,
                 app->partition_count * 2);

        zauto_write_lock l(app_lock());
        app->helpers->split_states.splitting_count = app->partition_count;
        app->partition_count *= 2;
        app->helpers->contexts.resize(app->partition_count);
        app->partitions.resize(app->partition_count);
        app->envs[replica_envs::SPLIT_VALIDATE_PARTITION_HASH] = "true";

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
    copy.envs[replica_envs::SPLIT_VALIDATE_PARTITION_HASH] = "true";
    blob value = dsn::json::json_forwarder<app_info>::encode(copy);
    _meta_svc->get_meta_storage()->set_data(
        _state->get_app_path(*app), std::move(value), on_write_storage_complete);
}

void meta_split_service::register_child_on_meta(register_child_rpc rpc)
{
    const auto &request = rpc.request();
    const std::string &app_name = request.app.app_name;
    auto &response = rpc.response();
    response.err = ERR_IO_PENDING;

    zauto_write_lock l(app_lock());
    std::shared_ptr<app_state> app = _state->get_app(app_name);
    CHECK(app, "app({}) is not existed", app_name);
    CHECK(app->is_stateful, "app({}) is stateless currently", app_name);

    const gpid &parent_gpid = request.parent_config.pid;
    const gpid &child_gpid = request.child_config.pid;
    const auto &parent_config = app->partitions[parent_gpid.get_partition_index()];
    if (request.parent_config.ballot != parent_config.ballot) {
        LOG_ERROR("app({}) partition({}) register child({}) failed, request is outdated, request "
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
    if (parent_context.stage == config_status::pending_remote_sync) {
        LOG_WARNING("app({}) partition({}): another request is syncing with remote storage, ignore "
                    "this request",
                    app_name,
                    parent_gpid);
        return;
    }

    if (child_gpid.get_partition_index() >= app->partition_count) {
        LOG_ERROR(
            "app({}) partition({}) register child({}) failed, partition split has been canceled",
            app_name,
            parent_gpid,
            child_gpid);
        response.err = ERR_INVALID_STATE;
        response.parent_config = parent_config;
        return;
    }

    auto iter = app->helpers->split_states.status.find(parent_gpid.get_partition_index());
    if (iter == app->helpers->split_states.status.end()) {
        LOG_ERROR(
            "duplicated register request, app({}) child partition({}) has already been registered",
            app_name,
            child_gpid);
        const auto &child_config = app->partitions[child_gpid.get_partition_index()];
        CHECK_GT_MSG(child_config.ballot,
                     0,
                     "app({}) partition({}) should have been registered",
                     app_name,
                     child_gpid);
        response.err = ERR_CHILD_REGISTERED;
        response.parent_config = parent_config;
        return;
    }

    if (iter->second != split_status::SPLITTING) {
        LOG_ERROR(
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
    LOG_INFO("app({}) parent({}) will register child({})", app_name, parent_gpid, child_gpid);

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
    CHECK(app, "app({}) is not existed", app_name);
    CHECK(app->is_stateful, "app({}) is stateless currently", app_name);

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
    CHECK_EQ_MSG(ec, ERR_OK, "we can't handle this right now");

    LOG_INFO("parent({}) resgiter child({}) on remote storage succeed", parent_gpid, child_gpid);

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

void meta_split_service::query_partition_split(query_split_rpc rpc) const
{
    const std::string &app_name = rpc.request().app_name;
    auto &response = rpc.response();
    response.err = ERR_OK;

    zauto_read_lock l(app_lock());
    std::shared_ptr<app_state> app = _state->get_app(app_name);
    if (app == nullptr || app->status != app_status::AS_AVAILABLE) {
        response.err = app == nullptr ? ERR_APP_NOT_EXIST : ERR_APP_DROPPED;
        response.__set_hint_msg(fmt::format(
            "app({}) {}", app_name, response.err == ERR_APP_NOT_EXIST ? "not existed" : "dropped"));
        LOG_ERROR("query partition split failed, {}", response.hint_msg);
        return;
    }

    if (!app->splitting()) {
        response.err = ERR_INVALID_STATE;
        response.__set_hint_msg(fmt::format("app({}) is not splitting", app_name));
        LOG_ERROR("query partition split failed, {}", response.hint_msg);
        return;
    }

    response.new_partition_count = app->partition_count;
    response.status = app->helpers->split_states.status;
    LOG_INFO("query partition split succeed, app({}), partition_count({}), splitting_count({})",
             app->app_name,
             response.new_partition_count,
             response.status.size());
}

void meta_split_service::control_partition_split(control_split_rpc rpc)
{
    const auto &req = rpc.request();
    const auto &control_type = req.control_type;
    auto &response = rpc.response();

    zauto_write_lock l(app_lock());
    std::shared_ptr<app_state> app = _state->get_app(req.app_name);
    if (app == nullptr || app->status != app_status::AS_AVAILABLE) {
        response.err = app == nullptr ? ERR_APP_NOT_EXIST : ERR_APP_DROPPED;
        response.__set_hint_msg(fmt::format(
            "app {}", response.err == ERR_APP_NOT_EXIST ? "not existed" : "dropped", req.app_name));
        LOG_ERROR("{} split failed, {}", control_type_str(control_type), response.hint_msg);
        return;
    }

    if (!app->splitting()) {
        response.err = ERR_INVALID_STATE;
        response.__set_hint_msg(fmt::format("app({}) is not splitting", req.app_name));
        LOG_ERROR("{} split failed, {}", control_type_str(control_type), response.hint_msg);
        return;
    }

    if (req.parent_pidx >= 0 && (control_type == split_control_type::PAUSE ||
                                 control_type == split_control_type::RESTART)) {
        do_control_single(std::move(app), std::move(rpc));
    } else {
        do_control_all(std::move(app), std::move(rpc));
    }
}

void meta_split_service::do_control_single(std::shared_ptr<app_state> app, control_split_rpc rpc)
{
    const auto &req = rpc.request();
    const std::string &app_name = req.app_name;
    const int32_t &parent_pidx = req.parent_pidx;
    const auto &control_type = req.control_type;
    auto &response = rpc.response();

    if (parent_pidx >= app->partition_count / 2) {
        response.err = ERR_INVALID_PARAMETERS;
        response.__set_hint_msg(fmt::format("invalid parent partition index({})", parent_pidx));
        LOG_ERROR("{} split for app({}) failed, {}",
                  control_type_str(control_type),
                  app_name,
                  response.hint_msg);
        return;
    }

    auto iter = app->helpers->split_states.status.find(parent_pidx);
    if (iter == app->helpers->split_states.status.end()) {
        response.err =
            control_type == split_control_type::PAUSE ? ERR_CHILD_REGISTERED : ERR_INVALID_STATE;
        response.__set_hint_msg(fmt::format("partition[{}] is not splitting", parent_pidx));
        LOG_ERROR("{} split for app({}) failed, {}",
                  control_type_str(control_type),
                  app_name,
                  response.hint_msg);
        return;
    }

    split_status::type old_status =
        control_type == split_control_type::PAUSE ? split_status::SPLITTING : split_status::PAUSED;
    split_status::type target_status =
        control_type == split_control_type::PAUSE ? split_status::PAUSING : split_status::SPLITTING;
    if (iter->second == old_status) {
        iter->second = target_status;
        response.err = ERR_OK;
        LOG_INFO("app({}) partition[{}] {} split succeed",
                 app_name,
                 parent_pidx,
                 control_type_str(control_type));
    } else {
        response.err = ERR_INVALID_STATE;
        response.__set_hint_msg(fmt::format("partition[{}] wrong split_status({})",
                                            parent_pidx,
                                            dsn::enum_to_string(iter->second)));
        LOG_ERROR("{} split for app({}) failed, {}",
                  control_type_str(control_type),
                  app_name,
                  response.hint_msg);
    }
}

void meta_split_service::do_control_all(std::shared_ptr<app_state> app, control_split_rpc rpc)
{
    const auto &req = rpc.request();
    const auto &control_type = req.control_type;
    auto &response = rpc.response();

    if (control_type == split_control_type::CANCEL) {
        if (req.old_partition_count != app->partition_count / 2) {
            response.err = ERR_INVALID_PARAMETERS;
            response.__set_hint_msg(
                fmt::format("wrong partition_count, should be {}", app->partition_count / 2));
            LOG_ERROR("cancel split for app({}) failed, wrong partition count: partition count({}) "
                      "VS req partition_count({})",
                      app->app_name,
                      app->partition_count,
                      req.old_partition_count);
            return;
        }

        if (app->helpers->split_states.splitting_count != req.old_partition_count) {
            response.err = ERR_CHILD_REGISTERED;
            response.__set_hint_msg("some partitions have already finished split");
            LOG_ERROR("cancel split for app({}) failed, {}", app->app_name, response.hint_msg);
            return;
        }

        for (auto &kv : app->helpers->split_states.status) {
            LOG_INFO("app({}) partition({}) cancel split, old status = {}",
                     app->app_name,
                     kv.first,
                     dsn::enum_to_string(kv.second));
            kv.second = split_status::CANCELING;
        }
        return;
    }

    split_status::type old_status =
        control_type == split_control_type::PAUSE ? split_status::SPLITTING : split_status::PAUSED;
    split_status::type target_status =
        control_type == split_control_type::PAUSE ? split_status::PAUSING : split_status::SPLITTING;
    for (auto &kv : app->helpers->split_states.status) {
        if (kv.second == old_status) {
            kv.second = target_status;
            LOG_INFO("app({}) partition[{}] {} split succeed",
                     app->app_name,
                     kv.first,
                     control_type_str(control_type));
        }
    }
    response.err = ERR_OK;
}

void meta_split_service::notify_stop_split(notify_stop_split_rpc rpc)
{
    const auto &request = rpc.request();
    auto &response = rpc.response();
    zauto_write_lock l(app_lock());
    std::shared_ptr<app_state> app = _state->get_app(request.app_name);
    CHECK(app, "app({}) is not existed", request.app_name);
    CHECK(app->is_stateful, "app({}) is stateless currently", request.app_name);
    CHECK(request.meta_split_status == split_status::PAUSING ||
              request.meta_split_status == split_status::CANCELING,
          "invalid split_status({})",
          dsn::enum_to_string(request.meta_split_status));

    const std::string &stop_type =
        rpc.request().meta_split_status == split_status::PAUSING ? "pause" : "cancel";
    const auto iter =
        app->helpers->split_states.status.find(request.parent_gpid.get_partition_index());
    if (iter == app->helpers->split_states.status.end()) {
        LOG_WARNING(
            "app({}) partition({}) is not executing partition split, ignore out-dated {} split "
            "request",
            app->app_name,
            request.parent_gpid,
            stop_type);
        response.err = ERR_INVALID_VERSION;
        return;
    }

    if (iter->second != request.meta_split_status) {
        LOG_WARNING("app({}) partition({}) split_status = {}, ignore out-dated {} split request",
                    app->app_name,
                    request.parent_gpid,
                    dsn::enum_to_string(iter->second),
                    stop_type);
        response.err = ERR_INVALID_VERSION;
        return;
    }

    LOG_INFO("app({}) partition({}) notify {} split succeed",
             app->app_name,
             request.parent_gpid,
             stop_type);

    // pausing split
    if (iter->second == split_status::PAUSING) {
        iter->second = split_status::PAUSED;
        response.err = ERR_OK;
        return;
    }

    // canceling split
    CHECK_EQ_MSG(request.partition_count * 2, app->partition_count, "wrong partition_count");
    app->helpers->split_states.status.erase(request.parent_gpid.get_partition_index());
    response.err = ERR_OK;
    // when all partitions finish, partition_count should be updated
    if (--app->helpers->split_states.splitting_count == 0) {
        do_cancel_partition_split(std::move(app), rpc);
    }
}

void meta_split_service::do_cancel_partition_split(std::shared_ptr<app_state> app,
                                                   notify_stop_split_rpc rpc)
{
    auto on_write_storage_complete = [app, rpc, this]() {
        LOG_INFO("app({}) update partition count on remote storage, new partition count is {}",
                 app->app_name,
                 app->partition_count / 2);
        zauto_write_lock l(app_lock());
        app->partition_count /= 2;
        app->helpers->contexts.resize(app->partition_count);
        app->partitions.resize(app->partition_count);
    };

    auto copy = *app;
    copy.partition_count = rpc.request().partition_count;
    blob value = dsn::json::json_forwarder<app_info>::encode(copy);
    _meta_svc->get_meta_storage()->set_data(
        _state->get_app_path(*app), std::move(value), on_write_storage_complete);
}

void meta_split_service::query_child_state(query_child_state_rpc rpc)
{
    const auto &request = rpc.request();
    const auto &app_name = request.app_name;
    const auto &parent_pid = request.pid;
    auto &response = rpc.response();

    zauto_read_lock l(app_lock());
    std::shared_ptr<app_state> app = _state->get_app(app_name);
    CHECK(app, "app({}) is not existed", app_name);
    CHECK(app->is_stateful, "app({}) is stateless currently", app_name);

    if (app->partition_count == request.partition_count) {
        response.err = ERR_INVALID_STATE;
        LOG_ERROR("app({}) is not executing partition split", app_name);
        return;
    }

    CHECK_EQ_MSG(app->partition_count,
                 request.partition_count * 2,
                 "app({}) has invalid partition_count",
                 app_name);

    auto child_pidx = parent_pid.get_partition_index() + request.partition_count;
    if (app->partitions[child_pidx].ballot == invalid_ballot) {
        response.err = ERR_INVALID_STATE;
        LOG_ERROR("app({}) parent partition({}) split has been canceled", app_name, parent_pid);
        return;
    }
    LOG_INFO(
        "app({}) child partition({}.{}) is ready", app_name, parent_pid.get_app_id(), child_pidx);
    response.err = ERR_OK;
    response.__set_partition_count(app->partition_count);
    response.__set_child_config(app->partitions[child_pidx]);
}

} // namespace replication
} // namespace dsn
