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

#include <dsn/dist/fmt_logging.h>
#include <dsn/utility/fail_point.h>
#include <dsn/utility/filesystem.h>

#include "meta_backup_service.h"

namespace dsn {
namespace replication {

backup_service::backup_service(meta_service *meta_svc, const std::string &remote_storage_root)
    : _meta_svc(meta_svc), _remote_storage_root(remote_storage_root)
{
    _state = _meta_svc->get_server_state();
}

backup_service::~backup_service() { _tracker.cancel_outstanding_tasks(); }

// TODO(heyuchen): implement it
void backup_service::start() {}

void backup_service::start_backup_app(start_backup_app_rpc rpc)
{
    const start_backup_app_request &request = rpc.request();
    start_backup_app_response &response = rpc.response();

    std::string app_name;
    int32_t partition_count;
    {
        zauto_read_lock l;
        _state->lock_read(l);
        const auto &app = _state->get_app(request.app_id);
        if (app == nullptr || app->status != app_status::AS_AVAILABLE) {
            response.err = app == nullptr ? ERR_APP_NOT_EXIST : ERR_APP_DROPPED;
            response.hint_message = fmt::format(
                "app is {}", response.err == ERR_APP_NOT_EXIST ? "not existed" : "not available");
            derror_f("{}", response.hint_message);
            return;
        }
        app_name = app->app_name;
        partition_count = app->partition_count;
    }

    {
        zauto_read_lock l(_lock);
        for (const auto &backup : _onetime_backup_states) {
            if (request.app_id == backup->get_backup_app_id() && backup->is_in_progress()) {
                response.err = ERR_INVALID_STATE;
                response.hint_message = fmt::format("app {} is actively being backup", app_name);
                derror_f("Failed to start backup for {}, {}", app_name, response.hint_message);
                return;
            }
        }

        // TODO(heyuchen): add periodic backup executing check
    }

    dist::block_service::block_filesystem *fs =
        get_meta_service()->get_block_service_manager().get_or_create_block_filesystem(
            request.backup_provider_type);
    if (fs == nullptr) {
        response.err = ERR_INVALID_PARAMETERS;
        response.hint_message =
            fmt::format("invalid backup_provider_type = {}", request.backup_provider_type);
        derror_f("Failed to start backup for {}, {}", app_name, response.hint_message);
        return;
    }

    // try to start backup
    std::shared_ptr<meta_backup_engine> engine =
        std::make_shared<meta_backup_engine>(get_meta_service(), false);
    engine->init_backup(request.app_id,
                        partition_count,
                        app_name,
                        request.backup_provider_type,
                        request.__isset.backup_path ? request.backup_path : "");
    create_onetime_backup_on_remote_storage(std::move(engine), rpc);
}

// ThreadPool: THREAD_POOL_DEFAULT
void backup_service::create_onetime_backup_on_remote_storage(
    std::shared_ptr<meta_backup_engine> engine, start_backup_app_rpc rpc)
{
    auto item = engine->get_backup_item();
    auto value = dsn::json::json_forwarder<backup_item>::encode(item);

    std::queue<std::string> nodes;
    get_node_path(item.app_id, false, nodes, item.backup_id);
    get_meta_service()->get_meta_storage()->create_node_recursively(
        std::move(nodes), std::move(value), [this, engine, rpc]() {
            FAIL_POINT_INJECT_F("meta_create_onetime_backup", [=](string_view str) {
                rpc.response().err = ERR_OK;
                ddebug_f("App({}) start backup succeed", engine->get_backup_item().app_name);
            });
            tasking::enqueue(
                LPC_DEFAULT_CALLBACK, _meta_svc->tracker(), [engine]() { engine->start(); });
            int64_t backup_id = engine->get_backup_id();
            {
                zauto_write_lock l(_lock);
                _onetime_backup_states.emplace_back(std::move(engine));
            }

            start_backup_app_response &response = rpc.response();
            response.__isset.backup_id = true;
            response.backup_id = backup_id;
            response.hint_message =
                fmt::format("App {} start backup succeed", engine->get_backup_item().app_name);
            ddebug_f("{}", response.hint_message);
        });
}

// TODO(heyuchen): implement it
void backup_service::query_backup_status(query_backup_status_rpc rpc) {}

} // namespace replication
} // namespace dsn
