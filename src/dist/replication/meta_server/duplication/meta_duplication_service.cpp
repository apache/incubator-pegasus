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

#include <dsn/dist/replication/duplication_common.h>
#include <dsn/dist/fmt_logging.h>
#include <dsn/utility/chrono_literals.h>
#include <dsn/utility/string_conv.h>

#include "dist/replication/meta_server/meta_service.h"
#include "meta_duplication_service.h"

namespace dsn {
namespace replication {

using namespace literals::chrono_literals;

// ThreadPool(READ): THREAD_POOL_META_SERVER
void meta_duplication_service::query_duplication_info(const duplication_query_request &request,
                                                      duplication_query_response &response)
{
    ddebug_f("query duplication info for app: {}", request.app_name);

    response.err = ERR_OK;
    {
        zauto_read_lock l(app_lock());
        std::shared_ptr<app_state> app = _state->get_app(request.app_name);
        if (!app || app->status != app_status::AS_AVAILABLE) {
            response.err = ERR_APP_NOT_EXIST;
        } else {
            response.appid = app->app_id;
            for (auto &dup_id_to_info : app->duplications) {
                const duplication_info_s_ptr &dup = dup_id_to_info.second;
                dup->append_if_valid_for_query(response.entry_list);
            }
        }
    }
}

// ThreadPool(WRITE): THREAD_POOL_META_STATE
void meta_duplication_service::change_duplication_status(duplication_status_change_rpc rpc)
{
    const auto &request = rpc.request();
    auto &response = rpc.response();

    ddebug_f("change status of duplication({}) to {} for app({})",
             request.dupid,
             duplication_status_to_string(request.status),
             request.app_name);

    dupid_t dupid = request.dupid;

    std::shared_ptr<app_state> app = _state->get_app(request.app_name);
    if (!app || app->status != app_status::AS_AVAILABLE) {
        response.err = ERR_APP_NOT_EXIST;
        return;
    }

    duplication_info_s_ptr dup = app->duplications[dupid];
    if (dup == nullptr) {
        response.err = ERR_OBJECT_NOT_FOUND;
        return;
    }

    response.err = dup->alter_status(request.status);
    if (response.err != ERR_OK) {
        return;
    }
    if (!dup->is_altering()) {
        return;
    }

    // validation passed
    do_change_duplication_status(app, dup, rpc);
}

// ThreadPool(WRITE): THREAD_POOL_META_STATE
void meta_duplication_service::do_change_duplication_status(std::shared_ptr<app_state> &app,
                                                            duplication_info_s_ptr &dup,
                                                            duplication_status_change_rpc &rpc)
{
    // store the duplication in requested status.
    blob value = dup->to_json_blob();

    _meta_svc->get_meta_storage()->set_data(
        std::string(dup->store_path), std::move(value), [rpc, this, app, dup]() {
            dup->persist_status();
            rpc.response().err = ERR_OK;
            rpc.response().appid = app->app_id;

            if (rpc.request().status == duplication_status::DS_REMOVED) {
                zauto_write_lock l(app_lock());
                app->duplications.erase(dup->id);
                refresh_duplicating_no_lock(app);
            }
        });
}

// This call will not recreate if the duplication
// with the same app name and remote end point already exists.
// ThreadPool(WRITE): THREAD_POOL_META_STATE
void meta_duplication_service::add_duplication(duplication_add_rpc rpc)
{
    const auto &request = rpc.request();
    auto &response = rpc.response();

    ddebug_f("add duplication for app({}), remote cluster name is {}",
             request.app_name,
             request.remote_cluster_name);

    response.err = ERR_OK;

    if (request.remote_cluster_name == get_current_cluster_name()) {
        dwarn("illegal operation: adding duplication to itself");
        response.err = ERR_INVALID_PARAMETERS;
        return;
    }

    auto remote_cluster_id = get_duplication_cluster_id(request.remote_cluster_name);
    if (!remote_cluster_id.is_ok()) {
        dwarn_f("get_duplication_cluster_id({}) failed, error: {}",
                request.remote_cluster_name,
                remote_cluster_id.get_error());
        response.err = ERR_INVALID_PARAMETERS;
        return;
    }

    auto app = _state->get_app(request.app_name);
    if (!app || app->status != app_status::AS_AVAILABLE) {
        response.err = ERR_APP_NOT_EXIST;
        return;
    }
    duplication_info_s_ptr dup;
    for (const auto &ent : app->duplications) {
        auto it = ent.second;
        if (it->remote == request.remote_cluster_name) {
            dup = ent.second;
            break;
        }
    }
    if (!dup) {
        dup = new_dup_from_init(request.remote_cluster_name, app);
    }
    do_add_duplication(app, dup, rpc);
}

// ThreadPool(WRITE): THREAD_POOL_META_STATE
void meta_duplication_service::do_add_duplication(std::shared_ptr<app_state> &app,
                                                  duplication_info_s_ptr &dup,
                                                  duplication_add_rpc &rpc)
{
    dup->start();
    if (rpc.request().freezed) {
        dup->persist_status();
        dup->alter_status(duplication_status::DS_PAUSE);
    }
    blob value = dup->to_json_blob();

    std::queue<std::string> nodes({get_duplication_path(*app), std::to_string(dup->id)});
    _meta_svc->get_meta_storage()->create_node_recursively(
        std::move(nodes), std::move(value), [app, this, dup, rpc]() mutable {
            ddebug_dup(dup,
                       "add duplication successfully [app_name: {}, remote: {}]",
                       app->app_name,
                       dup->remote);

            // The duplication starts only after it's been persisted.
            dup->persist_status();

            auto &resp = rpc.response();
            resp.err = ERR_OK;
            resp.appid = app->app_id;
            resp.dupid = dup->id;

            zauto_write_lock l(app_lock());
            refresh_duplicating_no_lock(app);
        });
}

std::shared_ptr<duplication_info>
meta_duplication_service::new_dup_from_init(const std::string &remote_cluster_name,
                                            std::shared_ptr<app_state> &app) const
{
    duplication_info_s_ptr dup;

    // use current time to identify this duplication.
    auto dupid = static_cast<dupid_t>(dsn_now_ms() / 1000);
    {
        zauto_write_lock l(app_lock());

        // hold write lock here to ensure that dupid is unique
        while (app->duplications.find(dupid) != app->duplications.end())
            dupid++;

        std::string dup_path = get_duplication_path(*app, std::to_string(dupid));
        dup = std::make_shared<duplication_info>(dupid,
                                                 app->app_id,
                                                 app->partition_count,
                                                 dsn_now_ms(),
                                                 remote_cluster_name,
                                                 std::move(dup_path));
        for (int32_t i = 0; i < app->partition_count; i++) {
            dup->init_progress(i, invalid_decree);
        }

        app->duplications.emplace(dup->id, dup);
    }

    return dup;
}

} // namespace replication
} // namespace dsn
