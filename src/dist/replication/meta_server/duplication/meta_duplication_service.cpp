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
                dup->append_if_valid_for_query(*app, response.entry_list);
            }
        }
    }
}

// ThreadPool(WRITE): THREAD_POOL_META_STATE
void meta_duplication_service::modify_duplication(duplication_modify_rpc rpc)
{
    const auto &request = rpc.request();
    auto &response = rpc.response();

    ddebug_f("modify duplication({}) to [status={},fail_mode={}] for app({})",
             request.dupid,
             request.__isset.status ? duplication_status_to_string(request.status) : "nil",
             request.__isset.fail_mode ? duplication_fail_mode_to_string(request.fail_mode) : "nil",
             request.app_name);

    dupid_t dupid = request.dupid;

    std::shared_ptr<app_state> app = _state->get_app(request.app_name);
    if (!app || app->status != app_status::AS_AVAILABLE) {
        response.err = ERR_APP_NOT_EXIST;
        return;
    }

    auto it = app->duplications.find(dupid);
    if (it == app->duplications.end()) {
        response.err = ERR_OBJECT_NOT_FOUND;
        return;
    }

    duplication_info_s_ptr dup = it->second;
    auto to_status = request.__isset.status ? request.status : dup->status();
    auto to_fail_mode = request.__isset.fail_mode ? request.fail_mode : dup->fail_mode();
    response.err = dup->alter_status(to_status, to_fail_mode);
    if (response.err != ERR_OK) {
        return;
    }
    if (!dup->is_altering()) {
        return;
    }

    // validation passed
    do_modify_duplication(app, dup, rpc);
}

// ThreadPool(WRITE): THREAD_POOL_META_STATE
void meta_duplication_service::do_modify_duplication(std::shared_ptr<app_state> &app,
                                                     duplication_info_s_ptr &dup,
                                                     duplication_modify_rpc &rpc)
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
        response.err = ERR_INVALID_PARAMETERS;
        response.__set_hint("illegal operation: adding duplication to itself");
        return;
    }
    auto remote_cluster_id = get_duplication_cluster_id(request.remote_cluster_name);
    if (!remote_cluster_id.is_ok()) {
        response.err = ERR_INVALID_PARAMETERS;
        response.__set_hint(fmt::format("get_duplication_cluster_id({}) failed, error: {}",
                                        request.remote_cluster_name,
                                        remote_cluster_id.get_error()));
        return;
    }
    std::vector<std::string> clusters;
    dsn_config_get_all_keys("pegasus.clusters", clusters);
    if (std::find(clusters.begin(), clusters.end(), request.remote_cluster_name) ==
        clusters.end()) {
        response.err = ERR_INVALID_PARAMETERS;
        response.__set_hint("failed to find cluster address in config [pegasus.clusters]");
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

/// get all available apps on node `ns`
void meta_duplication_service::get_all_available_app(
    const node_state &ns, std::map<int32_t, std::shared_ptr<app_state>> &app_map) const
{
    ns.for_each_partition([this, &ns, &app_map](const gpid &pid) -> bool {
        if (ns.served_as(pid) != partition_status::PS_PRIMARY) {
            return true;
        }

        std::shared_ptr<app_state> app = _state->get_app(pid.get_app_id());
        if (!app || app->status != app_status::AS_AVAILABLE) {
            return true;
        }

        // must have duplication
        if (app->duplications.empty()) {
            return true;
        }

        if (app_map.find(app->app_id) == app_map.end()) {
            app_map.emplace(std::make_pair(pid.get_app_id(), std::move(app)));
        }
        return true;
    });
}

// ThreadPool(WRITE): THREAD_POOL_META_STATE
void meta_duplication_service::duplication_sync(duplication_sync_rpc rpc)
{
    auto &request = rpc.request();
    auto &response = rpc.response();
    response.err = ERR_OK;

    node_state *ns = get_node_state(_state->_nodes, request.node, false);
    if (ns == nullptr) {
        dwarn_f("node({}) is not found in meta server", request.node.to_string());
        response.err = ERR_OBJECT_NOT_FOUND;
        return;
    }

    std::map<int32_t, std::shared_ptr<app_state>> app_map;
    get_all_available_app(*ns, app_map);
    for (const auto &kv : app_map) {
        int32_t app_id = kv.first;
        const auto &app = kv.second;

        for (const auto &kv2 : app->duplications) {
            dupid_t dup_id = kv2.first;
            const auto &dup = kv2.second;
            if (!dup->is_valid()) {
                continue;
            }

            response.dup_map[app_id][dup_id] = dup->to_duplication_entry();

            // report progress periodically for each duplications
            dup->report_progress_if_time_up();
        }
    }

    /// update progress
    for (const auto &kv : request.confirm_list) {
        gpid gpid = kv.first;

        auto it = app_map.find(gpid.get_app_id());
        if (it == app_map.end()) {
            // app is unsynced
            // Since duplication-sync separates with config-sync, it's not guaranteed to have the
            // latest state. duplication-sync has a loose consistency requirement.
            continue;
        }
        std::shared_ptr<app_state> &app = it->second;

        for (const duplication_confirm_entry &confirm : kv.second) {
            auto it2 = app->duplications.find(confirm.dupid);
            if (it2 == app->duplications.end()) {
                // dup is unsynced
                continue;
            }

            duplication_info_s_ptr &dup = it2->second;
            if (!dup->is_valid()) {
                continue;
            }
            do_update_partition_confirmed(
                dup, rpc, gpid.get_partition_index(), confirm.confirmed_decree);
        }
    }
}

void meta_duplication_service::do_update_partition_confirmed(duplication_info_s_ptr &dup,
                                                             duplication_sync_rpc &rpc,
                                                             int32_t partition_idx,
                                                             int64_t confirmed_decree)
{
    if (dup->alter_progress(partition_idx, confirmed_decree)) {
        std::string path = get_partition_path(dup, std::to_string(partition_idx));
        blob value = blob::create_from_bytes(std::to_string(confirmed_decree));

        _meta_svc->get_meta_storage()->get_data(std::string(path), [=](const blob &data) mutable {
            if (data.length() == 0) {
                _meta_svc->get_meta_storage()->create_node(
                    std::string(path), std::move(value), [=]() mutable {
                        dup->persist_progress(partition_idx);
                        rpc.response().dup_map[dup->app_id][dup->id].progress[partition_idx] =
                            confirmed_decree;
                    });
            } else {
                _meta_svc->get_meta_storage()->set_data(
                    std::string(path), std::move(value), [=]() mutable {
                        dup->persist_progress(partition_idx);
                        rpc.response().dup_map[dup->app_id][dup->id].progress[partition_idx] =
                            confirmed_decree;
                    });
            }

            // duplication_sync_rpc will finally be replied when confirmed points
            // of all partitions are stored.
        });
    }
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

// ThreadPool(WRITE): THREAD_POOL_META_STATE
void meta_duplication_service::recover_from_meta_state()
{
    ddebug_f("recovering duplication states from meta storage");

    // /<app>/duplication/<dupid>/<partition_idx>
    //                       |         |-> confirmed_decree
    //                       |
    //                       |-> json of dup info

    for (const auto &kv : _state->_exist_apps) {
        std::shared_ptr<app_state> app = kv.second;
        if (app->status != app_status::AS_AVAILABLE) {
            continue;
        }

        _meta_svc->get_meta_storage()->get_children(
            get_duplication_path(*app),
            [this, app](bool node_exists, const std::vector<std::string> &dup_id_list) {
                if (!node_exists) {
                    // if there's no duplication
                    return;
                }
                for (const std::string &raw_dup_id : dup_id_list) {
                    dupid_t dup_id;
                    if (!buf2int32(raw_dup_id, dup_id)) {
                        // unlikely
                        derror_f("invalid duplication path: {}",
                                 get_duplication_path(*app, raw_dup_id));
                        return;
                    }
                    do_restore_duplication(dup_id, app);
                }
            });
    }
}

// ThreadPool(WRITE): THREAD_POOL_META_STATE
void meta_duplication_service::do_restore_duplication_progress(
    const duplication_info_s_ptr &dup, const std::shared_ptr<app_state> &app)
{
    for (int partition_idx = 0; partition_idx < app->partition_count; partition_idx++) {
        std::string str_pidx = std::to_string(partition_idx);

        // <app_path>/duplication/<dup_id>/<partition_index>
        std::string partition_path = get_partition_path(dup, str_pidx);

        _meta_svc->get_meta_storage()->get_data(
            std::move(partition_path), [dup, partition_idx](const blob &value) {
                // value is confirmed_decree encoded in string.

                if (value.size() == 0) {
                    // not found
                    dup->init_progress(partition_idx, invalid_decree);
                    return;
                }

                int64_t confirmed_decree = invalid_decree;
                if (!buf2int64(value, confirmed_decree)) {
                    derror_dup(dup,
                               "invalid confirmed_decree {} on partition_idx {}",
                               value.to_string(),
                               partition_idx);
                    return; // fail fast
                }

                dup->init_progress(partition_idx, confirmed_decree);

                ddebug_dup(dup,
                           "initialize progress from metastore [partition_idx: {}, confirmed: {}]",
                           partition_idx,
                           confirmed_decree);
            });
    }
}

// ThreadPool(WRITE): THREAD_POOL_META_STATE
void meta_duplication_service::do_restore_duplication(dupid_t dup_id,
                                                      std::shared_ptr<app_state> app)
{
    std::string store_path = get_duplication_path(*app, std::to_string(dup_id));

    // restore duplication info from json
    _meta_svc->get_meta_storage()->get_data(
        std::string(store_path),
        [ dup_id, this, app = std::move(app), store_path ](const blob &json) {
            zauto_write_lock l(app_lock());

            auto dup = duplication_info::decode_from_blob(
                dup_id, app->app_id, app->partition_count, store_path, json);
            if (nullptr == dup) {
                derror_f("failed to decode json \"{}\" on path {}", json.to_string(), store_path);
                return; // fail fast
            }
            if (dup->is_valid()) {
                app->duplications[dup->id] = dup;
                refresh_duplicating_no_lock(app);

                // restore progress
                do_restore_duplication_progress(dup, app);
            }
        });
}

} // namespace replication
} // namespace dsn
