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
#include <cstdint>
#include <queue>
#include <type_traits>

#include "absl/strings/string_view.h"
#include "common//duplication_common.h"
#include "common/common.h"
#include "common/gpid.h"
#include "common/replication.codes.h"
#include "common/replication_enums.h"
#include "common/replication_other_types.h"
#include "dsn.layer2_types.h"
#include "duplication_types.h"
#include "meta/meta_service.h"
#include "meta/meta_state_service_utils.h"
#include "meta_admin_types.h"
#include "meta_duplication_service.h"
#include "metadata_types.h"
#include "runtime/api_layer1.h"
#include "runtime/rpc/dns_resolver.h"
#include "runtime/rpc/group_host_port.h"
#include "runtime/rpc/rpc_address.h"
#include "runtime/rpc/rpc_host_port.h"
#include "runtime/rpc/rpc_message.h"
#include "runtime/rpc/serialization.h"
#include "runtime/task/async_calls.h"
#include "utils/api_utilities.h"
#include "utils/blob.h"
#include "utils/chrono_literals.h"
#include "utils/error_code.h"
#include "utils/errors.h"
#include "utils/fail_point.h"
#include "utils/fmt_logging.h"
#include "utils/ports.h"
#include "utils/string_conv.h"
#include "utils/zlocks.h"

namespace dsn {
namespace replication {

using namespace literals::chrono_literals;

// ThreadPool(READ): THREAD_POOL_META_SERVER
void meta_duplication_service::query_duplication_info(const duplication_query_request &request,
                                                      duplication_query_response &response)
{
    LOG_INFO("query duplication info for app: {}", request.app_name);

    response.err = ERR_OK;
    {
        zauto_read_lock l(app_lock());
        std::shared_ptr<app_state> app = _state->get_app(request.app_name);
        if (!app || app->status != app_status::AS_AVAILABLE) {
            response.err = ERR_APP_NOT_EXIST;
            return;
        }

        response.appid = app->app_id;
        for (const auto & [ _, dup ] : app->duplications) {
            dup->append_if_valid_for_query(*app, response.entry_list);
        }
    }
}

// ThreadPool(WRITE): THREAD_POOL_META_STATE
void meta_duplication_service::modify_duplication(duplication_modify_rpc rpc)
{
    const auto &request = rpc.request();
    auto &response = rpc.response();

    LOG_INFO("modify duplication({}) to [status={},fail_mode={}] for app({})",
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
    if (rpc.request().status == duplication_status::DS_REMOVED) {
        _meta_svc->get_meta_storage()->delete_node_recursively(
            std::string(dup->store_path), [rpc, this, app, dup]() {
                dup->persist_status();
                rpc.response().err = ERR_OK;
                rpc.response().appid = app->app_id;

                if (rpc.request().status == duplication_status::DS_REMOVED) {
                    zauto_write_lock l(app_lock());
                    app->duplications.erase(dup->id);
                    refresh_duplicating_no_lock(app);
                }
            });
        return;
    }
    // store the duplication in requested status.
    blob value = dup->to_json_blob();
    _meta_svc->get_meta_storage()->set_data(
        std::string(dup->store_path), std::move(value), [rpc, app, dup]() {
            dup->persist_status();
            rpc.response().err = ERR_OK;
            rpc.response().appid = app->app_id;
        });
}

#define LOG_DUP_HINT_AND_RETURN(resp, level, ...)                                                  \
    do {                                                                                           \
        const std::string _msg(fmt::format(__VA_ARGS__));                                          \
        (resp).__set_hint(_msg);                                                                   \
        LOG(level, _msg);                                                                          \
        return;                                                                                    \
    } while (0)

#define LOG_DUP_HINT_AND_RETURN_IF_NOT(expr, resp, ec, level, ...)                                 \
    do {                                                                                           \
        if (dsn_likely(expr)) {                                                                    \
            break;                                                                                 \
        }                                                                                          \
                                                                                                   \
        (resp).err = (ec);                                                                         \
        LOG_DUP_HINT_AND_RETURN(resp, level, __VA_ARGS__);                                         \
    } while (0)

#define LOG_WARNING_DUP_HINT_AND_RETURN_IF_NOT(expr, resp, ec, ...)                                \
    LOG_DUP_HINT_AND_RETURN_IF_NOT(expr, resp, ec, LOG_LEVEL_WARNING, __VA_ARGS__)

#define LOG_ERROR_DUP_HINT_AND_RETURN_IF_NOT(expr, resp, ec, ...)                                  \
    LOG_DUP_HINT_AND_RETURN_IF_NOT(expr, resp, ec, LOG_LEVEL_ERROR, __VA_ARGS__)

// This call will not recreate if the duplication
// with the same app name and remote end point already exists.
// ThreadPool(WRITE): THREAD_POOL_META_STATE
void meta_duplication_service::add_duplication(duplication_add_rpc rpc)
{
    const auto &request = rpc.request();
    auto &response = rpc.response();

    std::string remote_app_name;
    if (request.__isset.remote_app_name) {
        remote_app_name = request.remote_app_name;
    } else {
        // Once remote_app_name is not specified by client, use source app_name as
        // remote_app_name to be compatible with old versions(< v2.6.0) of client.
        remote_app_name = request.app_name;
    }

    LOG_INFO("add duplication for app({}), remote cluster name is {}, "
             "remote app name is {}",
             request.app_name,
             request.remote_cluster_name,
             remote_app_name);

    LOG_WARNING_DUP_HINT_AND_RETURN_IF_NOT(request.remote_cluster_name !=
                                               get_current_cluster_name(),
                                           response,
                                           ERR_INVALID_PARAMETERS,
                                           "illegal operation: adding duplication to itself");

    auto remote_cluster_id = get_duplication_cluster_id(request.remote_cluster_name);
    LOG_WARNING_DUP_HINT_AND_RETURN_IF_NOT(remote_cluster_id.is_ok(),
                                           response,
                                           ERR_INVALID_PARAMETERS,
                                           "get_duplication_cluster_id({}) failed, error: {}",
                                           request.remote_cluster_name,
                                           remote_cluster_id.get_error());

    std::vector<host_port> meta_list;
    LOG_WARNING_DUP_HINT_AND_RETURN_IF_NOT(dsn::replication::replica_helper::load_meta_servers(
                                               meta_list,
                                               duplication_constants::kClustersSectionName.c_str(),
                                               request.remote_cluster_name.c_str()),
                                           response,
                                           ERR_INVALID_PARAMETERS,
                                           "failed to find cluster[{}] address in config [{}]",
                                           request.remote_cluster_name,
                                           duplication_constants::kClustersSectionName);

    std::shared_ptr<app_state> app;
    duplication_info_s_ptr dup;
    {
        zauto_read_lock l(app_lock());

        app = _state->get_app(request.app_name);
        // The reason why using !!app rather than just app is that passing std::shared_ptr into
        // dsn_likely(i.e. __builtin_expect) would lead to compilation error "cannot convert
        // 'std::shared_ptr<dsn::replication::app_state>' to 'long int'".
        LOG_WARNING_DUP_HINT_AND_RETURN_IF_NOT(
            !!app, response, ERR_APP_NOT_EXIST, "app {} was not found", request.app_name);
        LOG_WARNING_DUP_HINT_AND_RETURN_IF_NOT(app->status == app_status::AS_AVAILABLE,
                                               response,
                                               ERR_APP_NOT_EXIST,
                                               "app status was not AS_AVAILABLE: name={}, "
                                               "status={}",
                                               request.app_name,
                                               enum_to_string(app->status));

        for (const auto & [ _, dup_info ] : app->duplications) {
            if (dup_info->remote_cluster_name == request.remote_cluster_name) {
                dup = dup_info;
                break;
            }
        }

        if (dup) {
            // The duplication for the same app to the same remote cluster has existed.
            remote_app_name = dup->remote_app_name;
            LOG_INFO("no need to add duplication, since it has existed: app_name={}, "
                     "remote_cluster_name={}, remote_app_name={}",
                     request.app_name,
                     request.remote_cluster_name,
                     remote_app_name);
        } else {
            // Check if other apps of this cluster are duplicated to the same remote app.
            for (const auto & [ app_name, cur_app_state ] : _state->_exist_apps) {
                if (app_name == request.app_name) {
                    // Skip this app since we want to check other apps.
                    continue;
                }

                for (const auto & [ _, dup_info ] : cur_app_state->duplications) {
                    LOG_WARNING_DUP_HINT_AND_RETURN_IF_NOT(
                        dup_info->remote_cluster_name != request.remote_cluster_name ||
                            dup_info->remote_app_name != remote_app_name,
                        response,
                        ERR_INVALID_PARAMETERS,
                        "illegal operation: another app({}) is also "
                        "duplicated to the same remote app("
                        "cluster={}, app={})",
                        app_name,
                        request.remote_cluster_name,
                        remote_app_name);
                }
            }
        }
    }

    if (!dup) {
        dup = new_dup_from_init(
            request.remote_cluster_name, remote_app_name, std::move(meta_list), app);
    }

    do_add_duplication(app, dup, rpc, remote_app_name);
}

// ThreadPool(WRITE): THREAD_POOL_META_STATE
void meta_duplication_service::do_add_duplication(std::shared_ptr<app_state> &app,
                                                  duplication_info_s_ptr &dup,
                                                  duplication_add_rpc &rpc,
                                                  const std::string &remote_app_name)
{
    const auto &ec = dup->start(rpc.request().is_duplicating_checkpoint);
    LOG_ERROR_DUP_HINT_AND_RETURN_IF_NOT(ec == ERR_OK,
                                         rpc.response(),
                                         ec,
                                         "start dup[{}({})] failed: err = {}",
                                         app->app_name,
                                         dup->id,
                                         ec);

    auto value = dup->to_json_blob();
    std::queue<std::string> nodes({get_duplication_path(*app), std::to_string(dup->id)});
    _meta_svc->get_meta_storage()->create_node_recursively(
        std::move(nodes), std::move(value), [app, this, dup, rpc, remote_app_name]() mutable {
            LOG_INFO("[{}] add duplication successfully [app_name: {}, follower: {}]",
                     dup->log_prefix(),
                     app->app_name,
                     dup->remote_cluster_name);

            // The duplication starts only after it's been persisted.
            dup->persist_status();

            auto &resp = rpc.response();
            resp.err = ERR_OK;
            resp.appid = app->app_id;
            resp.dupid = dup->id;
            resp.__set_remote_app_name(remote_app_name);

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

    node_state *ns = get_node_state(_state->_nodes, host_port::from_address(request.node), false);
    if (ns == nullptr) {
        LOG_WARNING("node({}) is not found in meta server", request.node);
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
            if (dup->is_invalid_status()) {
                continue;
            }

            if (dup->status() < duplication_status::DS_LOG && dup->all_checkpoint_has_prepared()) {
                if (dup->status() == duplication_status::DS_PREPARE) {
                    create_follower_app_for_duplication(dup, app);
                } else if (dup->status() == duplication_status::DS_APP) {
                    check_follower_app_if_create_completed(dup);
                }
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
            if (dup->is_invalid_status()) {
                continue;
            }
            do_update_partition_confirmed(dup, rpc, gpid.get_partition_index(), confirm);
        }
    }
}

void meta_duplication_service::create_follower_app_for_duplication(
    const std::shared_ptr<duplication_info> &dup, const std::shared_ptr<app_state> &app)
{
    configuration_create_app_request request;
    request.app_name = dup->remote_app_name;
    request.options.app_type = app->app_type;
    request.options.partition_count = app->partition_count;
    request.options.replica_count = app->max_replica_count;
    request.options.success_if_exist = false;
    request.options.envs = app->envs;
    request.options.is_stateful = app->is_stateful;

    // add envs for follower table, which will use it know itself is `follower` and load master info
    // - env map:
    // `kDuplicationEnvMasterClusterKey=>{master_cluster_name}`
    // `kDuplicationEnvMasterMetasKey=>{master_meta_list}`
    request.options.envs.emplace(duplication_constants::kDuplicationEnvMasterClusterKey,
                                 get_current_cluster_name());
    request.options.envs.emplace(duplication_constants::kDuplicationEnvMasterMetasKey,
                                 _meta_svc->get_meta_list_string());
    request.options.envs.emplace(duplication_constants::kDuplicationEnvMasterAppNameKey,
                                 app->app_name);

    host_port meta_servers;
    meta_servers.assign_group(dup->remote_cluster_name.c_str());
    meta_servers.group_host_port()->add_list(dup->remote_cluster_metas);

    dsn::message_ex *msg = dsn::message_ex::create_request(RPC_CM_CREATE_APP);
    dsn::marshall(msg, request);
    rpc::call(
        dsn::dns_resolver::instance().resolve_address(meta_servers),
        msg,
        _meta_svc->tracker(),
        [=](error_code err, configuration_create_app_response &&resp) mutable {
            FAIL_POINT_INJECT_NOT_RETURN_F("update_app_request_ok",
                                           [&](absl::string_view s) -> void { err = ERR_OK; });
            error_code create_err = err == ERR_OK ? resp.err : err;
            error_code update_err = ERR_NO_NEED_OPERATE;

            FAIL_POINT_INJECT_NOT_RETURN_F(
                "persist_dup_status_failed",
                [&](absl::string_view s) -> void { create_err = ERR_OK; });
            if (create_err == ERR_OK) {
                update_err = dup->alter_status(duplication_status::DS_APP);
            }

            FAIL_POINT_INJECT_F("persist_dup_status_failed",
                                [&](absl::string_view s) -> void { return; });
            if (update_err == ERR_OK) {
                blob value = dup->to_json_blob();
                // Note: this function is `async`, it may not be persisted completed
                // after executing, now using `_is_altering` to judge whether `updating` or
                // `completed`, if `_is_altering`, dup->alter_status() will return `ERR_BUSY`
                _meta_svc->get_meta_storage()->set_data(std::string(dup->store_path),
                                                        std::move(value),
                                                        [=]() { dup->persist_status(); });
            } else {
                LOG_ERROR("created follower app[{}.{}] to trigger duplicate checkpoint failed: "
                          "duplication_status = {}, create_err = {}, update_err = {}",
                          dup->remote_cluster_name,
                          dup->app_name,
                          duplication_status_to_string(dup->status()),
                          create_err,
                          update_err);
            }
        });
}

void meta_duplication_service::check_follower_app_if_create_completed(
    const std::shared_ptr<duplication_info> &dup)
{
    host_port meta_servers;
    meta_servers.assign_group(dup->remote_cluster_name.c_str());
    meta_servers.group_host_port()->add_list(dup->remote_cluster_metas);

    query_cfg_request meta_config_request;
    meta_config_request.app_name = dup->app_name;

    dsn::message_ex *msg = dsn::message_ex::create_request(RPC_CM_QUERY_PARTITION_CONFIG_BY_INDEX);
    dsn::marshall(msg, meta_config_request);
    rpc::call(dsn::dns_resolver::instance().resolve_address(meta_servers),
              msg,
              _meta_svc->tracker(),
              [=](error_code err, query_cfg_response &&resp) mutable {
                  FAIL_POINT_INJECT_NOT_RETURN_F("create_app_ok", [&](absl::string_view s) -> void {
                      err = ERR_OK;
                      int count = dup->partition_count;
                      while (count-- > 0) {
                          partition_configuration p;
                          p.primary = rpc_address::from_ip_port("127.0.0.1", 34801);
                          p.secondaries.emplace_back(rpc_address::from_ip_port("127.0.0.2", 34801));
                          p.secondaries.emplace_back(rpc_address::from_ip_port("127.0.0.3", 34801));
                          p.__set_hp_primary(host_port("localhost", 34801));
                          p.__set_hp_secondaries(std::vector<host_port>());
                          p.hp_secondaries.emplace_back(host_port("localhost", 34802));
                          p.hp_secondaries.emplace_back(host_port("localhost", 34803));
                          resp.partitions.emplace_back(p);
                      }
                  });

                  // - ERR_INCONSISTENT_STATE: partition count of response isn't equal with local
                  // - ERR_INACTIVE_STATE: the follower table hasn't been healthy
                  error_code query_err = err == ERR_OK ? resp.err : err;
                  if (query_err == ERR_OK) {
                      if (resp.partitions.size() != dup->partition_count) {
                          query_err = ERR_INCONSISTENT_STATE;
                      } else {
                          for (const auto &partition : resp.partitions) {
                              if (partition.hp_primary.is_invalid()) {
                                  query_err = ERR_INACTIVE_STATE;
                                  break;
                              }

                              if (partition.hp_secondaries.empty()) {
                                  query_err = ERR_NOT_ENOUGH_MEMBER;
                                  break;
                              }

                              for (const auto &secondary : partition.hp_secondaries) {
                                  if (secondary.is_invalid()) {
                                      query_err = ERR_INACTIVE_STATE;
                                      break;
                                  }
                              }
                          }
                      }
                  }

                  error_code update_err = ERR_NO_NEED_OPERATE;
                  if (query_err == ERR_OK) {
                      update_err = dup->alter_status(duplication_status::DS_LOG);
                  }

                  FAIL_POINT_INJECT_F("persist_dup_status_failed",
                                      [&](absl::string_view s) -> void { return; });
                  if (update_err == ERR_OK) {
                      blob value = dup->to_json_blob();
                      // Note: this function is `async`, it may not be persisted completed
                      // after executing, now using `_is_altering` to judge whether `updating` or
                      // `completed`, if `_is_altering`, dup->alter_status() will return `ERR_BUSY`
                      _meta_svc->get_meta_storage()->set_data(std::string(dup->store_path),
                                                              std::move(value),
                                                              [dup]() { dup->persist_status(); });
                  } else {
                      LOG_ERROR(
                          "query follower app[{}.{}] replica configuration completed, result: "
                          "duplication_status = {}, query_err = {}, update_err = {}",
                          dup->remote_cluster_name,
                          dup->app_name,
                          duplication_status_to_string(dup->status()),
                          query_err,
                          update_err);
                  }
              });
}

void meta_duplication_service::do_update_partition_confirmed(
    duplication_info_s_ptr &dup,
    duplication_sync_rpc &rpc,
    int32_t partition_idx,
    const duplication_confirm_entry &confirm_entry)
{
    if (dup->alter_progress(partition_idx, confirm_entry)) {
        std::string path = get_partition_path(dup, std::to_string(partition_idx));
        blob value = blob::create_from_bytes(std::to_string(confirm_entry.confirmed_decree));

        _meta_svc->get_meta_storage()->get_data(std::string(path), [=](const blob &data) mutable {
            if (data.length() == 0) {
                _meta_svc->get_meta_storage()->create_node(
                    std::string(path), std::move(value), [=]() mutable {
                        dup->persist_progress(partition_idx);
                        rpc.response().dup_map[dup->app_id][dup->id].progress[partition_idx] =
                            confirm_entry.confirmed_decree;
                    });
            } else {
                _meta_svc->get_meta_storage()->set_data(
                    std::string(path), std::move(value), [=]() mutable {
                        dup->persist_progress(partition_idx);
                        rpc.response().dup_map[dup->app_id][dup->id].progress[partition_idx] =
                            confirm_entry.confirmed_decree;
                    });
            }

            // duplication_sync_rpc will finally be replied when confirmed points
            // of all partitions are stored.
        });
    }
}

std::shared_ptr<duplication_info>
meta_duplication_service::new_dup_from_init(const std::string &remote_cluster_name,
                                            const std::string &remote_app_name,
                                            std::vector<host_port> &&remote_cluster_metas,
                                            std::shared_ptr<app_state> &app) const
{
    duplication_info_s_ptr dup;

    // Use current time to identify this duplication.
    auto dupid = static_cast<dupid_t>(dsn_now_s());
    {
        zauto_write_lock l(app_lock());

        // Hold write lock here to ensure that dupid is unique.
        for (; app->duplications.find(dupid) != app->duplications.end(); ++dupid) {
        }

        std::string dup_path = get_duplication_path(*app, std::to_string(dupid));
        dup = std::make_shared<duplication_info>(dupid,
                                                 app->app_id,
                                                 app->app_name,
                                                 app->partition_count,
                                                 dsn_now_ms(),
                                                 remote_cluster_name,
                                                 remote_app_name,
                                                 std::move(remote_cluster_metas),
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
    LOG_INFO("recovering duplication states from meta storage");

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
                        LOG_ERROR("invalid duplication path: {}",
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
                if (!buf2int64(value.to_string_view(), confirmed_decree)) {
                    LOG_ERROR("[{}] invalid confirmed_decree {} on partition_idx {}",
                              dup->log_prefix(),
                              value,
                              partition_idx);
                    return; // fail fast
                }

                dup->init_progress(partition_idx, confirmed_decree);

                LOG_INFO(
                    "[{}] initialize progress from metastore [partition_idx: {}, confirmed: {}]",
                    dup->log_prefix(),
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
                dup_id, app->app_id, app->app_name, app->partition_count, store_path, json);
            if (nullptr == dup) {
                LOG_ERROR("failed to decode json \"{}\" on path {}", json, store_path);
                return; // fail fast
            }
            if (!dup->is_invalid_status()) {
                app->duplications[dup->id] = dup;
                refresh_duplicating_no_lock(app);

                // restore progress
                do_restore_duplication_progress(dup, app);
            }
        });
}

} // namespace replication
} // namespace dsn
