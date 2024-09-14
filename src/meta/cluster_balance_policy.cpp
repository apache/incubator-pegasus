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

#include "cluster_balance_policy.h"

#include <limits.h>
#include <stdlib.h>
#include <cstdint>
#include <functional>
#include <iterator>
#include <unordered_map>

#include "dsn.layer2_types.h"
#include "gutil/map_util.h"
#include "meta/load_balance_policy.h"
#include "rpc/dns_resolver.h" // IWYU pragma: keep
#include "rpc/rpc_address.h"
#include "rpc/rpc_host_port.h"
#include "utils/flags.h"
#include "utils/fmt_logging.h"
#include "utils/utils.h"

DSN_DEFINE_uint32(meta_server,
                  balance_op_count_per_round,
                  10,
                  "balance operation count per round for cluster balancer");
DSN_TAG_VARIABLE(balance_op_count_per_round, FT_MUTABLE);

namespace dsn {
namespace replication {
class meta_service;

uint32_t get_partition_count(const node_state &ns, balance_type type, int32_t app_id)
{
    unsigned count = 0;
    switch (type) {
    case balance_type::COPY_SECONDARY:
        if (app_id > 0) {
            count = ns.partition_count(app_id) - ns.primary_count(app_id);
        } else {
            count = ns.partition_count() - ns.primary_count();
        }
        break;
    case balance_type::COPY_PRIMARY:
        if (app_id > 0) {
            count = ns.primary_count(app_id);
        } else {
            count = ns.primary_count();
        }
        break;
    default:
        break;
    }
    return (uint32_t)count;
}

uint32_t get_skew(const std::map<host_port, uint32_t> &count_map)
{
    uint32_t min = UINT_MAX, max = 0;
    for (const auto &kv : count_map) {
        if (kv.second < min) {
            min = kv.second;
        }
        if (kv.second > max) {
            max = kv.second;
        }
    }
    return max - min;
}

void get_min_max_set(const std::map<host_port, uint32_t> &node_count_map,
                     /*out*/ std::set<host_port> &min_set,
                     /*out*/ std::set<host_port> &max_set)
{
    std::multimap<uint32_t, host_port> count_multimap = utils::flip_map(node_count_map);

    auto range = count_multimap.equal_range(count_multimap.begin()->first);
    for (auto iter = range.first; iter != range.second; ++iter) {
        min_set.insert(iter->second);
    }

    range = count_multimap.equal_range(count_multimap.rbegin()->first);
    for (auto iter = range.first; iter != range.second; ++iter) {
        max_set.insert(iter->second);
    }
}

cluster_balance_policy::cluster_balance_policy(meta_service *svc) : load_balance_policy(svc) {}

void cluster_balance_policy::balance(bool checker,
                                     const meta_view *global_view,
                                     migration_list *list)
{
    init(global_view, list);

    if (!execute_balance(*_global_view->apps,
                         false, /* balance_checker */
                         true,  /* balance_in_turn */
                         true,  /* only_move_primary */
                         std::bind(&cluster_balance_policy::primary_balance,
                                   this,
                                   std::placeholders::_1,
                                   std::placeholders::_2))) {
        return;
    }

    bool need_continue =
        cluster_replica_balance(_global_view, balance_type::COPY_SECONDARY, *_migration_result);
    if (!need_continue) {
        return;
    }

    cluster_replica_balance(_global_view, balance_type::COPY_PRIMARY, *_migration_result);
}

bool cluster_balance_policy::cluster_replica_balance(const meta_view *global_view,
                                                     const balance_type type,
                                                     /*out*/ migration_list &list)
{
    bool enough_information = do_cluster_replica_balance(global_view, type, list);
    if (!enough_information) {
        return false;
    }
    if (!list.empty()) {
        LOG_INFO("migration count of {} = {}", enum_to_string(type), list.size());
        return false;
    }
    return true;
}

bool cluster_balance_policy::do_cluster_replica_balance(const meta_view *global_view,
                                                        const balance_type type,
                                                        /*out*/ migration_list &list)
{
    cluster_migration_info cluster_info;
    if (!get_cluster_migration_info(global_view, type, cluster_info)) {
        return false;
    }

    partition_set selected_pid;
    move_info next_move;
    while (get_next_move(cluster_info, selected_pid, next_move)) {
        if (!apply_move(next_move, selected_pid, list, cluster_info)) {
            break;
        }
        if (list.size() >= FLAGS_balance_op_count_per_round) {
            break;
        }
    }

    return true;
}

bool cluster_balance_policy::get_cluster_migration_info(
    const meta_view *global_view,
    const balance_type type,
    /*out*/ cluster_migration_info &cluster_info)
{
    const node_mapper &nodes = *global_view->nodes;
    if (nodes.size() < 3) {
        return false;
    }

    const app_mapper &all_apps = *global_view->apps;
    app_mapper apps;
    for (const auto &kv : all_apps) {
        const std::shared_ptr<app_state> &app = kv.second;
        auto ignored = is_ignored_app(app->app_id);
        if (ignored || app->is_bulk_loading || app->splitting()) {
            LOG_INFO("skip to balance app({}), ignored={}, bulk loading={}, splitting={}",
                     app->app_name,
                     ignored,
                     app->is_bulk_loading,
                     app->splitting());
            continue;
        }
        if (app->status == app_status::AS_AVAILABLE) {
            apps[app->app_id] = app;
        }
    }

    for (const auto &kv : apps) {
        std::shared_ptr<app_state> app = kv.second;
        app_migration_info info;
        if (!get_app_migration_info(app, nodes, type, info)) {
            return false;
        }
        cluster_info.apps_info.emplace(kv.first, std::move(info));
        cluster_info.apps_skew[kv.first] = get_skew(info.replicas_count);
    }

    for (const auto &kv : nodes) {
        const node_state &ns = kv.second;
        node_migration_info info;
        get_node_migration_info(ns, apps, info);
        cluster_info.nodes_info.emplace(kv.first, std::move(info));

        auto count = get_partition_count(ns, type, -1);
        cluster_info.replicas_count[kv.first] = count;
    }

    cluster_info.type = type;
    return true;
}

bool cluster_balance_policy::get_app_migration_info(std::shared_ptr<app_state> app,
                                                    const node_mapper &nodes,
                                                    const balance_type type,
                                                    app_migration_info &info)
{
    info.app_id = app->app_id;
    info.app_name = app->app_name;
    info.partitions.reserve(app->pcs.size());
    for (const auto &pc : app->pcs) {
        std::map<host_port, partition_status::type> pstatus_map;
        pstatus_map[pc.hp_primary] = partition_status::PS_PRIMARY;
        if (pc.hp_secondaries.size() != pc.max_replica_count - 1) {
            // partition is unhealthy
            return false;
        }
        for (const auto &secondary : pc.hp_secondaries) {
            pstatus_map[secondary] = partition_status::PS_SECONDARY;
        }
        info.partitions.push_back(std::move(pstatus_map));
    }

    for (const auto &it : nodes) {
        const node_state &ns = it.second;
        auto count = get_partition_count(ns, type, app->app_id);
        info.replicas_count[ns.host_port()] = count;
    }

    return true;
}

void cluster_balance_policy::get_node_migration_info(const node_state &ns,
                                                     const app_mapper &apps,
                                                     /*out*/ node_migration_info &info)
{
    info.hp = ns.host_port();
    for (const auto &iter : apps) {
        std::shared_ptr<app_state> app = iter.second;
        for (const auto &context : app->helpers->contexts) {
            std::string disk_tag;
            if (!context.get_disk_tag(ns.host_port(), disk_tag)) {
                continue;
            }
            auto &partitions_of_disk = gutil::LookupOrInsert(&info.partitions, disk_tag, {});
            partitions_of_disk.insert(context.pc->pid);
        }
    }
}

bool cluster_balance_policy::get_next_move(const cluster_migration_info &cluster_info,
                                           const partition_set &selected_pid,
                                           /*out*/ move_info &next_move)
{
    // key-app skew, value-app id
    std::multimap<uint32_t, int32_t> app_skew_multimap = utils::flip_map(cluster_info.apps_skew);
    auto max_app_skew = app_skew_multimap.rbegin()->first;
    if (max_app_skew == 0) {
        LOG_INFO("every app is balanced and any move will unbalance a app");
        return false;
    }

    auto server_skew = get_skew(cluster_info.replicas_count);
    if (max_app_skew <= 1 && server_skew <= 1) {
        LOG_INFO("every app is balanced and the cluster as a whole is balanced");
        return false;
    }

    /**
     * Among the apps with maximum skew, attempt to pick a app where there is
     * a move that improves the app skew and the cluster skew, if possible. If
     * not, attempt to pick a move that improves the app skew.
     **/
    std::set<host_port> cluster_min_count_nodes;
    std::set<host_port> cluster_max_count_nodes;
    get_min_max_set(cluster_info.replicas_count, cluster_min_count_nodes, cluster_max_count_nodes);

    bool found = false;
    auto app_range = app_skew_multimap.equal_range(max_app_skew);
    for (auto iter = app_range.first; iter != app_range.second; ++iter) {
        auto app_id = iter->second;
        auto it = cluster_info.apps_info.find(app_id);
        if (it == cluster_info.apps_info.end()) {
            continue;
        }
        auto app_map = it->second.replicas_count;
        std::set<host_port> app_min_count_nodes;
        std::set<host_port> app_max_count_nodes;
        get_min_max_set(app_map, app_min_count_nodes, app_max_count_nodes);

        /**
         * Compute the intersection of the replica servers most loaded for the app
         * with the replica servers most loaded overall, and likewise for least loaded.
         * These are our ideal candidates for moving from and to, respectively.
         **/
        std::set<host_port> app_cluster_min_set =
            utils::get_intersection(app_min_count_nodes, cluster_min_count_nodes);
        std::set<host_port> app_cluster_max_set =
            utils::get_intersection(app_max_count_nodes, cluster_max_count_nodes);

        /**
         * Do not move replicas of a balanced app if the least (most) loaded
         * servers overall do not intersect the servers hosting the least (most)
         * replicas of the app. Moving a replica in that case might keep the
         * cluster skew the same or make it worse while keeping the app balanced.
         **/
        std::multimap<uint32_t, host_port> app_count_multimap = utils::flip_map(app_map);
        if (app_count_multimap.rbegin()->first <= app_count_multimap.begin()->first + 1 &&
            (app_cluster_min_set.empty() || app_cluster_max_set.empty())) {
            LOG_INFO("do not move replicas of a balanced app({}) if the least (most) loaded "
                     "servers overall do not intersect the servers hosting the least (most) "
                     "replicas of the app",
                     app_id);
            continue;
        }

        if (pick_up_move(cluster_info,
                         app_cluster_max_set.empty() ? app_max_count_nodes : app_cluster_max_set,
                         app_cluster_min_set.empty() ? app_min_count_nodes : app_cluster_min_set,
                         app_id,
                         selected_pid,
                         next_move)) {
            found = true;
            break;
        }
    }

    return found;
}

template <typename S>
auto select_random(const S &s, size_t n)
{
    auto it = std::begin(s);
    std::advance(it, n);
    return it;
}

bool cluster_balance_policy::pick_up_move(const cluster_migration_info &cluster_info,
                                          const std::set<host_port> &max_nodes,
                                          const std::set<host_port> &min_nodes,
                                          const int32_t app_id,
                                          const partition_set &selected_pid,
                                          /*out*/ move_info &move_info)
{
    std::set<app_disk_info> max_load_disk_set;
    get_max_load_disk_set(cluster_info, max_nodes, app_id, max_load_disk_set);
    if (max_load_disk_set.empty()) {
        return false;
    }
    auto index = rand() % max_load_disk_set.size();
    auto max_load_disk = *select_random(max_load_disk_set, index);
    LOG_INFO("most load disk({}) on node({}) is picked, has {} partition",
             max_load_disk.node,
             max_load_disk.disk_tag,
             max_load_disk.partitions.size());
    for (const auto &node_hp : min_nodes) {
        gpid picked_pid;
        if (pick_up_partition(
                cluster_info, node_hp, max_load_disk.partitions, selected_pid, picked_pid)) {
            move_info.pid = picked_pid;
            move_info.source_node = max_load_disk.node;
            move_info.source_disk_tag = max_load_disk.disk_tag;
            move_info.target_node = node_hp;
            move_info.type = cluster_info.type;
            LOG_INFO("partition[{}] will migrate from {} to {}",
                     picked_pid,
                     max_load_disk.node,
                     node_hp);
            return true;
        }
    }
    LOG_INFO("can not find a partition(app_id={}) from random max load disk(node={}, disk={})",
             app_id,
             max_load_disk.node,
             max_load_disk.disk_tag);
    return false;
}

void cluster_balance_policy::get_max_load_disk_set(
    const cluster_migration_info &cluster_info,
    const std::set<host_port> &max_nodes,
    const int32_t app_id,
    /*out*/ std::set<app_disk_info> &max_load_disk_set)
{
    // key: partition count (app_disk_info.partitions.size())
    // value: app_disk_info structure
    std::multimap<uint32_t, app_disk_info> app_disk_info_multimap;
    for (const auto &node_hp : max_nodes) {
        // key: disk_tag
        // value: partition set for app(app id=app_id) in node(hp=node_hp)
        std::map<std::string, partition_set> disk_partitions =
            get_disk_partitions_map(cluster_info, node_hp, app_id);
        for (const auto &kv : disk_partitions) {
            app_disk_info info;
            info.app_id = app_id;
            info.node = node_hp;
            info.disk_tag = kv.first;
            info.partitions = kv.second;
            app_disk_info_multimap.insert(
                std::pair<uint32_t, app_disk_info>(kv.second.size(), info));
        }
    }
    auto range = app_disk_info_multimap.equal_range(app_disk_info_multimap.rbegin()->first);
    for (auto iter = range.first; iter != range.second; ++iter) {
        max_load_disk_set.insert(iter->second);
    }
}

std::map<std::string, partition_set> cluster_balance_policy::get_disk_partitions_map(
    const cluster_migration_info &cluster_info, const host_port &hp, const int32_t app_id)
{
    std::map<std::string, partition_set> disk_partitions;
    auto app_iter = cluster_info.apps_info.find(app_id);
    auto node_iter = cluster_info.nodes_info.find(hp);
    if (app_iter == cluster_info.apps_info.end() || node_iter == cluster_info.nodes_info.end()) {
        return disk_partitions;
    }

    auto status = cluster_info.type == balance_type::COPY_SECONDARY ? partition_status::PS_SECONDARY
                                                                    : partition_status::PS_PRIMARY;
    auto app_partition = app_iter->second.partitions;
    auto disk_partition = node_iter->second.partitions;
    for (const auto &kv : disk_partition) {
        auto disk_tag = kv.first;
        for (const auto &pid : kv.second) {
            if (pid.get_app_id() != app_id) {
                continue;
            }
            auto status_map = app_partition[pid.get_partition_index()];
            auto iter = status_map.find(hp);
            if (iter != status_map.end() && iter->second == status) {
                disk_partitions[disk_tag].insert(pid);
            }
        }
    }
    return disk_partitions;
}

bool cluster_balance_policy::pick_up_partition(const cluster_migration_info &cluster_info,
                                               const host_port &min_node_hp,
                                               const partition_set &max_load_partitions,
                                               const partition_set &selected_pid,
                                               /*out*/ gpid &picked_pid)
{
    bool found = false;
    for (const auto &pid : max_load_partitions) {
        auto iter = cluster_info.apps_info.find(pid.get_app_id());
        if (iter == cluster_info.apps_info.end()) {
            continue;
        }

        // partition has already in mirgration list
        if (selected_pid.find(pid) != selected_pid.end()) {
            continue;
        }

        // partition has already been primary or secondary on min_node
        app_migration_info info = iter->second;
        if (info.get_partition_status(pid.get_partition_index(), min_node_hp) !=
            partition_status::PS_INACTIVE) {
            continue;
        }

        picked_pid = pid;
        found = true;
        break;
    }
    return found;
}

bool cluster_balance_policy::apply_move(const move_info &move,
                                        /*out*/ partition_set &selected_pids,
                                        /*out*/ migration_list &list,
                                        /*out*/ cluster_migration_info &cluster_info)
{
    int32_t app_id = move.pid.get_app_id();
    const auto &source = move.source_node;
    const auto &target = move.target_node;
    if (cluster_info.apps_skew.find(app_id) == cluster_info.apps_skew.end() ||
        cluster_info.replicas_count.find(source) == cluster_info.replicas_count.end() ||
        cluster_info.replicas_count.find(target) == cluster_info.replicas_count.end() ||
        cluster_info.apps_info.find(app_id) == cluster_info.apps_info.end()) {
        return false;
    }

    app_migration_info app_info = cluster_info.apps_info[app_id];
    if (app_info.partitions.size() <= move.pid.get_partition_index() ||
        app_info.replicas_count.find(source) == app_info.replicas_count.end() ||
        app_info.replicas_count.find(target) == app_info.replicas_count.end()) {
        return false;
    }
    app_info.replicas_count[source]--;
    app_info.replicas_count[target]++;

    auto &pmap = app_info.partitions[move.pid.get_partition_index()];
    host_port primary_hp;
    for (const auto &kv : pmap) {
        if (kv.second == partition_status::PS_PRIMARY) {
            primary_hp = kv.first;
        }
    }
    auto status = cluster_info.type == balance_type::COPY_SECONDARY ? partition_status::PS_SECONDARY
                                                                    : partition_status::PS_PRIMARY;
    auto iter = pmap.find(source);
    if (iter == pmap.end() || iter->second != status) {
        return false;
    }
    pmap.erase(source);
    pmap[target] = status;

    auto iters = cluster_info.nodes_info.find(source);
    auto itert = cluster_info.nodes_info.find(target);
    if (iters == cluster_info.nodes_info.end() || itert == cluster_info.nodes_info.end()) {
        return false;
    }
    node_migration_info node_source = iters->second;
    node_migration_info node_target = itert->second;
    auto it = node_source.partitions.find(move.source_disk_tag);
    if (it == node_source.partitions.end()) {
        return false;
    }
    it->second.erase(move.pid);
    node_target.future_partitions.insert(move.pid);

    // add into the migration list and selected_pid
    partition_configuration pc;
    pc.pid = move.pid;
    SET_IP_AND_HOST_PORT_BY_DNS(pc, primary, primary_hp);
    list[move.pid] = generate_balancer_request(*_global_view->apps, pc, move.type, source, target);
    _migration_result->emplace(
        move.pid, generate_balancer_request(*_global_view->apps, pc, move.type, source, target));
    selected_pids.insert(move.pid);

    cluster_info.apps_skew[app_id] = get_skew(app_info.replicas_count);
    cluster_info.apps_info[app_id] = app_info;
    cluster_info.nodes_info[source] = node_source;
    cluster_info.nodes_info[target] = node_target;
    cluster_info.replicas_count[source]--;
    cluster_info.replicas_count[target]++;
    return true;
}
} // namespace replication
} // namespace dsn
