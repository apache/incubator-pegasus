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

// IWYU pragma: no_include <ext/alloc_traits.h>
#include <cstdint>
#include <iterator>
#include <list>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/gpid.h"
#include "dsn.layer2_types.h"
#include "gtest/gtest.h"
#include "meta/cluster_balance_policy.h"
#include "meta/load_balance_policy.h"
#include "meta/meta_data.h"
#include "meta_admin_types.h"
#include "metadata_types.h"
#include "runtime/rpc/rpc_address.h"
#include "utils/defer.h"
#include "utils/fail_point.h"

namespace dsn {
namespace replication {

TEST(cluster_balance_policy, app_migration_info)
{
    {
        cluster_balance_policy::app_migration_info info1;
        info1.app_id = 1;
        cluster_balance_policy::app_migration_info info2;
        info2.app_id = 2;
        ASSERT_LT(info1, info2);
    }

    {
        cluster_balance_policy::app_migration_info info1;
        info1.app_id = 2;
        cluster_balance_policy::app_migration_info info2;
        info2.app_id = 2;
        ASSERT_EQ(info1, info2);
    }
}

TEST(cluster_balance_policy, node_migration_info)
{
    {
        cluster_balance_policy::node_migration_info info1;
        info1.address = rpc_address(1, 10086);
        cluster_balance_policy::node_migration_info info2;
        info2.address = rpc_address(2, 10086);
        ASSERT_LT(info1, info2);
    }

    {
        cluster_balance_policy::node_migration_info info1;
        info1.address = rpc_address(1, 10000);
        cluster_balance_policy::node_migration_info info2;
        info2.address = rpc_address(1, 10086);
        ASSERT_LT(info1, info2);
    }

    {
        cluster_balance_policy::node_migration_info info1;
        info1.address = rpc_address(1, 10086);
        cluster_balance_policy::node_migration_info info2;
        info2.address = rpc_address(1, 10086);
        ASSERT_EQ(info1, info2);
    }
}

TEST(cluster_balance_policy, get_skew)
{
    std::map<rpc_address, uint32_t> count_map = {
        {rpc_address(1, 10086), 1}, {rpc_address(2, 10086), 3}, {rpc_address(3, 10086), 5},
    };

    ASSERT_EQ(get_skew(count_map), count_map.rbegin()->second - count_map.begin()->second);
}

TEST(cluster_balance_policy, get_partition_count)
{
    node_state ns;
    int appid = 1;
    ns.put_partition(gpid(appid, 0), true);
    ns.put_partition(gpid(appid, 1), false);
    ns.put_partition(gpid(appid, 2), false);
    ns.put_partition(gpid(appid, 3), false);

    ASSERT_EQ(get_partition_count(ns, balance_type::COPY_PRIMARY, appid), 1);
    ASSERT_EQ(get_partition_count(ns, balance_type::COPY_SECONDARY, appid), 3);
}

TEST(cluster_balance_policy, get_app_migration_info)
{
    cluster_balance_policy policy(nullptr);

    int appid = 1;
    std::string appname = "test";
    auto address = rpc_address(1, 10086);
    app_info info;
    info.app_id = appid;
    info.app_name = appname;
    info.partition_count = 1;
    auto app = std::make_shared<app_state>(info);
    app->partitions[0].primary = address;

    node_state ns;
    ns.set_addr(address);
    ns.put_partition(gpid(appid, 0), true);
    node_mapper nodes;
    nodes[address] = ns;

    cluster_balance_policy::app_migration_info migration_info;
    {
        app->partitions[0].max_replica_count = 100;
        auto res =
            policy.get_app_migration_info(app, nodes, balance_type::COPY_PRIMARY, migration_info);
        ASSERT_FALSE(res);
    }

    {
        app->partitions[0].max_replica_count = 1;
        auto res =
            policy.get_app_migration_info(app, nodes, balance_type::COPY_PRIMARY, migration_info);
        ASSERT_TRUE(res);
        ASSERT_EQ(migration_info.app_id, appid);
        ASSERT_EQ(migration_info.app_name, appname);
        std::map<rpc_address, partition_status::type> pstatus_map;
        pstatus_map[address] = partition_status::type::PS_PRIMARY;
        ASSERT_EQ(migration_info.partitions[0], pstatus_map);
        ASSERT_EQ(migration_info.replicas_count[address], 1);
    }
}

TEST(cluster_balance_policy, get_node_migration_info)
{
    cluster_balance_policy policy(nullptr);

    int appid = 1;
    std::string appname = "test";
    auto address = rpc_address(1, 10086);
    app_info info;
    info.app_id = appid;
    info.app_name = appname;
    info.partition_count = 1;
    auto app = std::make_shared<app_state>(info);
    app->partitions[0].primary = address;
    serving_replica sr;
    sr.node = address;
    std::string disk_tag = "disk1";
    sr.disk_tag = disk_tag;
    config_context context;
    context.config_owner = new partition_configuration();
    auto cleanup = dsn::defer([&context]() { delete context.config_owner; });
    context.config_owner->pid = gpid(appid, 0);
    context.serving.emplace_back(std::move(sr));
    app->helpers->contexts.emplace_back(std::move(context));

    app_mapper all_apps;
    all_apps[appid] = app;

    node_state ns;
    ns.set_addr(address);
    gpid pid = gpid(appid, 0);
    ns.put_partition(pid, true);

    cluster_balance_policy::node_migration_info migration_info;
    policy.get_node_migration_info(ns, all_apps, migration_info);

    ASSERT_EQ(migration_info.address, address);
    ASSERT_NE(migration_info.partitions.find(disk_tag), migration_info.partitions.end());
    ASSERT_EQ(migration_info.partitions.at(disk_tag).size(), 1);
    ASSERT_EQ(*migration_info.partitions.at(disk_tag).begin(), pid);
}

TEST(cluster_balance_policy, get_min_max_set)
{
    std::map<rpc_address, uint32_t> node_count_map;
    node_count_map.emplace(rpc_address(1, 10086), 1);
    node_count_map.emplace(rpc_address(2, 10086), 3);
    node_count_map.emplace(rpc_address(3, 10086), 5);
    node_count_map.emplace(rpc_address(4, 10086), 5);

    std::set<rpc_address> min_set, max_set;
    get_min_max_set(node_count_map, min_set, max_set);

    ASSERT_EQ(min_set.size(), 1);
    ASSERT_EQ(*min_set.begin(), rpc_address(1, 10086));
    ASSERT_EQ(max_set.size(), 2);
    ASSERT_EQ(*max_set.begin(), rpc_address(3, 10086));
    ASSERT_EQ(*max_set.rbegin(), rpc_address(4, 10086));
}

TEST(cluster_balance_policy, get_disk_partitions_map)
{
    cluster_balance_policy policy(nullptr);
    cluster_balance_policy::cluster_migration_info cluster_info;
    rpc_address addr(1, 10086);
    int32_t app_id = 1;

    auto disk_partitions = policy.get_disk_partitions_map(cluster_info, addr, app_id);
    ASSERT_TRUE(disk_partitions.empty());

    std::map<rpc_address, partition_status::type> partition;
    partition[addr] = partition_status::PS_SECONDARY;
    cluster_balance_policy::app_migration_info app_info;
    app_info.partitions.push_back(partition);
    cluster_info.apps_info[app_id] = app_info;

    partition_set partitions;
    gpid pid(app_id, 0);
    partitions.insert(pid);
    cluster_balance_policy::node_migration_info node_info;
    std::string disk_tag = "disk1";
    node_info.partitions[disk_tag] = partitions;
    cluster_info.nodes_info[addr] = node_info;

    cluster_info.type = balance_type::COPY_SECONDARY;
    disk_partitions = policy.get_disk_partitions_map(cluster_info, addr, app_id);
    ASSERT_EQ(disk_partitions.size(), 1);
    ASSERT_EQ(disk_partitions.count(disk_tag), 1);
    ASSERT_EQ(disk_partitions[disk_tag].size(), 1);
    ASSERT_EQ(disk_partitions[disk_tag].count(pid), 1);
}

TEST(cluster_balance_policy, get_max_load_disk_set)
{
    cluster_balance_policy::cluster_migration_info cluster_info;
    cluster_info.type = balance_type::COPY_SECONDARY;

    int32_t app_id = 1;
    rpc_address addr(1, 10086);
    rpc_address addr2(2, 10086);
    std::map<rpc_address, partition_status::type> partition;
    partition[addr] = partition_status::PS_SECONDARY;
    std::map<rpc_address, partition_status::type> partition2;
    partition2[addr] = partition_status::PS_SECONDARY;
    partition2[addr2] = partition_status::PS_SECONDARY;
    cluster_balance_policy::app_migration_info app_info;
    app_info.partitions.push_back(partition);
    app_info.partitions.push_back(partition2);
    cluster_info.apps_info[app_id] = app_info;

    cluster_balance_policy::node_migration_info node_info;
    partition_set partitions;
    gpid pid(app_id, 0);
    partitions.insert(pid);
    std::string disk_tag = "disk1";
    node_info.partitions[disk_tag] = partitions;
    partition_set partitions2;
    gpid pid2(app_id, 1);
    partitions2.insert(pid2);
    std::string disk_tag2 = "disk2";
    node_info.partitions[disk_tag2] = partitions2;
    cluster_info.nodes_info[addr] = node_info;

    cluster_balance_policy::node_migration_info node_info2;
    partition_set partitions3;
    gpid pid3(app_id, 1);
    partitions3.insert(pid3);
    std::string disk_tag3 = "disk3";
    node_info2.partitions[disk_tag3] = partitions3;
    cluster_info.nodes_info[addr2] = node_info2;

    cluster_balance_policy policy(nullptr);
    std::set<rpc_address> max_nodes;
    max_nodes.insert(addr);
    max_nodes.insert(addr2);

    std::set<cluster_balance_policy::app_disk_info> max_load_disk_set;
    policy.get_max_load_disk_set(cluster_info, max_nodes, app_id, max_load_disk_set);

    ASSERT_EQ(max_load_disk_set.size(), 3);
}

TEST(cluster_balance_policy, apply_move)
{
    struct cluster_balance_policy::move_info minfo;
    int32_t app_id = 1;
    int32_t partition_index = 1;
    minfo.pid = gpid(app_id, partition_index);
    rpc_address source_node(1, 10086);
    minfo.source_node = source_node;
    std::string disk_tag = "disk1";
    minfo.source_disk_tag = disk_tag;
    rpc_address target_node(2, 10086);
    minfo.target_node = target_node;
    minfo.type = balance_type::MOVE_PRIMARY;

    node_mapper nodes;
    app_mapper apps;
    meta_view view;
    view.apps = &apps;
    view.nodes = &nodes;

    cluster_balance_policy policy(nullptr);
    policy._global_view = &view;
    cluster_balance_policy::cluster_migration_info cluster_info;
    cluster_info.type = balance_type::COPY_SECONDARY;
    partition_set selected_pids;
    migration_list list;
    policy._migration_result = &list;

    // target_node is not found in cluster_info.replicas_count
    auto res = policy.apply_move(minfo, selected_pids, list, cluster_info);
    ASSERT_FALSE(res);

    // source_node is not found in cluster_info.replicas_count
    cluster_info.apps_skew[app_id] = 1;
    res = policy.apply_move(minfo, selected_pids, list, cluster_info);
    ASSERT_FALSE(res);

    // target_node is not found in cluster_info.replicas_count
    cluster_info.replicas_count[source_node] = 1;
    res = policy.apply_move(minfo, selected_pids, list, cluster_info);
    ASSERT_FALSE(res);

    // app_id is not found in cluster_info.app_skew
    cluster_info.replicas_count[target_node] = 1;
    res = policy.apply_move(minfo, selected_pids, list, cluster_info);
    ASSERT_FALSE(res);

    // source_node and target_node are not found in app_info
    cluster_balance_policy::app_migration_info app_info;
    cluster_info.apps_info[app_id] = app_info;
    res = policy.apply_move(minfo, selected_pids, list, cluster_info);
    ASSERT_FALSE(res);

    // app_info.partitions.size() < partition_index
    app_info.replicas_count[target_node] = 1;
    app_info.replicas_count[source_node] = 1;
    cluster_info.apps_info[app_id] = app_info;
    res = policy.apply_move(minfo, selected_pids, list, cluster_info);
    ASSERT_FALSE(res);

    // all of the partition status are not PS_SECONDARY
    std::map<rpc_address, partition_status::type> partition_status;
    partition_status[source_node] = partition_status::type::PS_PRIMARY;
    cluster_info.apps_info[app_id].partitions.push_back(partition_status);
    cluster_info.apps_info[app_id].partitions.push_back(partition_status);
    res = policy.apply_move(minfo, selected_pids, list, cluster_info);
    ASSERT_FALSE(res);

    // target_node and source_node are not found in cluster_info.nodes_info
    partition_status[source_node] = partition_status::type::PS_SECONDARY;
    cluster_info.apps_info[app_id].partitions.clear();
    cluster_info.apps_info[app_id].partitions.push_back(partition_status);
    cluster_info.apps_info[app_id].partitions.push_back(partition_status);
    res = policy.apply_move(minfo, selected_pids, list, cluster_info);
    ASSERT_FALSE(res);

    // disk_tag is not found in node_info
    cluster_balance_policy::node_migration_info target_info;
    cluster_balance_policy::node_migration_info source_info;
    cluster_info.nodes_info[target_node] = target_info;
    cluster_info.nodes_info[source_node] = source_info;
    res = policy.apply_move(minfo, selected_pids, list, cluster_info);
    ASSERT_FALSE(res);

    fail::setup();
    fail::cfg("generate_balancer_request", "return()");
    partition_set source_partition_set;
    cluster_info.nodes_info[source_node].partitions[disk_tag] = source_partition_set;
    res = policy.apply_move(minfo, selected_pids, list, cluster_info);
    fail::teardown();
    ASSERT_TRUE(res);
}

TEST(cluster_balance_policy, pick_up_partition)
{
    cluster_balance_policy::cluster_migration_info cluster_info;
    rpc_address addr(1, 10086);
    int32_t app_id = 1;
    std::map<rpc_address, partition_status::type> partition;
    partition[addr] = partition_status::PS_SECONDARY;
    cluster_balance_policy::app_migration_info app_info;
    app_info.partitions.push_back(partition);
    cluster_info.apps_info[app_id] = app_info;

    cluster_balance_policy policy(nullptr);
    {
        // all of the partitions in max_load_partitions are not found in cluster_info
        partition_set max_load_partitions;
        int32_t not_exist_app_id = 2;
        max_load_partitions.insert(gpid(not_exist_app_id, 10086));

        partition_set selected_pid;
        gpid picked_pid;
        auto found = policy.pick_up_partition(
            cluster_info, addr, max_load_partitions, selected_pid, picked_pid);
        ASSERT_FALSE(found);
    }

    {
        // all of the partitions in max_load_partitions are found in selected_pid
        partition_set max_load_partitions;
        max_load_partitions.insert(gpid(app_id, 10086));
        partition_set selected_pid;
        selected_pid.insert(gpid(app_id, 10086));

        gpid picked_pid;
        auto found = policy.pick_up_partition(
            cluster_info, addr, max_load_partitions, selected_pid, picked_pid);
        ASSERT_FALSE(found);
    }

    {
        // partition has already been primary or secondary on min_node
        partition_set max_load_partitions;
        max_load_partitions.insert(gpid(app_id, 0));
        partition_set selected_pid;

        gpid picked_pid;
        auto found = policy.pick_up_partition(
            cluster_info, addr, max_load_partitions, selected_pid, picked_pid);
        ASSERT_FALSE(found);
    }

    {
        partition_set max_load_partitions;
        gpid pid(app_id, 0);
        max_load_partitions.insert(pid);
        partition_set selected_pid;
        rpc_address not_exist_addr(3, 12345);

        gpid picked_pid;
        auto found = policy.pick_up_partition(
            cluster_info, not_exist_addr, max_load_partitions, selected_pid, picked_pid);
        ASSERT_TRUE(found);
        ASSERT_EQ(pid, picked_pid);
    }
}

bool balance_func(const std::shared_ptr<app_state> &app, bool only_move_primary)
{
    return only_move_primary;
}

TEST(cluster_balance_policy, execute_balance)
{
    int32_t app_id = 1;
    std::string app_name = "test";
    app_info info;
    info.app_id = app_id;
    info.app_name = app_name;
    info.partition_count = 1;
    info.status = app_status::AS_AVAILABLE;
    info.is_bulk_loading = false;
    auto app = std::make_shared<app_state>(info);
    app->helpers->split_states.splitting_count = 0;
    app_mapper apps;
    apps[app_id] = app;
    cluster_balance_policy policy(nullptr);

    app->status = app_status::AS_DROPPED;
    auto res = policy.execute_balance(apps, false, false, true, balance_func);
    app->status = app_status::AS_AVAILABLE;
    ASSERT_EQ(res, true);

    app->is_bulk_loading = true;
    res = policy.execute_balance(apps, false, false, true, balance_func);
    app->is_bulk_loading = false;
    ASSERT_EQ(res, true);

    app->helpers->split_states.splitting_count = 1;
    res = policy.execute_balance(apps, false, false, true, balance_func);
    app->helpers->split_states.splitting_count = 0;
    ASSERT_EQ(res, true);

    res = policy.execute_balance(apps, false, true, false, balance_func);
    ASSERT_EQ(res, false);

    res = policy.execute_balance(apps, true, false, true, balance_func);
    ASSERT_EQ(res, true);

    migration_list migration_result;
    migration_result.emplace(gpid(1, 1), std::make_shared<configuration_balancer_request>());
    policy._migration_result = &migration_result;
    res = policy.execute_balance(apps, false, true, true, balance_func);
    ASSERT_EQ(res, false);
}

TEST(cluster_balance_policy, calc_potential_moving)
{
    auto addr1 = rpc_address(1, 1);
    auto addr2 = rpc_address(1, 2);
    auto addr3 = rpc_address(1, 3);

    int32_t app_id = 1;
    dsn::app_info info;
    info.app_id = app_id;
    info.partition_count = 4;
    std::shared_ptr<app_state> app = app_state::create(info);
    partition_configuration pc;
    pc.primary = addr1;
    pc.secondaries.push_back(addr2);
    pc.secondaries.push_back(addr3);
    app->partitions[0] = pc;
    app->partitions[1] = pc;

    app_mapper apps;
    apps[app_id] = app;

    node_mapper nodes;
    node_state ns1;
    ns1.put_partition(gpid(app_id, 0), true);
    ns1.put_partition(gpid(app_id, 1), true);
    nodes[addr1] = ns1;

    node_state ns2;
    ns2.put_partition(gpid(app_id, 0), false);
    ns2.put_partition(gpid(app_id, 1), false);
    nodes[addr2] = ns2;
    nodes[addr3] = ns2;

    struct meta_view view;
    view.nodes = &nodes;
    view.apps = &apps;
    cluster_balance_policy policy(nullptr);
    policy._global_view = &view;

    auto gpids = policy.calc_potential_moving(app, addr1, addr2);
    ASSERT_EQ(gpids.size(), 2);
    ASSERT_EQ(*gpids.begin(), gpid(app_id, 0));
    ASSERT_EQ(*gpids.rbegin(), gpid(app_id, 1));

    gpids = policy.calc_potential_moving(app, addr1, addr3);
    ASSERT_EQ(gpids.size(), 2);
    ASSERT_EQ(*gpids.begin(), gpid(app_id, 0));
    ASSERT_EQ(*gpids.rbegin(), gpid(app_id, 1));

    gpids = policy.calc_potential_moving(app, addr2, addr3);
    ASSERT_EQ(gpids.size(), 0);
}
} // namespace replication
} // namespace dsn
