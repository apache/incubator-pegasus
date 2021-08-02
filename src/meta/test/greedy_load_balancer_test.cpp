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

#include <gtest/gtest.h>
#include <dsn/utility/defer.h>
#include "meta/greedy_load_balancer.h"

namespace dsn {
namespace replication {

TEST(greedy_load_balancer, app_migration_info)
{
    {
        greedy_load_balancer::app_migration_info info1;
        info1.app_id = 1;
        greedy_load_balancer::app_migration_info info2;
        info2.app_id = 2;
        ASSERT_LT(info1, info2);
    }

    {
        greedy_load_balancer::app_migration_info info1;
        info1.app_id = 2;
        greedy_load_balancer::app_migration_info info2;
        info2.app_id = 2;
        ASSERT_EQ(info1, info2);
    }
}

TEST(greedy_load_balancer, node_migration_info)
{
    {
        greedy_load_balancer::node_migration_info info1;
        info1.address = rpc_address(1, 10086);
        greedy_load_balancer::node_migration_info info2;
        info2.address = rpc_address(2, 10086);
        ASSERT_LT(info1, info2);
    }

    {
        greedy_load_balancer::node_migration_info info1;
        info1.address = rpc_address(1, 10000);
        greedy_load_balancer::node_migration_info info2;
        info2.address = rpc_address(1, 10086);
        ASSERT_LT(info1, info2);
    }

    {
        greedy_load_balancer::node_migration_info info1;
        info1.address = rpc_address(1, 10086);
        greedy_load_balancer::node_migration_info info2;
        info2.address = rpc_address(1, 10086);
        ASSERT_EQ(info1, info2);
    }
}

TEST(greedy_load_balancer, get_skew)
{
    std::map<rpc_address, uint32_t> count_map = {
        {rpc_address(1, 10086), 1}, {rpc_address(2, 10086), 3}, {rpc_address(3, 10086), 5},
    };

    ASSERT_EQ(get_skew(count_map), count_map.rbegin()->second - count_map.begin()->second);
}

TEST(greedy_load_balancer, get_count)
{
    node_state ns;
    int apid = 1;
    ns.put_partition(gpid(apid, 0), true);
    ns.put_partition(gpid(apid, 1), false);
    ns.put_partition(gpid(apid, 2), false);
    ns.put_partition(gpid(apid, 3), false);

    ASSERT_EQ(get_count(ns, cluster_balance_type::Primary, apid), 1);
    ASSERT_EQ(get_count(ns, cluster_balance_type::Secondary, apid), 3);
}

TEST(greedy_load_balancer, get_app_migration_info)
{
    greedy_load_balancer balancer(nullptr);

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

    greedy_load_balancer::app_migration_info migration_info;
    {
        app->partitions[0].max_replica_count = 100;
        auto res = balancer.get_app_migration_info(
            app, nodes, cluster_balance_type::Primary, migration_info);
        ASSERT_FALSE(res);
    }

    {
        app->partitions[0].max_replica_count = 1;
        auto res = balancer.get_app_migration_info(
            app, nodes, cluster_balance_type::Primary, migration_info);
        ASSERT_TRUE(res);
        ASSERT_EQ(migration_info.app_id, appid);
        ASSERT_EQ(migration_info.app_name, appname);
        std::map<rpc_address, partition_status::type> pstatus_map;
        pstatus_map[address] = partition_status::type::PS_PRIMARY;
        ASSERT_EQ(migration_info.partitions[0], pstatus_map);
        ASSERT_EQ(migration_info.replicas_count[address], 1);
    }
}

TEST(greedy_load_balancer, get_node_migration_info)
{
    greedy_load_balancer balancer(nullptr);

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

    greedy_load_balancer::node_migration_info migration_info;
    balancer.get_node_migration_info(ns, all_apps, migration_info);

    ASSERT_EQ(migration_info.address, address);
    ASSERT_NE(migration_info.partitions.find(disk_tag), migration_info.partitions.end());
    ASSERT_EQ(migration_info.partitions.at(disk_tag).size(), 1);
    ASSERT_EQ(*migration_info.partitions.at(disk_tag).begin(), pid);
}
} // namespace replication
} // namespace dsn
