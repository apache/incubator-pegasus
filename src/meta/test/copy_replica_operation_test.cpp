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

#include <stdint.h>
#include <iterator>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/gpid.h"
#include "dsn.layer2_types.h"
#include "gtest/gtest.h"
#include "meta/app_balance_policy.h"
#include "meta/load_balance_policy.h"
#include "meta/meta_data.h"
#include "rpc/rpc_host_port.h"
#include "utils/fail_point.h"

namespace dsn {
namespace replication {

TEST(copy_primary_operation, misc)
{
    int32_t app_id = 1;
    dsn::app_info info;
    info.app_id = app_id;
    info.partition_count = 4;
    std::shared_ptr<app_state> app = app_state::create(info);
    app_mapper apps;
    apps[app_id] = app;

    const auto &hp1 = host_port("localhost", 1);
    const auto &hp2 = host_port("localhost", 2);
    const auto &hp3 = host_port("localhost", 3);

    node_mapper nodes;
    node_state ns1;
    ns1.put_partition(gpid(app_id, 2), true);
    ns1.put_partition(gpid(app_id, 0), false);
    nodes[hp1] = ns1;
    node_state ns2;
    ns2.put_partition(gpid(app_id, 0), true);
    ns2.put_partition(gpid(app_id, 1), true);
    nodes[hp2] = ns2;
    node_state ns3;
    ns3.put_partition(gpid(app_id, 2), false);
    nodes[hp3] = ns3;

    std::vector<dsn::host_port> host_port_vec{hp1, hp2, hp3};
    std::unordered_map<dsn::host_port, int> host_port_id;
    host_port_id[hp1] = 0;
    host_port_id[hp2] = 1;
    host_port_id[hp3] = 2;
    copy_primary_operation op(app, apps, nodes, host_port_vec, host_port_id, false, 0);

    /**
     * Test init_ordered_host_port_ids
     */
    op.init_ordered_host_port_ids();
    ASSERT_EQ(op._ordered_host_port_ids.size(), 3);
    ASSERT_EQ(*op._ordered_host_port_ids.begin(), 2);
    ASSERT_EQ(*(++op._ordered_host_port_ids.begin()), 0);
    ASSERT_EQ(*op._ordered_host_port_ids.rbegin(), 1);
    ASSERT_EQ(op._partition_counts[0], 1);
    ASSERT_EQ(op._partition_counts[1], 2);
    ASSERT_EQ(op._partition_counts[2], 0);

    /**
     * Test get_all_partitions
     */
    auto partitions = op.get_all_partitions();
    ASSERT_EQ(partitions->size(), 2);
    ASSERT_EQ(*partitions->begin(), gpid(app_id, 0));
    ASSERT_EQ(*partitions->rbegin(), gpid(app_id, 1));

    /**
     * Test select_partition
     */
    std::string disk1 = "disk1", disk2 = "disk2";
    disk_load load;
    load[disk1] = 2;
    load[disk2] = 6;
    op._node_loads[hp2] = load;

    serving_replica serving_partition0;
    serving_partition0.node = hp2;
    serving_partition0.disk_tag = disk1;
    app->helpers->contexts[0].serving.push_back(serving_partition0);
    serving_replica serving_partition1;
    serving_partition1.node = hp2;
    serving_partition1.disk_tag = disk2;
    app->helpers->contexts[1].serving.push_back(serving_partition1);

    migration_list list;
    auto res_gpid = op.select_partition(&list);
    ASSERT_EQ(res_gpid.get_partition_index(), 1);

    /**
     * Test can_continue
     **/
    op._have_lower_than_average = true;
    ASSERT_FALSE(op.can_continue());

    op._have_lower_than_average = false;
    ASSERT_TRUE(op.can_continue());
    op._have_lower_than_average = true;

    op._replicas_low = 1;
    ASSERT_TRUE(op.can_continue());
    op._replicas_low = 0;

    nodes[hp2].remove_partition(gpid(app_id, 1), false);
    op.init_ordered_host_port_ids();
    ASSERT_FALSE(op.can_continue());
    nodes[hp2].put_partition(gpid(app_id, 1), true);

    /**
     * Test update_ordered_host_port_ids
     */
    nodes[hp1].put_partition(gpid(app_id, 3), true);
    nodes[hp2].put_partition(gpid(app_id, 4), true);
    nodes[hp2].put_partition(gpid(app_id, 5), true);
    op.init_ordered_host_port_ids();
    op.update_ordered_host_port_ids();
    ASSERT_EQ(op._ordered_host_port_ids.size(), 3);
    ASSERT_EQ(*op._ordered_host_port_ids.begin(), 2);
    ASSERT_EQ(*(++op._ordered_host_port_ids.begin()), 0);
    ASSERT_EQ(*op._ordered_host_port_ids.rbegin(), 1);
    ASSERT_EQ(op._partition_counts[0], 2);
    ASSERT_EQ(op._partition_counts[1], 3);
    ASSERT_EQ(op._partition_counts[2], 1);

    /**
     * Test copy_once
     */
    fail::setup();
    fail::cfg("generate_balancer_request", "return()");
    gpid gpid1(1, 0);
    gpid gpid2(1, 1);
    list.clear();
    op.copy_once(gpid1, &list);
    ASSERT_EQ(list.size(), 1);
    ASSERT_EQ(list.count(gpid1), 1);
    ASSERT_EQ(list.count(gpid2), 0);
    fail::teardown();
}

TEST(copy_primary_operation, can_select)
{
    app_mapper apps;
    node_mapper nodes;
    std::vector<dsn::host_port> host_port_vec;
    std::unordered_map<dsn::host_port, int> host_port_id;
    copy_primary_operation op(nullptr, apps, nodes, host_port_vec, host_port_id, false, false);

    gpid cannot_select_gpid(1, 1);
    gpid can_select_gpid(1, 2);
    migration_list list;
    list[cannot_select_gpid] = nullptr;

    ASSERT_FALSE(op.can_select(cannot_select_gpid, &list));
    ASSERT_TRUE(op.can_select(can_select_gpid, &list));
}

TEST(copy_primary_operation, only_copy_primary)
{
    app_mapper apps;
    node_mapper nodes;
    std::vector<dsn::host_port> host_port_vec;
    std::unordered_map<dsn::host_port, int> host_port_id;
    copy_primary_operation op(nullptr, apps, nodes, host_port_vec, host_port_id, false, false);

    ASSERT_TRUE(op.only_copy_primary());
}

TEST(copy_secondary_operation, misc)
{
    int32_t app_id = 1;
    dsn::app_info info;
    info.app_id = app_id;
    info.partition_count = 4;
    std::shared_ptr<app_state> app = app_state::create(info);
    app_mapper apps;
    apps[app_id] = app;

    const auto &hp1 = host_port("localhost", 1);
    const auto &hp2 = host_port("localhost", 2);
    const auto &hp3 = host_port("localhost", 3);

    node_mapper nodes;
    node_state ns1;
    ns1.put_partition(gpid(app_id, 2), true);
    ns1.put_partition(gpid(app_id, 0), false);
    nodes[hp1] = ns1;
    node_state ns2;
    ns2.put_partition(gpid(app_id, 0), true);
    ns2.put_partition(gpid(app_id, 1), true);
    nodes[hp2] = ns2;
    node_state ns3;
    nodes[hp3] = ns3;

    std::vector<dsn::host_port> host_port_vec{hp1, hp2, hp3};
    std::unordered_map<dsn::host_port, int> host_port_id;
    host_port_id[hp1] = 0;
    host_port_id[hp2] = 1;
    host_port_id[hp3] = 2;
    copy_secondary_operation op(app, apps, nodes, host_port_vec, host_port_id, 0);
    op.init_ordered_host_port_ids();

    /**
     * Test copy_secondary_operation::get_partition_count
     */
    ASSERT_EQ(op.get_partition_count(ns1), 2);
    ASSERT_EQ(op.get_partition_count(ns2), 2);
    ASSERT_EQ(op.get_partition_count(ns3), 0);

    /**
     * Test copy_secondary_operation::can_continue
     */
    auto res = op.can_continue();
    ASSERT_TRUE(res);

    op._replicas_low = 100;
    res = op.can_continue();
    ASSERT_FALSE(res);
    op._replicas_low = 0;

    nodes[hp3].put_partition(gpid(app_id, 2), false);
    op.init_ordered_host_port_ids();
    res = op.can_continue();
    ASSERT_FALSE(res);
    nodes[hp3].remove_partition(gpid(app_id, 2), false);

    /**
     * Test copy_secondary_operation::can_select
     */
    nodes[hp1].put_partition(gpid(app_id, 3), true);
    op.init_ordered_host_port_ids();
    migration_list list;
    res = op.can_select(gpid(app_id, 3), &list);
    ASSERT_FALSE(res);

    auto secondary_gpid = gpid(app_id, 0);
    list[secondary_gpid] = nullptr;
    res = op.can_select(secondary_gpid, &list);
    ASSERT_FALSE(res);
    list.clear();

    nodes[hp3].put_partition(secondary_gpid, true);
    op.init_ordered_host_port_ids();
    res = op.can_select(secondary_gpid, &list);
    ASSERT_FALSE(res);

    nodes[hp3].remove_partition(secondary_gpid, false);
    op.init_ordered_host_port_ids();
    res = op.can_select(secondary_gpid, &list);
    ASSERT_TRUE(res);

    /**
     * Test copy_secondary_operation::get_balance_type
     */
    ASSERT_EQ(op.get_balance_type(), balance_type::COPY_SECONDARY);

    /**
     * Test copy_secondary_operation::only_copy_primary
     */
    ASSERT_FALSE(op.only_copy_primary());
}
} // namespace replication
} // namespace dsn
