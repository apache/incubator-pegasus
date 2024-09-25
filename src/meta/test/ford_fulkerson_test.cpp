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
#include <stdint.h>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/gpid.h"
#include "dsn.layer2_types.h"
#include "gtest/gtest.h"
#include "meta/load_balance_policy.h"
#include "meta/meta_data.h"
#include "rpc/dns_resolver.h" // IWYU pragma: keep
#include "rpc/rpc_address.h"
#include "rpc/rpc_host_port.h"

namespace dsn {
namespace replication {
TEST(ford_fulkerson, build_failure)
{
    int32_t app_id = 1;
    dsn::app_info info;
    info.app_id = app_id;
    info.partition_count = 4;
    std::shared_ptr<app_state> app = app_state::create(info);

    node_mapper nodes;
    node_state ns;
    ns.put_partition(gpid(app_id, 0), true);
    nodes[host_port("localhost", 1)] = ns;
    nodes[host_port("localhost", 2)] = ns;
    nodes[host_port("localhost", 3)] = ns;

    std::unordered_map<dsn::host_port, int> host_port_id;
    auto ff = ford_fulkerson::builder(app, nodes, host_port_id).build();
    ASSERT_EQ(ff, nullptr);
}

TEST(ford_fulkerson, add_edge)
{
    int32_t app_id = 1;
    dsn::app_info info;
    info.app_id = app_id;
    info.partition_count = 4;
    std::shared_ptr<app_state> app = app_state::create(info);

    std::unordered_map<dsn::host_port, int> host_port_id;
    const auto &hp1 = host_port("localhost", 1);
    const auto &hp2 = host_port("localhost", 2);
    const auto &hp3 = host_port("localhost", 3);
    host_port_id[hp1] = 1;
    host_port_id[hp2] = 2;
    host_port_id[hp3] = 3;

    node_mapper nodes;
    node_state ns;
    nodes[hp1] = ns;
    nodes[hp2] = ns;
    nodes[hp3] = ns;

    auto ff = ford_fulkerson::builder(app, nodes, host_port_id).build();
    ff->add_edge(1, ns);
    ASSERT_EQ(ff->_network[1].back(), 1);

    ns.put_partition(gpid(app_id, 0), true);
    ns.put_partition(gpid(app_id, 1), true);
    ns.put_partition(gpid(app_id, 2), true);
    ff->add_edge(3, ns);
    ASSERT_EQ(ff->_network[0][3], 2);
}

TEST(ford_fulkerson, update_decree)
{
    const auto &hp1 = host_port("localhost", 1);
    const auto &hp2 = host_port("localhost", 2);
    const auto &hp3 = host_port("localhost", 3);

    int32_t app_id = 1;
    dsn::app_info info;
    info.app_id = app_id;
    info.partition_count = 1;
    std::shared_ptr<app_state> app = app_state::create(info);
    partition_configuration pc;
    SET_IPS_AND_HOST_PORTS_BY_DNS(pc, secondaries, hp2, hp3);
    app->pcs.push_back(pc);
    app->pcs.push_back(pc);

    node_mapper nodes;
    node_state ns;
    ns.put_partition(gpid(app_id, 0), true);
    ns.put_partition(gpid(app_id, 1), true);
    nodes[hp1] = ns;
    nodes[hp2] = ns;
    nodes[hp3] = ns;

    std::unordered_map<dsn::host_port, int> host_port_id;
    host_port_id[hp1] = 1;
    host_port_id[hp2] = 2;
    host_port_id[hp3] = 3;

    auto node_id = 1;
    auto ff = ford_fulkerson::builder(app, nodes, host_port_id).build();
    ff->update_decree(node_id, ns);
    ASSERT_EQ(ff->_network[1][2], 2);
    ASSERT_EQ(ff->_network[1][3], 2);
}

TEST(ford_fulkerson, find_shortest_path)
{
    auto hp1 = host_port("localhost", 1);
    auto hp2 = host_port("localhost", 2);
    auto hp3 = host_port("localhost", 3);

    int32_t app_id = 1;
    dsn::app_info info;
    info.app_id = app_id;
    info.partition_count = 2;
    std::shared_ptr<app_state> app = app_state::create(info);

    partition_configuration pc;
    SET_IP_AND_HOST_PORT_BY_DNS(pc, primary, hp1);
    SET_IPS_AND_HOST_PORTS_BY_DNS(pc, secondaries, hp2, hp3);
    app->pcs[0] = pc;
    app->pcs[1] = pc;

    node_mapper nodes;
    node_state ns1;
    ns1.put_partition(gpid(app_id, 0), true);
    ns1.put_partition(gpid(app_id, 1), true);
    nodes[hp1] = ns1;

    node_state ns2;
    ns2.put_partition(gpid(app_id, 0), false);
    ns2.put_partition(gpid(app_id, 1), false);
    nodes[hp2] = ns2;
    nodes[hp3] = ns2;

    std::unordered_map<dsn::host_port, int> host_port_id;
    host_port_id[hp1] = 1;
    host_port_id[hp2] = 2;
    host_port_id[hp3] = 3;

    /**
     * ford fulkerson graph:
     *             1      2      1
     * (source) 0 ---> 1 ---> 3 ---
     *               2 |           |
     *                 v           v
     *                 2 --------> 4 (sink)
     *                      1
     */
    auto ff = ford_fulkerson::builder(app, nodes, host_port_id).build();
    ASSERT_EQ(ff->_network[0][0], 0);
    ASSERT_EQ(ff->_network[0][1], 1);
    ASSERT_EQ(ff->_network[0][2], 0);
    ASSERT_EQ(ff->_network[0][3], 0);
    ASSERT_EQ(ff->_network[0][4], 0);

    ASSERT_EQ(ff->_network[1][0], 0);
    ASSERT_EQ(ff->_network[1][1], 0);
    ASSERT_EQ(ff->_network[1][2], 2);
    ASSERT_EQ(ff->_network[1][3], 2);
    ASSERT_EQ(ff->_network[1][4], 0);

    ASSERT_EQ(ff->_network[2][0], 0);
    ASSERT_EQ(ff->_network[2][1], 0);
    ASSERT_EQ(ff->_network[2][2], 0);
    ASSERT_EQ(ff->_network[2][3], 0);
    ASSERT_EQ(ff->_network[2][4], 1);

    ASSERT_EQ(ff->_network[3][0], 0);
    ASSERT_EQ(ff->_network[3][1], 0);
    ASSERT_EQ(ff->_network[3][2], 0);
    ASSERT_EQ(ff->_network[3][3], 0);
    ASSERT_EQ(ff->_network[3][4], 1);

    ASSERT_EQ(ff->_network[4][0], 0);
    ASSERT_EQ(ff->_network[4][1], 0);
    ASSERT_EQ(ff->_network[4][2], 0);
    ASSERT_EQ(ff->_network[4][3], 0);
    ASSERT_EQ(ff->_network[4][4], 0);

    /**
     * shortest path:
     *         1      1      1
     *      0 ---> 1 ---> 2 ---> 4
     *  (source)               (sink)
     */
    auto flow_path = ff->find_shortest_path();
    ASSERT_EQ(flow_path->_prev[4], 2);
    ASSERT_EQ(flow_path->_flow[4], 1);
    ASSERT_EQ(flow_path->_prev[2], 1);
    ASSERT_EQ(flow_path->_flow[2], 1);
    ASSERT_EQ(flow_path->_prev[1], 0);
    ASSERT_EQ(flow_path->_flow[1], 1);
}

TEST(ford_fulkerson, max_value_pos)
{
    int32_t app_id = 1;
    dsn::app_info info;
    info.app_id = app_id;
    info.partition_count = 4;
    std::shared_ptr<app_state> app = app_state::create(info);

    std::unordered_map<dsn::host_port, int> host_port_id;
    auto hp1 = host_port("localhost", 1);
    auto hp2 = host_port("localhost", 2);
    auto hp3 = host_port("localhost", 3);
    host_port_id[hp1] = 1;
    host_port_id[hp2] = 2;
    host_port_id[hp3] = 3;

    node_mapper nodes;
    node_state ns;
    nodes[hp1] = ns;
    nodes[hp2] = ns;
    nodes[hp3] = ns;
    auto ff = ford_fulkerson::builder(app, nodes, host_port_id).build();

    std::vector<bool> visit(5, false);
    std::vector<int> flow(5, 0);
    auto pos = ff->max_value_pos(visit, flow);
    ASSERT_EQ(pos, -1);

    flow[1] = 3;
    flow[2] = 5;
    pos = ff->max_value_pos(visit, flow);
    ASSERT_EQ(pos, 2);

    visit[2] = true;
    pos = ff->max_value_pos(visit, flow);
    ASSERT_EQ(pos, 1);
}

TEST(ford_fulkerson, select_node)
{
    int32_t app_id = 1;
    dsn::app_info info;
    info.app_id = app_id;
    info.partition_count = 4;
    std::shared_ptr<app_state> app = app_state::create(info);

    std::unordered_map<dsn::host_port, int> host_port_id;
    auto hp1 = host_port("localhost", 1);
    auto hp2 = host_port("localhost", 2);
    auto hp3 = host_port("localhost", 3);
    host_port_id[hp1] = 1;
    host_port_id[hp2] = 2;
    host_port_id[hp3] = 3;

    node_mapper nodes;
    node_state ns;
    nodes[hp1] = ns;
    nodes[hp2] = ns;
    nodes[hp3] = ns;
    auto ff = ford_fulkerson::builder(app, nodes, host_port_id).build();

    std::vector<bool> visit(5, false);
    std::vector<int> flow(5, 0);
    auto pos = ff->select_node(visit, flow);
    ASSERT_EQ(pos, -1);

    flow[1] = 3;
    flow[2] = 5;
    pos = ff->select_node(visit, flow);
    ASSERT_EQ(pos, 2);
    ASSERT_EQ(visit[pos], true);

    visit[2] = true;
    pos = ff->select_node(visit, flow);
    ASSERT_EQ(pos, 1);
    ASSERT_EQ(visit[pos], true);
}
} // namespace replication
} // namespace dsn
