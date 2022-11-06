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

#include <algorithm>

#include <gtest/gtest.h>

#include "meta/greedy_load_balancer.h"
#include "meta/meta_data.h"
#include "meta/server_load_balancer.h"
#include "meta/test/misc/misc.h"
#include "utils/fmt_logging.h"

using namespace dsn::replication;

class simple_priority_queue
{
public:
    simple_priority_queue(const std::vector<dsn::rpc_address> &nl,
                          server_load_balancer::node_comparator &&compare)
        : container(nl), cmp(std::move(compare))
    {
        std::make_heap(container.begin(), container.end(), cmp);
    }
    void push(const dsn::rpc_address &addr)
    {
        container.push_back(addr);
        std::push_heap(container.begin(), container.end(), cmp);
    }
    dsn::rpc_address pop()
    {
        std::pop_heap(container.begin(), container.end(), cmp);
        dsn::rpc_address result = container.back();
        container.pop_back();
        return result;
    }
    dsn::rpc_address top() const { return container.front(); }
    bool empty() const { return container.empty(); }
private:
    std::vector<dsn::rpc_address> container;
    server_load_balancer::node_comparator cmp;
};

void generate_balanced_apps(/*out*/ app_mapper &apps,
                            node_mapper &nodes,
                            const std::vector<dsn::rpc_address> &node_list)
{
    nodes.clear();
    for (const auto &node : node_list)
        nodes[node].set_alive(true);

    int partitions_per_node = random32(20, 100);
    dsn::app_info info;
    info.status = dsn::app_status::AS_AVAILABLE;
    info.is_stateful = true;
    info.app_id = 1;
    info.app_name = "test";
    info.app_type = "test";
    info.partition_count = partitions_per_node * node_list.size();
    info.max_replica_count = 3;

    std::shared_ptr<app_state> the_app = app_state::create(info);

    simple_priority_queue pq1(node_list, server_load_balancer::primary_comparator(nodes));
    // generate balanced primary
    for (dsn::partition_configuration &pc : the_app->partitions) {
        dsn::rpc_address n = pq1.pop();
        nodes[n].put_partition(pc.pid, true);
        pc.primary = n;
        pq1.push(n);
    }

    // generate balanced secondary
    simple_priority_queue pq2(node_list, server_load_balancer::partition_comparator(nodes));
    std::vector<dsn::rpc_address> temp;

    for (dsn::partition_configuration &pc : the_app->partitions) {
        temp.clear();
        while (pc.secondaries.size() + 1 < pc.max_replica_count) {
            dsn::rpc_address n = pq2.pop();
            if (!is_member(pc, n)) {
                pc.secondaries.push_back(n);
                nodes[n].put_partition(pc.pid, false);
            }
            temp.push_back(n);
        }
        for (auto n : temp)
            pq2.push(n);
    }

    // check if balanced
    int pri_min, part_min;
    pri_min = part_min = the_app->partition_count + 1;
    int pri_max, part_max;
    pri_max = part_max = -1;

    for (auto &kv : nodes) {
        if (kv.second.primary_count() > pri_max)
            pri_max = kv.second.primary_count();
        if (kv.second.primary_count() < pri_min)
            pri_min = kv.second.primary_count();
        if (kv.second.partition_count() > part_max)
            part_max = kv.second.partition_count();
        if (kv.second.partition_count() < part_min)
            part_min = kv.second.partition_count();
    }

    apps.emplace(the_app->app_id, the_app);

    CHECK_LE(pri_max - pri_min, 1);
    CHECK_LE(part_max - part_min, 1);
}

void random_move_primary(app_mapper &apps, node_mapper &nodes, int primary_move_ratio)
{
    app_state &the_app = *(apps[0]);
    int space_size = the_app.partition_count * 100;
    for (dsn::partition_configuration &pc : the_app.partitions) {
        int n = random32(1, space_size) / 100;
        if (n < primary_move_ratio) {
            int indice = random32(0, 1);
            nodes[pc.primary].remove_partition(pc.pid, true);
            std::swap(pc.primary, pc.secondaries[indice]);
            nodes[pc.primary].put_partition(pc.pid, true);
        }
    }
}

void greedy_balancer_perfect_move_primary()
{
    app_mapper apps;
    node_mapper nodes;
    std::vector<dsn::rpc_address> node_list;

    generate_node_list(node_list, 20, 100);
    generate_balanced_apps(apps, nodes, node_list);

    random_move_primary(apps, nodes, 70);
    // test the greedy balancer's move primary
    greedy_load_balancer glb(nullptr);
    migration_list ml;

    glb.check({&apps, &nodes}, ml);
    LOG_DEBUG("balance checker operation count = %d", ml.size());
    int i = 0;

    while (glb.balance({&apps, &nodes}, ml)) {
        for (const auto &kv : ml) {
            const std::shared_ptr<configuration_balancer_request> &req = kv.second;
            for (const configuration_proposal_action &act : req->action_list) {
                CHECK_NE(act.type, config_type::CT_ADD_SECONDARY_FOR_LB);
            }
        }
        glb.check({&apps, &nodes}, ml);
        LOG_DEBUG("round %d: balance checker operation count = %d", ++i, ml.size());
    }
}

int main(int, char **)
{
    dsn_run_config("config.ini", false);
    greedy_balancer_perfect_move_primary();
    return 0;
}
