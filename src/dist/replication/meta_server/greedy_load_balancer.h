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

/*
 * Description:
 *     A greedy load balancer based on Dijkstra & Ford-Fulkerson
 *
 * Revision history:
 *     2016-02-03, Weijie Sun, first version
 */

#pragma once

#include <functional>
#include "server_load_balancer.h"

namespace dsn {
namespace replication {

class greedy_load_balancer : public simple_load_balancer
{
public:
    greedy_load_balancer(meta_service *svc) : simple_load_balancer(svc) {}
    bool balance(meta_view view, migration_list &list) override;

private:
    enum class balance_type
    {
        move_primary,
        copy_primary,
        copy_secondary
    };

    // these variables are temporarily assigned by interface "balance"
    const meta_view *t_global_view;
    migration_list *t_migration_result;
    int t_total_partitions;
    int t_alive_nodes;

    // this is used to assign an integer id for every node
    // and these are generated from the above data, which are tempory too
    std::unordered_map<dsn::rpc_address, int> address_id;
    std::vector<dsn::rpc_address> address_vec;

private:
    void number_nodes(const node_mapper &nodes);

    void primary_balancer_per_app(const std::shared_ptr<app_state> &app);
    void copy_primary_per_app(const std::shared_ptr<app_state> &app,
                              bool still_have_less_than_average,
                              int replicas_low);
    void primary_balancer_globally();

    void copy_secondary_per_app(const std::shared_ptr<app_state> &app);
    void secondary_balancer_globally();

    void greedy_balancer();
    void shortest_path(std::vector<bool> &visit,
                       std::vector<int> &flow,
                       std::vector<int> &prev,
                       std::vector<std::vector<int>> &network);
    void make_balancer_decision_based_on_flow(const std::shared_ptr<app_state> &app,
                                              const std::vector<int> &prev,
                                              const std::vector<int> &flow);

    std::shared_ptr<configuration_balancer_request>
    generate_balancer_request(const partition_configuration &pc,
                              const balance_type &type,
                              const rpc_address &from,
                              const rpc_address &to);
};
}
}
