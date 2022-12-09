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

#include "server_load_balancer.h"

namespace dsn {
namespace replication {
class load_balance_policy;

class greedy_load_balancer : public server_load_balancer
{
public:
    explicit greedy_load_balancer(meta_service *svc);
    virtual ~greedy_load_balancer();
    bool balance(meta_view view, migration_list &list) override;
    bool check(meta_view view, migration_list &list) override;
    void report(const migration_list &list, bool balance_checker) override;
    void score(meta_view view, double &primary_stddev, double &total_stddev) override;

    void register_ctrl_commands() override;

    std::string get_balance_operation_count(const std::vector<std::string> &args) override;

private:
    enum operation_counters
    {
        MOVE_PRI_COUNT = 0,
        COPY_PRI_COUNT = 1,
        COPY_SEC_COUNT = 2,
        ALL_COUNT = 3,
        MAX_COUNT = 4
    };

    // these variables are temporarily assigned by interface "balance"
    const meta_view *t_global_view;
    migration_list *t_migration_result;
    int t_alive_nodes;
    int t_operation_counters[MAX_COUNT];

    std::unique_ptr<load_balance_policy> _app_balance_policy;
    std::unique_ptr<load_balance_policy> _cluster_balance_policy;

    std::unique_ptr<command_deregister> _get_balance_operation_count;

    // perf counters
    perf_counter_wrapper _balance_operation_count;
    perf_counter_wrapper _recent_balance_move_primary_count;
    perf_counter_wrapper _recent_balance_copy_primary_count;
    perf_counter_wrapper _recent_balance_copy_secondary_count;

private:
    void greedy_balancer(bool balance_checker);
    bool all_replica_infos_collected(const node_state &ns);
};

inline configuration_proposal_action
new_proposal_action(const rpc_address &target, const rpc_address &node, config_type::type type)
{
    configuration_proposal_action act;
    act.__set_target(target);
    act.__set_node(node);
    act.__set_type(type);
    return act;
}

} // namespace replication
} // namespace dsn
