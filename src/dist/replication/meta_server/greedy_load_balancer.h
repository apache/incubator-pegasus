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
 *     A greedy load balancer
 *
 * Revision history:
 *     2016-02-03, Weijie Sun, first version
 */

# pragma once

# include "server_load_balancer.h"
# include <functional>
# include <atomic>

using namespace dsn;
using namespace dsn::service;
using namespace dsn::replication;

class greedy_load_balancer
    : public dsn::dist::server_load_balancer,
      public serverlet<greedy_load_balancer>
{
public:
    greedy_load_balancer(server_state* state);
    ~greedy_load_balancer();

    virtual void run() override;
    virtual void run(gpid gpid) override;
    virtual void on_config_changed(std::shared_ptr<configuration_update_request>& request) override;
    virtual void on_balancer_proposal(/*in*/const balancer_proposal_request& request, /*out*/balancer_proposal_response& response) override;
    virtual void on_control_migration(/*in*/const control_balancer_migration_request& request,
                                      /*out*/control_balancer_migration_response& response) override;
private:
    bool run_lb(app_info& info, partition_configuration& pc, bool is_stateful);

    bool balancer_proposal_check(const balancer_proposal_request& balancer_proposal);

    void execute_balancer_proposal();
    void greedy_copy_secondary();
    void greedy_copy_primary(int total_replicas);
    void greedy_move_primary(const std::vector<dsn::rpc_address>& node_list,
                             const std::vector<int>& prev,
                             int flows);
    void greedy_balancer(int total_replicas);

    bool walk_through_primary(const dsn::rpc_address& addr, const std::function<bool (partition_configuration& pc)>& func);
    bool walk_through_partitions(const dsn::rpc_address& addr, const std::function<bool (partition_configuration& pc)>& func);

    dsn::rpc_address recommend_primary(partition_configuration& pc);
    dsn::rpc_address find_minimal_load_machine(bool primaryOnly);

    void insert_balancer_proposal_request(const gpid& gpid, balancer_type::type type, dsn::rpc_address from, dsn::rpc_address to)
    {
        balancer_proposal_request& request = _balancer_proposals_map[gpid];
        request.pid = gpid;
        request.type = type;
        request.from_addr= from;
        request.to_addr = to;
    }
private:
    volatile bool _is_greedy_rebalancer_enabled;

    std::unordered_map<gpid, dsn::rpc_address> _primary_recommender;
    std::unordered_map<gpid, balancer_proposal_request> _balancer_proposals_map;
};
