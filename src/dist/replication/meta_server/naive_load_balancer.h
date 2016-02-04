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
 *     A naive load balancer
 *
 * Revision history:
 *     2016-02-03, Weijie Sun, first version
 */

# pragma once

# include "server_load_balancer.h"
# include <functional>

using namespace dsn;
using namespace dsn::service;
using namespace dsn::replication;

class naive_load_balancer
    : public dsn::dist::server_load_balancer,
      public serverlet<naive_load_balancer>
{
public:
    naive_load_balancer(server_state* state);
    ~naive_load_balancer();

    virtual void run() override;
    virtual void run(global_partition_id gpid) override;

    // this method is for testing
    virtual void explictly_send_proposal(global_partition_id gpid,
                                         rpc_address receiver, config_type type,
                                         rpc_address node) override;
private:
    void query_decree(std::shared_ptr<query_replica_decree_request> query);
    void on_query_decree_ack(error_code err,
                             const std::shared_ptr<query_replica_decree_request>& query,
                             const std::shared_ptr<query_replica_decree_response>& resp);
    bool run_lb(partition_configuration& pc);

    void naive_balancer(int total_replicas);
    void ideal_primary_balancer(int total_replicas);
    void load_balancer_decision(const std::vector<dsn::rpc_address>& node_list,
                                const std::unordered_map<dsn::rpc_address, int>& node_id,
                                std::vector< std::vector<int> >& original_network,
                                const std::vector< std::vector<int> >& residual_network
                                );

    void reassign_primary(global_partition_id gpid);
    void add_new_secondary(global_partition_id gpid, dsn::rpc_address target);
    void reset_balance_pq();

    dsn::rpc_address random_find_machine(dsn::rpc_address excluded);
    int primaries_now(const dsn::rpc_address& addr) const
    {
        auto iter = _state->_nodes.find(addr);
        if (iter != _state->_nodes.end())
            return iter->second.primaries.size();
        return 0;
    }

    int replicas_now(const dsn::rpc_address& addr) const
    {
        auto iter = _state->_nodes.find(addr);
        if (iter != _state->_nodes.end())
            return iter->second.partitions.size();
        return 0;
    }

    int primaries_expected(const dsn::rpc_address& addr) const
    {
        int result = primaries_now(addr);
        auto iter = _future_primary.find(addr);
        if ( iter != _future_primary.end() )
            return result + iter->second;
        return result;
    }
private:
    server_state *_state;

    typedef std::function<bool (const dsn::rpc_address&, const dsn::rpc_address&)> load_comparator;
    std::set<dsn::rpc_address, load_comparator> _pq_now_primary;
    std::set<dsn::rpc_address, load_comparator> _pq_expect_primary;

    std::unordered_map<dsn::rpc_address, int> _future_primary;
    std::unordered_map<global_partition_id, dsn::rpc_address> _primary_recommender;
};
