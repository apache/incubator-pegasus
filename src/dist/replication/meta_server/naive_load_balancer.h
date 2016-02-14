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

enum migration_type
{
    mt_move_primary, //downgrade current primary to secondary, and update another secondary to primary
    mt_copy_primary, //add a new learner and make it primary
    mt_copy_secondary //add a new learner and make it secondary
};

struct migration_proposal
{
    global_partition_id _gpid;
    migration_type _type;
    rpc_address _from;
    rpc_address _to;

    migration_proposal(){}
    migration_proposal(global_partition_id id, migration_type t, dsn::rpc_address from, dsn::rpc_address to):
        _type(t), _from(from), _to(to)
    {
        memcpy(&_gpid, &id, sizeof(id));
    }
};

class naive_load_balancer
    : public dsn::dist::server_load_balancer,
      public serverlet<naive_load_balancer>
{
public:
    naive_load_balancer(server_state* state);
    ~naive_load_balancer();

    virtual void run() override;
    virtual void run(global_partition_id gpid, std::shared_ptr<configuration_update_request> request) override;

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

    void greedy_copy_primary();
    void greedy_move_primary(const std::vector<dsn::rpc_address>& node_list,
                             const std::vector<int>& prev,
                             int flows);
    void greedy_balancer(int total_replicas);

    void walk_through_primary(const dsn::rpc_address& addr, const std::function<bool (partition_configuration& pc)>& func);
    void walk_through_partitions(const dsn::rpc_address& addr, const std::function<bool (partition_configuration& pc)>& func);

    void recommend_primary(partition_configuration& pc);
    dsn::rpc_address random_find_machine(dsn::rpc_address excluded);
private:
    server_state *_state;

    std::unordered_map<global_partition_id, dsn::rpc_address> _primary_recommender;
    std::unordered_map<global_partition_id, migration_proposal> _migration_proposals_map;
};
