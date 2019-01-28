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
    greedy_load_balancer(meta_service *svc);
    virtual ~greedy_load_balancer();
    bool balance(meta_view view, migration_list &list) override;
    bool check(meta_view view, migration_list &list) override;
    void report(const migration_list &list, bool balance_checker) override;
    void score(meta_view view, double &primary_stddev, double &total_stddev) override;

    void register_ctrl_commands() override;
    void unregister_ctrl_commands() override;

    std::string get_balance_operation_count(const std::vector<std::string> &args) override;

private:
    enum class balance_type
    {
        move_primary,
        copy_primary,
        copy_secondary
    };

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
    int t_total_partitions;
    int t_alive_nodes;
    int t_operation_counters[MAX_COUNT];

    // this is used to assign an integer id for every node
    // and these are generated from the above data, which are tempory too
    std::unordered_map<dsn::rpc_address, int> address_id;
    std::vector<dsn::rpc_address> address_vec;

    // disk_tag -> targets(primaries/partitions)_on_this_disk
    typedef std::map<std::string, int> disk_load;

    // options
    bool _balancer_in_turn;
    bool _only_primary_balancer;
    bool _only_move_primary;

    dsn_handle_t _ctrl_balancer_in_turn;
    dsn_handle_t _ctrl_only_primary_balancer;
    dsn_handle_t _ctrl_only_move_primary;
    dsn_handle_t _get_balance_operation_count;

    // perf counters
    perf_counter_wrapper _balance_operation_count;
    perf_counter_wrapper _recent_balance_move_primary_count;
    perf_counter_wrapper _recent_balance_copy_primary_count;
    perf_counter_wrapper _recent_balance_copy_secondary_count;

private:
    void number_nodes(const node_mapper &nodes);
    void shortest_path(std::vector<bool> &visit,
                       std::vector<int> &flow,
                       std::vector<int> &prev,
                       std::vector<std::vector<int>> &network);

    // balance decision generators. All these functions try to make balance decisions
    // and store them to t_migration_result.
    //
    // return true if some decision is made, which means that these generators either put some
    // actions to the migration_list or don't take any action if they think the state is balanced.
    //
    // when return false, it means generators refuse to make decision coz
    // they think they need more informations.
    bool move_primary_based_on_flow_per_app(const std::shared_ptr<app_state> &app,
                                            const std::vector<int> &prev,
                                            const std::vector<int> &flow);
    bool copy_primary_per_app(const std::shared_ptr<app_state> &app,
                              bool still_have_less_than_average,
                              int replicas_low);
    bool primary_balancer_per_app(const std::shared_ptr<app_state> &app);
    bool primary_balancer_globally();

    bool copy_secondary_per_app(const std::shared_ptr<app_state> &app);
    bool secondary_balancer_globally();

    void greedy_balancer(const bool balance_checker);

    bool all_replica_infos_collected(const node_state &ns);
    // using t_global_view to get disk_tag of node's pid
    const std::string &get_disk_tag(const dsn::rpc_address &node, const dsn::gpid &pid);

    // return false if can't get the replica_info for some replicas on this node
    bool calc_disk_load(app_id id,
                        const dsn::rpc_address &node,
                        bool only_primary,
                        /*out*/ disk_load &load);
    void
    dump_disk_load(app_id id, const rpc_address &node, bool only_primary, const disk_load &load);

    std::shared_ptr<configuration_balancer_request>
    generate_balancer_request(const partition_configuration &pc,
                              const balance_type &type,
                              const rpc_address &from,
                              const rpc_address &to);
};
}
}
