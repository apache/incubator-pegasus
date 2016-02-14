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
 *     a naive load balancer implementation
 *
 * Revision history:
 *     2016-02-3, Weijie Sun(sunweijie@xiaomi.com), first version
 */

#include "naive_load_balancer.h"
#include <algorithm>

# ifdef __TITLE__
# undef __TITLE__
# endif
# define __TITLE__ "load.balancer.naive"

naive_load_balancer::naive_load_balancer(server_state* state):
    dsn::dist::server_load_balancer(state),
    serverlet<naive_load_balancer>("naive_load_balancer"),
    _state(state),
    _pq_now_primary([this](const dsn::rpc_address& addr1, const dsn::rpc_address& addr2)
    {
        int count1 = primaries_now(addr1), count2 = primaries_now(addr2);
        if (count1 != count2)
            return count1 < count2;
        return addr1 < addr2;
    }),
    _pq_expect_primary([this](const dsn::rpc_address& addr1, const dsn::rpc_address& addr2){
        int count1 = primaries_expected(addr1), count2 = primaries_expected(addr2);
        if (count1 != count2)
            return count1 < count2;
        return addr1 < addr2;
    })
{
}

naive_load_balancer::~naive_load_balancer()
{
}

void naive_load_balancer::reassign_primary(global_partition_id gpid)
{
    partition_configuration& pc = _state->_apps[gpid.app_id-1].partitions[gpid.pidx];
    configuration_update_request proposal;
    proposal.config = pc;
    proposal.type = CT_DOWNGRADE_TO_SECONDARY;
    proposal.node = pc.primary;

    send_proposal(pc.primary, proposal);
}

void naive_load_balancer::add_new_secondary(global_partition_id gpid, rpc_address target)
{
    partition_configuration& pc = _state->_apps[gpid.app_id-1].partitions[gpid.pidx];
    configuration_update_request proposal;
    proposal.config = pc;
    proposal.type = CT_ADD_SECONDARY;
    proposal.node = target;

    send_proposal(pc.primary, proposal);
}

void naive_load_balancer::naive_balancer(int total_replicas)
{
    dassert(_state->_node_live_count != 0, "no alive node");

    int replicas_low, replicas_high;
    replicas_low = replicas_high = total_replicas/_state->_node_live_count;
    if (total_replicas%_state->_node_live_count != 0)
        replicas_high++;

    std::map<dsn::rpc_address, int> new_servers;
    for (auto iter=_state->_nodes.begin(); iter!=_state->_nodes.end(); ++iter)
    {
        server_state::node_state& state = iter->second;
        if (state.is_alive && state.partitions.empty())
            new_servers.emplace(iter->first, 0);
    }

    if (new_servers.empty())
    {
        dsn::rpc_address addr = *(--_pq_now_primary.end());
        std::set<global_partition_id>& primary_set = _state->_nodes[addr].primaries;
        int primary_count = primary_set.size();
        if ( replicas_high <  primary_count )
        {
            for (auto iter=primary_set.begin(); iter!=primary_set.end(); ++iter)
            {
                int number = static_cast<int>(dsn_random32(0, primary_count-1));
                if (number >= replicas_high)
                {
                    reassign_primary(*iter);
                }
            }
        }
    }
    else
    {
        for (auto iter=new_servers.begin(); iter!=new_servers.end(); ++iter)
        {
            _pq_now_primary.erase(iter->first);
        }

        while (true)
        {
            dsn::rpc_address heaviest_server = *(--_pq_now_primary.end());

            server_state::node_state& state = _state->_nodes[ heaviest_server ];
            server_state::node_state& state2 = _state->_nodes[ *_pq_now_primary.begin() ];
            if ( state.primaries.empty() ||
                 state.primaries.size() <= replicas_high ||
                 new_servers.begin()->second >= state2.primaries.size() )
                break;

            add_new_secondary(*state.primaries.begin(), new_servers.begin()->first);
            new_servers.begin()->second++;
        }
    }
}

void naive_load_balancer::load_balancer_decision(const std::vector<dsn::rpc_address>& node_list,
                                                 const std::unordered_map<dsn::rpc_address, int>& node_id,
                                                 std::vector< std::vector<int> >& original_network,
                                                 const std::vector< std::vector<int> >& residual_network)
{
    int total_nodes = original_network.size() - 2;
    for (size_t i=1; i<=total_nodes; ++i)
        for (size_t j=1; j<=total_nodes; ++j)
            if ( original_network[i][j]>0 )
                original_network[i][j] -= residual_network[i][j];

    for (const auto& pair: _state->_nodes)
    {
        server_state::node_state& ns = pair.second;
        int from_id = node_id[ ns.address ];
        for (const global_partition_id& gpid: ns.primaries)
        {
            const partition_configuration& pc = _state->_apps[gpid.app_id - 1].partitions[gpid.pidx];
            for (const auto& addr: pc.secondaries)
            {
                int to_id = node_id[addr];
                if ( original_network[from_id][to_id] > 0 ) {
                    reassign_primary(gpid, addr);
                }
            }
        }
    }
}

/* utils function for walk through */
bool naive_load_balancer::walk_through_primary(const rpc_address &addr, const std::function<bool (partition_configuration &)> func)
{
    auto iter = _state->_nodes.find(addr);
    if (iter == _state->_nodes.end() )
        return false;
    server_state::node_state& ns = iter->second;
    for (const global_partition_id& gpid: ns.primaries)
    {
        partition_configuration& pc = _state->_apps[gpid.app_id - 1][gpid.pidx];
        if ( !func(pc) )
            return false;
    }
    return true;
}

/* utils function for walk through */
bool naive_load_balancer::walk_through_partitions(const rpc_address &addr, const std::function<bool (partition_configuration &)> func)
{
    auto iter = _state->_nodes.find(addr);
    if (iter == _state->_nodes.end() )
        return false;
    server_state::node_state& ns = iter->second;
    for (const global_partition_id& gpid: ns.partitions)
    {
        partition_configuration& pc = _state->_apps[gpid.app_id - 1][gpid.pidx];
        if ( !func(pc) )
            return false;
    }
    return true;
}

void naive_load_balancer::greedy_primary_balancer(int total_replicas)
{
    size_t graph_nodes = _state->_node_live_count + 2;
    std::vector< std::vector<int> > network(graph_nodes, std::vector<int>(graph_nodes, 0));
    std::unordered_map<dsn::rpc_address, int> node_id;
    std::vector<dsn::rpc_address> node_list(graph_nodes);

    // assign id for nodes
    int current_id = 1;
    for (auto iter=_state->_nodes.begin(); iter!=_state->_nodes.end(); ++iter)
    {
        if ( !iter->second.is_alive )
            continue;
        node_id[ iter->first ] = current_id;
        node_list[ current_id ] = iter->first;
        ++current_id;
    }

    int average_replicas = total_replicas/_state->_node_live_count;

    // make graph
    for (auto iter=_state->_nodes.begin(); iter!=_state->_nodes.end(); ++iter)
    {
        if ( !iter->second.is_alive )
            continue;
        int from = node_id[iter->first];
        walk_through_primary(iter->first, [this, &](partition_configuration& replica_in_primary){
            for (auto& target: replica_in_primary.secondaries)
            {
                auto i = node_id.find(target);
                if ( i != node_id.end() )
                    network[from][ i->second ]++;
            }
        } );

        // add flow for source
        int primaries_count = iter->second.primaries.size();
        if (primaries_count > average_replicas)
            network[0][from] = primaries_count - average_replicas;
        else
            network[from][graph_nodes - 1] = average_replicas - primaries_count;
    }

    std::vector<bool> visit(graph_nodes, false);
    std::vector<int> flow(graph_nodes, -1);
    std::vector<int> prev(graph_nodes);

    while ( !visit[graph_nodes - 1] )
    {
        int max_flow_pos = -1, max_flow_value = 0;
        for (int i=0; i!=graph_nodes; ++i)
        {
            if (visit[i]==false && flow[i]>max_flow_value)
            {
                max_flow_pos = i;
                max_flow_value = flow[i];
            }
        }
        max_iter -= graph_nodes;

        if (max_flow_pos == -1)
            break;

        visit[max_flow_pos] = true;
        for (int i=0; i!=graph_nodes; ++i)
        {
            if (!visit[i] && min(flow[max_flow_pos], residual_network[max_flow_pos][i])>flow[i])
            {
                flow[i] = min(flow[max_flow_pos], residual_network[max_flow_pos][i]);
                prev[i] = max_flow_pos;
            }
        }
        max_iter -= graph_nodes;
    }
}

void naive_load_balancer::reset_balance_pq()
{
    for (auto iter = _state->_nodes.begin(); iter!=_state->_nodes.end(); ++iter)
    {
        _pq_now_primary.insert(iter->first);
        _future_primary.emplace(iter->fist, 0);
        _pq_expect_primary.insert(iter->first);
    }
}

void naive_load_balancer::run()
{
    zauto_read_lock l(_state->_lock);

    bool is_system_healthy = true;
    int total_replicas = 0;

    for (size_t i = 0; i < _state->_apps.size(); i++)
    {
        app_state& app = _state->_apps[i];
        if (app.status != AS_AVAILABLE)
            continue;
        for (int j = 0; j < app.partition_count; j++)
        {
            partition_configuration& pc = app.partitions[j];
            is_system_healthy = is_system_healthy && run_lb(pc);
        }
        total_replicas += app.partition_count;
    }
    if ( is_system_healthy )
    {
        naive_balancer(total_replicas);
    }
}

void naive_load_balancer::run(global_partition_id gpid)
{
    zauto_read_lock l(_state->_lock);
    partition_configuration& pc = _state->_apps[gpid.app_id - 1].partitions[gpid.pidx];
    run_lb(pc);
}

dsn::rpc_address naive_load_balancer::random_find_machine(dsn::rpc_address excluded)
{
    std::vector<dsn::rpc_address> vec;
    vec.reserve(_state->_node_live_count);
    for (auto iter = _state->_nodes.begin(); iter!=_state->_nodes.end(); ++iter)
    {
        if (!iter->second.is_alive || iter->first == excluded )
            continue;
        vec.push_back(iter->first);
    }
    if ( vec.empty() )
        return dsn::rpc_address();
    int num = static_cast<int>(dsn_random32(0, vec.size()-1));
    return vec[num];
}

dsn::rpc_address naive_load_balancer::find_expected_minimal_load_machine(const std::vector<dsn::rpc_address>& candidate_list)
{
    dsn::rpc_address selected;
    int select_value;
    for (const dsn::rpc_address& addr: candidate_list)
    {
        if ( _state->_nodes[addr].is_alive )
        {
            int load_value = primaries_expected(addr);
            if (selected.is_invalid() || select_value>load_value)
                selected = addr, select_value = load_value;
        }
    }
    return selected;
}

dsn::rpc_address naive_load_balancer::find_expected_minimal_load_machine()
{
    dassert( !_pq_expect_primary.empty(), "");
    return *_pq_expect_primary.begin();
}

dsn::rpc_address naive_load_balancer::adjust_priority_queue_expected(dsn::rpc_address old_candidate, dsn::rpc_address new_candidate, bool is_old_died)
{
    if ( is_old_died )
    {
        _pq_expect_primary.erase(old_candidate);
        _pq_now_primary.erase(old_candidate);
        _future_primary.erase(old_candidate);
    }
    else if ( !old_candidate.is_invalid() )
    {
        _pq_expect_primary.erase(old_candidate);
        _future_primary[old_candidate]--;
        _pq_expect_primary.insert(old_candidate);
    }

    if ( !new_candidate.is_invalid() )
    {
        _pq_expect_primary.erase(new_candidate);
        _future_primary[new_candidate]++;
        _pq_expect_primary.insert(new_candidate);
    }
}

dsn::rpc_address naive_load_balancer::recommend_primary(partition_configuration& pc)
{
    dsn::rpc_address new_candidate;
    dsn::rpc_address old_candidate;
    if ( _primary_recommender.find(pc.gpid) != _primary_recommender.end() )
    {
        old_candidate = _primary_recommender[gpid];

        if ( _state->_nodes[old_candidate].is_alive )
            return old_candidate;

        if (pc.secondaries.size() > 0)
            new_candidate = find_expected_minimal_load_machine( pc.secondaries );
        else
            new_candidate = find_expected_minimal_load_machine();

        adjust_priority_queue_expected(old_candidate, new_candidate, true);
    }
    else
    {
        if (pc.secondaries.size() > 0)
            new_candidate = find_expected_minimal_load_machine( pc.secondaries );
        else
            new_candidate = find_expected_minimal_load_machine();

        adjust_priority_queue_expected(old_candidate, new_candidate, false);
    }

    _primary_recommender[gpid] = new_candidate;
    return new_candidate;
}

bool naive_load_balancer::run_lb(partition_configuration &pc)
{
    if (_state->freezed())
        return false;

    bool is_system_healthy = true;

    configuration_update_request proposal;
    proposal.config = pc;

    if (pc.primary.is_invalid())
    {
        if (pc.secondaries.size() > 0)
        {
            proposal.node = recommend_primary(pc);
            proposal.type = CT_UPGRADE_TO_PRIMARY;

            if (proposal.node.is_invalid())
            {
                derror("all replicas has been died");
            }
        }
        else if (pc.last_drops.size() == 0)
        {
            proposal.node = recommend_primary(pc);
            proposal.type = CT_ASSIGN_PRIMARY;
        }
        // DDD
        else
        {
            proposal.node = *pc.last_drops.rbegin();
            proposal.type = CT_ASSIGN_PRIMARY;

            derror("%s.%d.%d enters DDD state, we are waiting for its last primary node %s to come back ...",
                pc.app_type.c_str(),
                pc.gpid.app_id,
                pc.gpid.pidx,
                proposal.node.to_string()
                );
        }

        if (proposal.node.is_invalid() == false)
        {
            send_proposal(proposal.node, proposal);
        }

        is_system_healthy = false;
    }

    else if (static_cast<int>(pc.secondaries.size()) + 1 < pc.max_replica_count)
    {
        proposal.type = CT_ADD_SECONDARY;
        proposal.node = random_find_machine( pc.primary );
        if (proposal.node.is_invalid() == false &&
            proposal.node != pc.primary &&
            std::find(pc.secondaries.begin(), pc.secondaries.end(), proposal.node) == pc.secondaries.end())
        {
            send_proposal(pc.primary, proposal);
        }
        is_system_healthy = false;
    }
    else
    {
        is_system_healthy = true;
    }
    return is_system_healthy;
}
