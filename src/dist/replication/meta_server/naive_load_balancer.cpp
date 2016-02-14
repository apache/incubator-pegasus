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
{
}

naive_load_balancer::~naive_load_balancer()
{
}

/* utils function for walk through */
bool naive_load_balancer::walk_through_primary(const rpc_address &addr, const std::function<bool (partition_configuration &)>& func)
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
bool naive_load_balancer::walk_through_partitions(const rpc_address &addr, const std::function<bool (partition_configuration &)>& func)
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

void naive_load_balancer::execute_migration_proposal()
{
    for (auto iter=_migration_proposals_map.begin(); iter!=_migration_proposals_map.end(); ++iter)
    {
        migration_proposal& p = iter->second;
        global_partition_id& gpid = iter->first;
        switch (proposal._type)
        {
        case mt_move_primary: {
            dassert(pc.primary==p._from, "invalid migration proposal");
            partition_configuration& pc = _state->_apps[gpid.app_id-1].partitions[gpid.pidx];
            configuration_update_request proposal;
            proposal.config = pc;
            proposal.type = CT_DOWNGRADE_TO_SECONDARY;
            proposal.node = pc.primary;
            send_proposal(pc.primary, proposal);
            }
            break;

        case mt_copy_primary: {
            dassert(pc.primary==p._from, "invalid migration proposal");
            partition_configuration& pc = _state->_apps[gpid.app_id-1].partitions[gpid.pidx];
            configuration_update_request proposal;
            proposal.config = pc;
            proposal.type = CT_ADD_SECONDARY;
            proposal.node = p._to;

            send_proposal(pc.primary, proposal);
            }
            break;

        case mt_copy_secondary: {
            dassert(false, "");
            }
            break;

        default:
            break;
        }
    }
}

void naive_load_balancer::greedy_primary_balancer(
        const std::vector<rpc_address> &node_list,
        const std::vector<int> &prev,
        int flows)
{
    int current = prev[node_list.size() - 1];
    while (prev[current] != 0)
    {
        dsn::rpc_address from = node_list[prev[current]];
        dsn::rpc_address to = node_list[current];

        int primaries_need_to_remove = flows;
        walk_through_primary(from, [this, &](partition_configuration &pc){
            if (0 == primary_need_to_remove)
                return false;
            for (dsn::rpc_address& addr: pc.secondaries)
            {
                if (addr == to)
                {
                    _migration_proposals_map.emplace(gpid, migration_proposal(gpid, type, from, to));
                    --primaries_need_to_remove;
                    break;
                }
            }
            return true;
        });

        current = prev[current];
    }
    execute_migration_proposal();
}

void naive_load_balancer::greedy_secondary_balancer(const std::vector< std::vector<int> >& network,
                                                    const std::vector<dsn::rpc_address>& node_list,
                                                    )
{

}

void naive_load_balancer::greedy_balancer(int total_replicas)
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
    int light_servers_count = 0;

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

        int primaries_count = iter->second.primaries.size();
        if (primaries_count > average_replicas)
            network[0][from] = primaries_count - average_replicas;
        else {
            network[from][graph_nodes - 1] = average_replicas - primaries_count;
            if (network[from][graph_nodes - 1] > 0)
                light_servers_count++;
        }
    }

    if ( light_servers_count == 0)
    {
        greedy_copy_secondary();
        return;
    }

    std::vector<bool> visit(graph_nodes, false);
    std::vector<int> flow(graph_nodes, 0);
    std::vector<int> prev(graph_nodes, -1);
    int pos, max_value;
    flow[0] = INT_MAX;

    while ( !visit[graph_nodes - 1] )
    {
        pos = -1, max_value = 0;
        for (int i=0; i!=graph_nodes; ++i)
        {
            if (visit[i]==false && flow[i]>max_value)
            {
                pos = i;
                max_value = flow[i];
            }
        }

        if (pos == -1)
            break;

        visit[pos] = true;
        for (int i=0; i!=graph_nodes; ++i)
        {
            if (!visit[i] && min(flow[pos], network[pos][i])>flow[i])
            {
                flow[i] = min(flow[pos], network[pos][i]);
                prev[i] = pos;
            }
        }
    }

    //we can't make the server load more balanced by moving primaries to secondaries
    if (pos == -1)
    {
        greedy_copy_primary();
    }
    else
    {
        greedy_move_primary(node_list, prev, flow[graph_nodes-1]);
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
        if ( _migration_proposals_map.empty() )
            greedy_balancer(total_replicas);
        else
            execute_migration_proposal();
    }
}

void naive_load_balancer::run(global_partition_id gpid, std::shared_ptr<configuration_update_request> request)
{
    zauto_read_lock l(_state->_lock);
    std::unordered_map<global_partition_id, migration_proposal>::iterator it;
    switch (request->type)
    {
    case CT_DOWNGRADE_TO_SECONDARY:
    {
        it = _migration_proposals_map.find(gpid);
        dassert(it != _migration_proposals_map.end(), "");
        _primary_recommender[gpid] = it->second._to;
        _migration_proposals_map.erase(it);
    }
        break;

    case CT_ADD_SECONDARY_FOR_LB:
        it = _migration_proposals_map.find(gpid);
        dassert(it != _migration_proposals_map.end(), "");
        if (it->second._type == mt_copy_primary)
        {
            it->second._type = mt_move_primary;
            return;
        }
        else
            _migration_proposals_map.erase(it);
        break;

    case CT_DOWNGRADE_TO_INACTIVE:
        it = _migration_proposals_map.find(gpid);
        if (it != _migration_proposals_map.end())
        {
            migration_proposal& p = it->second;
            if (request->config.primary.is_invalid())
                _primary_recommender[gpid] = it->second._to;
            _migration_proposals_map.erase(it);
        }
        break;
    default:
        break;
    }

    partition_configuration& pc = _state->_apps[gpid.app_id-1].partitions[gpid.pidx];
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

dsn::rpc_address naive_load_balancer::recommend_primary(partition_configuration& pc)
{
    auto iter = _primary_recommender.find(pc.gpid);
    if (iter != _primary_recommender.end())
    {
        if ( !_state->_nodes[iter->second].is_alive )
            iter->second = random_find_machine(pc.secondaries);
        return iter->second;
    }
    else {
        dsn::rpc_address result = random_find_machine(pc.secondaries);
        _primary_recommender.emplace(pc.gpid, result);
        return result;
    }
}

bool naive_load_balancer::run_lb(partition_configuration &pc)
{
    if (_state->freezed())
        return false;

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

        return false;
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
        return false;
    }
    //we have too many secondaries, let's do remove
    else if (static_cast<int>(pc.secondaries.size()) >= pc.max_replica_count)
    {
        proposal.type = CT_REMOVE;
        proposal.node = find_max_load_machine(pc.secondaries);
        send_proposal(pc.primary, proposal);
        return false;
    }
    else
        return true;
}
