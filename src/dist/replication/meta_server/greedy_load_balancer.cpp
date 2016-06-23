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
 *     a greedy load balancer implementation
 *
 * Revision history:
 *     2016-02-3, Weijie Sun(sunweijie@xiaomi.com), first version
 *     2016-04-25, Weijie Sun(sunweijie at xiaomi.com), refactor, now the balancer only have strategy
 */

#include <algorithm>
#include "greedy_load_balancer.h"

# ifdef __TITLE__
# undef __TITLE__
# endif
# define __TITLE__ "lb.greedy"

namespace dsn { namespace replication {

std::shared_ptr<configuration_balancer_request> greedy_load_balancer::generate_balancer_request(
    const partition_configuration &pc,
    const balance_type &type,
    const rpc_address &from,
    const rpc_address &to)
{
    configuration_balancer_request result;
    result.gpid = pc.pid;

    switch (type) {
    case balance_type::move_primary:
        result.action_list.emplace_back( configuration_proposal_action{ from, from, config_type::CT_DOWNGRADE_TO_SECONDARY} );
        result.action_list.emplace_back( configuration_proposal_action{ to, to, config_type::CT_UPGRADE_TO_PRIMARY } );
        break;
    case balance_type::copy_primary:
        result.action_list.emplace_back( configuration_proposal_action{ from, to, config_type::CT_ADD_SECONDARY_FOR_LB } );
        result.action_list.emplace_back( configuration_proposal_action{ from, from, config_type::CT_DOWNGRADE_TO_SECONDARY } );
        result.action_list.emplace_back( configuration_proposal_action{ to, to, config_type::CT_UPGRADE_TO_PRIMARY } );
        result.action_list.emplace_back( configuration_proposal_action{ to, from, config_type::CT_REMOVE} );
        break;
    case balance_type::copy_secondary:
        result.action_list.emplace_back( configuration_proposal_action{ pc.primary, to, config_type::CT_ADD_SECONDARY_FOR_LB} );
        result.action_list.emplace_back( configuration_proposal_action{ pc.primary, from, config_type::CT_REMOVE } );
        break;
    default:
        dassert(false, "");
    }
    return std::make_shared<configuration_balancer_request>(std::move(result));
}

void greedy_load_balancer::greedy_move_primary(const std::vector<rpc_address> &node_list, const std::vector<int> &prev, int flows)
{
    dinfo("%u primaries are flew", flows);
    int current = prev[node_list.size() - 1];
    while (prev[current] != 0)
    {
        rpc_address from = node_list[prev[current]];
        rpc_address to = node_list[current];
        int primaries_need_to_move = flows;
        walk_through_primary(*_view, from, [&, this](const partition_configuration &pc)
        {
            if (0 == primaries_need_to_move)
                return false;
            for (const rpc_address& addr: pc.secondaries)
            {
                if (addr == to)
                {
                    dinfo("plan to move primary, gpid(%d.%d), from(%s), to(%s)", pc.pid.get_app_id(), pc.pid.get_partition_index(),
                          from.to_string(), to.to_string());
                    _migration->emplace_back( generate_balancer_request(pc, balance_type::move_primary, from, to) );
                    --primaries_need_to_move;
                    break;
                }
            }
            return true;
        });
        current = prev[current];
    }
}

//assume all nodes are alive
void greedy_load_balancer::greedy_copy_primary()
{
    std::unordered_map<dsn::rpc_address, int> changing_primaries;
    for (auto iter=_view->nodes->begin(); iter!=_view->nodes->end(); ++iter)
        changing_primaries[iter->first] = iter->second.primaries.size();

    std::set<dsn::rpc_address, node_comparator> pri_queue([&changing_primaries](const dsn::rpc_address& r1, const dsn::rpc_address& r2)
    {
        int c1 = changing_primaries[r1], c2 = changing_primaries[r2];
        if (c1 != c2)
            return c1<c2;
        return r1<r2;
    });

    for (auto iter=_view->nodes->begin(); iter!=_view->nodes->end(); ++iter) {
        pri_queue.insert(iter->first);
    }

    int replicas_low = total_partitions/alive_nodes;
    int replicas_high = (total_partitions+alive_nodes-1)/alive_nodes;

    node_mapper& nodes = *(_view->nodes);
    while ( changing_primaries[*pri_queue.begin()]<replicas_low || changing_primaries[*pri_queue.rbegin()]>replicas_high )
    {
        dsn::rpc_address min_load = *pri_queue.begin();
        dsn::rpc_address max_load = *pri_queue.rbegin();

        dinfo("server with min/max load: %s have %d/%s have %d",
            min_load.to_string(), changing_primaries[min_load], max_load.to_string(), changing_primaries[max_load]);

        pri_queue.erase(pri_queue.begin());
        pri_queue.erase(--pri_queue.end());

        dassert(min_load!=max_load, "min load and max load machines shouldn't the same");

        //currently we simply random copy one primary from one machine to another
        //TODO: a better policy is necessary if considering the copying cost
        const dsn::gpid& gpid = *(nodes[max_load].primaries.begin());
        const partition_configuration& pc = *get_config(*(_view->apps), gpid);

        //we run greedy_copy_primary after we run the greedy_move_primary. If a secondary exist on min_load,
        //we are able to do the move_primary in greedy_move_primary
        dassert( std::find(pc.secondaries.begin(), pc.secondaries.end(), min_load)==pc.secondaries.end(), "");

        _migration->emplace_back( generate_balancer_request(pc, balance_type::copy_primary, max_load, min_load) );
        dinfo("copy gpid(%d:%d) primary from %s to %s", gpid.get_app_id(), gpid.get_partition_index(), max_load.to_string(), min_load.to_string());

        //adjust the priority queue
        ++changing_primaries[min_load];
        pri_queue.insert(min_load);
    }
}

//TODO: a better secondary balancer policy
void greedy_load_balancer::greedy_copy_secondary()
{
    int total_replicas = 0;
    for (auto iter=_view->nodes->begin(); iter!=_view->nodes->end(); ++iter)
    {
        const node_state& state = iter->second;
        if ( state.is_alive )
            total_replicas += state.partitions.size();
    }

    int replicas_low = total_replicas/alive_nodes;
    int replicas_high = (total_replicas+alive_nodes-1)/alive_nodes;

    std::unordered_map<dsn::rpc_address, int> changing_secondaries;
    int maximum_secondaries = 0;
    dsn::rpc_address output_server;
    std::vector<node_state*> input_server;

    node_mapper& nodes = *(_view->nodes);
    for (auto iter=nodes.begin(); iter!=nodes.end(); ++iter)
    {
        if ( iter->second.is_alive ) {
            int l = iter->second.partitions.size();
            changing_secondaries[iter->first] = l;
            if (l > maximum_secondaries) {
                maximum_secondaries = l;
                output_server = iter->first;
            }
            if (l < replicas_low)
                input_server.push_back( &(iter->second) );
        }
    }

    if (maximum_secondaries <= replicas_high)
    {
        dinfo("secondaries is balanced, just ignore");
        return;
    }

    walk_through_partitions(*_view, output_server, [&, this](const partition_configuration& pc)
    {
        if (pc.primary == output_server)
            return true;
        if (maximum_secondaries <= replicas_high)
            return false;

        for (node_state* ns: input_server)
        {
            if ( changing_secondaries[ns->address] < replicas_low && ns->partitions.find(pc.pid)==ns->partitions.end() )
            {
                ++changing_secondaries[ns->address];
                --maximum_secondaries;
                dinfo("plan to cp secondary: gpid(%d.%d), from(%s), to(%s)", pc.pid.get_app_id(), pc.pid.get_partition_index(),
                      output_server.to_string(), ns->address.to_string());
                _migration->emplace_back( generate_balancer_request(pc, balance_type::copy_secondary, output_server, ns->address) );
                break;
            }
        }
        return true;
    });
}

// load balancer based on ford-fulkerson
void greedy_load_balancer::greedy_balancer()
{
    dassert(alive_nodes>2, "too few alive nodes will lead to freeze");

    const node_mapper& nodes = *(_view->nodes);
    size_t graph_nodes = alive_nodes + 2;
    std::vector< std::vector<int> > network(graph_nodes, std::vector<int>(graph_nodes, 0));
    std::unordered_map<dsn::rpc_address, int> node_id;
    std::vector<dsn::rpc_address> node_list(graph_nodes);

    // assign id for nodes
    int current_id = 1;
    for (auto iter=nodes.begin(); iter!=nodes.end(); ++iter)
    {
        dassert(!iter->first.is_invalid() &&
            !iter->second.address.is_invalid() &&
            iter->second.is_alive, "nodes(%s) not alive shouldn't here", iter->second.address.to_string());
        node_id[ iter->first ] = current_id;
        node_list[ current_id ] = iter->first;
        ++current_id;
    }

    int replicas_low = total_partitions/alive_nodes;
    int replicas_high = (total_partitions+alive_nodes-1)/alive_nodes;
    int lower_count = 0, higher_count = 0;

    // make graph
    for (auto iter=nodes.begin(); iter!=nodes.end(); ++iter)
    {
        dassert(iter->second.is_alive, "");
        dassert(node_id.find(iter->first) != node_id.end(), "");
        int from = node_id[iter->first];
        walk_through_primary(*_view, iter->first, [&, this](const partition_configuration& replica_in_primary){
            for (auto& target: replica_in_primary.secondaries)
            {
                auto i = node_id.find(target);
                dassert(i != node_id.end(), "");
                network[from][ i->second ]++;
            }
            return true;
        } );

        int primaries_count = iter->second.primaries.size();
        if (primaries_count > replicas_low)
            network[0][from] = primaries_count - replicas_low;
        else
            network[from][graph_nodes - 1] = replicas_low - primaries_count;

        if (primaries_count > replicas_high)
            higher_count++;
        if (primaries_count < replicas_low)
            lower_count++;
    }

    if (higher_count==0 && lower_count==0)
    {
        ddebug("the primaries is balanced, start to do secondary balancer");
        greedy_copy_secondary();
        return;
    }
    else if (higher_count>0 && lower_count==0)
    {
        for (int i=0; i!=graph_nodes; ++i){
            if (network[0][i] > 0)
                --network[0][i];
            else
                ++network[i][graph_nodes - 1];
        }
    }
    ddebug("start to move primary");
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
            if (!visit[i] && std::min(flow[pos], network[pos][i])>flow[i])
            {
                flow[i] = std::min(flow[pos], network[pos][i]);
                prev[i] = pos;
            }
        }
    }

    //we can't make the server load more balanced by moving primaries to secondaries
    if (!visit[graph_nodes - 1] || flow[graph_nodes - 1]==0)
        greedy_copy_primary();
    else
        greedy_move_primary(node_list, prev, flow[graph_nodes-1]);
}

bool greedy_load_balancer::balance(const meta_view &view, migration_list &list)
{
    list.clear();

    total_partitions = count_partitions(*(view.apps));
    alive_nodes = view.nodes->size();
    _view = &view;
    _migration = &list;
    _migration->clear();
    greedy_balancer();
    return !_migration->empty();
}

}}
