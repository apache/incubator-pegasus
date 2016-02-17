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
 */

#include "greedy_load_balancer.h"
#include <algorithm>

# ifdef __TITLE__
# undef __TITLE__
# endif
# define __TITLE__ "load.balancer.greedy"

greedy_load_balancer::greedy_load_balancer(server_state* state):
    dsn::dist::server_load_balancer(state),
    serverlet<greedy_load_balancer>("greedy_load_balancer"),
    _is_greedy_rebalancer_enabled(true)
{
}

greedy_load_balancer::~greedy_load_balancer()
{
}

/* utils function for walk through */
bool greedy_load_balancer::walk_through_primary(const rpc_address &addr, const std::function<bool (partition_configuration &)>& func)
{
    auto iter = _state->_nodes.find(addr);
    if (iter == _state->_nodes.end() )
        return false;
    server_state::node_state& ns = iter->second;
    for (const global_partition_id& gpid: ns.primaries)
    {
        partition_configuration& pc = _state->_apps[gpid.app_id - 1].partitions[gpid.pidx];
        if ( !func(pc) )
            return false;
    }
    return true;
}

/* utils function for walk through */
bool greedy_load_balancer::walk_through_partitions(const rpc_address &addr, const std::function<bool (partition_configuration &)>& func)
{
    auto iter = _state->_nodes.find(addr);
    if (iter == _state->_nodes.end() )
        return false;
    server_state::node_state& ns = iter->second;
    for (const global_partition_id& gpid: ns.partitions)
    {
        partition_configuration& pc = _state->_apps[gpid.app_id - 1].partitions[gpid.pidx];
        if ( !func(pc) )
            return false;
    }
    return true;
}

bool greedy_load_balancer::balancer_proposal_check(const balancer_proposal_request &balancer_proposal)
{
    auto is_node_alive = [&, this](const dsn::rpc_address& addr) {
        auto iter = _state->_nodes.find(addr);
        if (iter == _state->_nodes.end())
            return false;
        return iter->second.is_alive;
    };

    auto is_primary = [&, this](const dsn::rpc_address& addr, const global_partition_id& gpid)
    {
        partition_configuration& pc = _state->_apps[gpid.app_id - 1].partitions[gpid.pidx];
        return pc.primary == addr;
    };

    auto is_secondary = [&, this](const dsn::rpc_address& addr, const global_partition_id& gpid)
    {
        partition_configuration& pc = _state->_apps[gpid.app_id - 1].partitions[gpid.pidx];
        return std::find(pc.secondaries.begin(), pc.secondaries.end(), addr)!=pc.secondaries.end();
    };

    const global_partition_id& gpid = balancer_proposal.gpid;
    if (gpid.app_id < 0 ||
        gpid.pidx < 0 ||
        gpid.app_id > _state->_apps.size() ||
        gpid.pidx >= _state->_apps[gpid.app_id - 1].partitions.size() )
        return false;

    if ( !is_node_alive(balancer_proposal.from) || !is_node_alive(balancer_proposal.to) )
        return false;

    if (balancer_proposal.from == balancer_proposal.to)
        return false;

    switch (balancer_proposal.type)
    {
    case BT_MOVE_PRIMARY:
        return is_primary(balancer_proposal.from, gpid) && is_secondary(balancer_proposal.to, gpid);
    case BT_COPY_PRIMARY:
        return is_primary(balancer_proposal.from, gpid) && !is_secondary(balancer_proposal.to, gpid);
    case BT_COPY_SECONDARY:
        return is_secondary(balancer_proposal.from, gpid) &&
               !is_primary(balancer_proposal.to, gpid) &&
               !is_secondary(balancer_proposal.to, gpid);
    default:
        return false;
    }
}
void greedy_load_balancer::execute_balancer_proposal()
{
    dinfo("execute balancer proposal, proposals count(%u)", _balancer_proposals_map.size());
    std::vector<global_partition_id> staled_balancer_proposal;
    for (auto iter=_balancer_proposals_map.begin(); iter!=_balancer_proposals_map.end(); ++iter)
    {
        balancer_proposal_request& p = iter->second;
        const global_partition_id& gpid = iter->first;
        partition_configuration& pc = _state->_apps[gpid.app_id-1].partitions[gpid.pidx];
        configuration_update_request proposal;

        if ( !balancer_proposal_check(p) )
        {
            staled_balancer_proposal.push_back(gpid);
            continue;
        }

        switch (p.type)
        {
        case BT_MOVE_PRIMARY:
            dassert(pc.primary==p.from, "invalid balancer proposal");

            proposal.config = pc;
            proposal.type = CT_DOWNGRADE_TO_SECONDARY;
            proposal.node = pc.primary;
            break;

        case BT_COPY_PRIMARY:
            dassert(pc.primary==p.from, "invalid balancer proposal");

            proposal.config = pc;
            proposal.type = CT_ADD_SECONDARY_FOR_LB;
            proposal.node = p.to;
            break;

        case BT_COPY_SECONDARY:
            proposal.config = pc;
            proposal.type = CT_ADD_SECONDARY_FOR_LB;
            proposal.node = p.to;
            break;

        default:
            break;
        }

        send_proposal(pc.primary, proposal);
    }

    dinfo("%u proposals are staled as they don't match the partition config", staled_balancer_proposal.size());
    for (const global_partition_id& gpid: staled_balancer_proposal)
        _balancer_proposals_map.erase(gpid);
}

void greedy_load_balancer::greedy_move_primary(const std::vector<rpc_address> &node_list, const std::vector<int> &prev, int flows)
{
    dinfo("%u primaries are flew", flows);
    int current = prev[node_list.size() - 1];
    while (prev[current] != 0)
    {
        dsn::rpc_address from = node_list[prev[current]];
        dsn::rpc_address to = node_list[current];

        int primaries_need_to_remove = flows;
        walk_through_primary(from, [&, this](partition_configuration &pc)
        {
            if (0 == primaries_need_to_remove)
                return false;
            for (dsn::rpc_address& addr: pc.secondaries)
            {
                if (addr == to)
                {
                    insert_balancer_proposal_request(pc.gpid, BT_MOVE_PRIMARY, from, to);
                    --primaries_need_to_remove;
                    break;
                }
            }
            return true;
        });
        current = prev[current];
    }
    execute_balancer_proposal();
}

void greedy_load_balancer::greedy_copy_primary(int total_replicas)
{
    if (_state->_node_live_count <= 0)
        return;

    std::unordered_map<dsn::rpc_address, int> changing_primaries;
    for (auto iter=_state->_nodes.begin(); iter!=_state->_nodes.end(); ++iter)
    {
        if ( iter->second.is_alive )
            changing_primaries[iter->first] = iter->second.primaries.size();
    }

    typedef std::function<bool (const dsn::rpc_address&, const dsn::rpc_address&)> comparator;
    std::set<dsn::rpc_address, comparator> pri_queue([&changing_primaries](const dsn::rpc_address& r1, const dsn::rpc_address& r2)
    {
        int c1 = changing_primaries[r1], c2 = changing_primaries[r2];
        if (c1 != c2)
            return c1<c2;
        return r1<r2;
    });

    for (auto iter=_state->_nodes.begin(); iter!=_state->_nodes.end(); ++iter) {
        if (iter->second.is_alive)
            pri_queue.insert(iter->first);
    }

    int replicas_low = total_replicas/_state->_node_live_count;
    int replicas_high = (total_replicas+_state->_node_live_count-1)/_state->_node_live_count;

    while ( changing_primaries[*pri_queue.begin()]<replicas_low ||
            changing_primaries[*pri_queue.rbegin()]>replicas_high )
    {
        dsn::rpc_address min_load = *pri_queue.begin();
        dsn::rpc_address max_load = *pri_queue.rbegin();

        dinfo("server with min/max load: %s:%d/%s:%d",
              min_load.to_string(), changing_primaries[min_load], max_load.to_string(), changing_primaries[max_load]);

        pri_queue.erase(pri_queue.begin());
        pri_queue.erase(--pri_queue.end());

        dassert(min_load!=max_load, "min load and max load machines shouldn't the same");

        //currently we simply random copy one primary from one machine to another
        //TODO: a better policy is necessary if considering the copying cost
        const global_partition_id& gpid = *(_state->_nodes[max_load].primaries.begin());
        partition_configuration& pc = _state->_apps[gpid.app_id-1].partitions[gpid.pidx];

        //we run greedy_copy_primary after we run the greedy_move_primary. If a secondary exist on min_load,
        //we are able to do the move_primary in greedy_move_primary
        dassert( std::find(pc.secondaries.begin(), pc.secondaries.end(), min_load)==pc.secondaries.end(), "");

        insert_balancer_proposal_request(gpid, BT_COPY_PRIMARY, max_load, min_load);
        dinfo("copy gpid(%d:%d) primary from %s to %s", gpid.app_id, gpid.pidx, max_load.to_string(), min_load.to_string());
        //adjust the priority queue
        ++changing_primaries[min_load];
        --changing_primaries[max_load];

        pri_queue.insert(min_load);
        pri_queue.insert(max_load);
    }

    dinfo("start to execute proposal");
    execute_balancer_proposal();
}

//TODO: a better secondary balancer policy
void greedy_load_balancer::greedy_copy_secondary()
{
    int total_replicas = 0;
    for (auto iter=_state->_nodes.begin(); iter!=_state->_nodes.end(); ++iter)
    {
        server_state::node_state& state = iter->second;
        if ( state.is_alive )
            total_replicas += state.partitions.size();
    }

    int replicas_low = total_replicas/_state->_node_live_count;
    int replicas_high = (total_replicas+_state->_node_live_count-1)/_state->_node_live_count;

    std::unordered_map<dsn::rpc_address, int> changing_secondaries;
    int maximum_secondaries = 0;
    dsn::rpc_address output_server;
    std::vector<server_state::node_state*> input_server;

    for (auto iter=_state->_nodes.begin(); iter!=_state->_nodes.end(); ++iter)
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

    walk_through_partitions(output_server, [&, this](partition_configuration& pc)
    {
        if (pc.primary == output_server)
            return true;
        if (maximum_secondaries <= replicas_high)
            return false;

        for (server_state::node_state* ns: input_server)
        {
            if ( changing_secondaries[ns->address] < replicas_low && ns->partitions.find(pc.gpid)==ns->partitions.end() )
            {
                ++changing_secondaries[ns->address];
                --maximum_secondaries;

                insert_balancer_proposal_request(pc.gpid, BT_COPY_SECONDARY, output_server, ns->address);
                break;
            }
        }
        return true;
    });

    if (_balancer_proposals_map.empty())
    {
        dwarn("can't copy secondaries");
    }
    else
        execute_balancer_proposal();
}

// load balancer based on fulkson-ford
void greedy_load_balancer::greedy_balancer(int total_replicas)
{
    if ( !_balancer_proposals_map.empty() )
    {
        dinfo("won't start new round of balancer coz old proposals(%d) exist", _balancer_proposals_map.size());
        execute_balancer_proposal();
        return;
    }

    if ( !_is_greedy_rebalancer_enabled )
    {
        dinfo("won't start balancer coz it is disabled");
        return;
    }

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

    int replicas_low = total_replicas/_state->_node_live_count;
    int replicas_high = (total_replicas+_state->_node_live_count - 1)/_state->_node_live_count;
    int lower_count = 0, higher_count = 0;
    // make graph
    for (auto iter=_state->_nodes.begin(); iter!=_state->_nodes.end(); ++iter)
    {
        if ( !iter->second.is_alive )
            continue;
        int from = node_id[iter->first];
        walk_through_primary(iter->first, [&, this](partition_configuration& replica_in_primary){
            for (auto& target: replica_in_primary.secondaries)
            {
                auto i = node_id.find(target);
                if ( i != node_id.end() )
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
        dinfo("the primaries is balanced, start to do secondary balancer");
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
        greedy_copy_primary(total_replicas);
    else
        greedy_move_primary(node_list, prev, flow[graph_nodes-1]);
}

void greedy_load_balancer::on_control_migration(/*in*/const control_balancer_migration_request& request,
                                                /*out*/control_balancer_migration_response& response)
{
    zauto_write_lock l(_state->_lock);
    dinfo("%s greedy rebalancer, ", request.enable_migration?"enable":"disable");

    _is_greedy_rebalancer_enabled = request.enable_migration;
    if ( !_is_greedy_rebalancer_enabled )
    {
        _balancer_proposals_map.clear();
    }
    response.err = ERR_OK;
}

void greedy_load_balancer::on_balancer_proposal(/*in*/const balancer_proposal_request& request,
                                                /*out*/balancer_proposal_response& response)
{
    zauto_write_lock l(_state->_lock);
    if ( !balancer_proposal_check(request) )
        response.err = ERR_INVALID_PARAMETERS;
    else {
        _balancer_proposals_map[request.gpid] = request;
        response.err = ERR_OK;
    }
}

void greedy_load_balancer::run()
{
    zauto_read_lock l(_state->_lock);

    dinfo("start to run global balancer");
    bool is_system_healthy = !_state->freezed();
    int total_replicas = 0;

    for (size_t i = 0; i < _state->_apps.size(); i++)
    {
        app_state& app = _state->_apps[i];
        if (app.status != AS_AVAILABLE)
        {
            dinfo("ignore app(%s)", app.app_name.c_str());
            continue;
        }
        for (int j = 0; j < app.partition_count; j++)
        {
            partition_configuration& pc = app.partitions[j];
            is_system_healthy = (run_lb(pc) && is_system_healthy);
        }
        total_replicas += app.partition_count;
    }

    if (is_system_healthy)
    {
        dinfo("system is healthy, trying to do balancer");
        greedy_balancer(total_replicas);
    }
}

void greedy_load_balancer::on_config_changed(std::shared_ptr<configuration_update_request> request)
{
    std::unordered_map<global_partition_id, balancer_proposal_request>::iterator it;
    global_partition_id& gpid = request->config.gpid;
    switch (request->type)
    {
    case CT_DOWNGRADE_TO_SECONDARY:
        it = _balancer_proposals_map.find(gpid);
        //it is possible that we can't find gpid, coz the meta server may switch
        if (it != _balancer_proposals_map.end())
        {
            _primary_recommender[gpid] = it->second.to;
            _balancer_proposals_map.erase(it);
        }
        break;

    case CT_UPGRADE_TO_SECONDARY:
        it = _balancer_proposals_map.find(gpid);
        if (it != _balancer_proposals_map.end())
        {
            balancer_proposal_request& p = it->second;
            //this secondary is what we add
            if (p.to == request->node)
            {
                if (p.type == BT_COPY_PRIMARY)
                    p.type = BT_MOVE_PRIMARY;
                else if (p.type == BT_COPY_SECONDARY)
                    _balancer_proposals_map.erase(it);
                else
                {
                    //do nothing
                }
            }
        }
        break;

    case CT_DOWNGRADE_TO_INACTIVE:
        //which means that the primary has been died
        if ( request->config.primary.is_invalid() )
        {
            _balancer_proposals_map.erase(gpid);
            _primary_recommender.erase(gpid);
        }
        else if (_balancer_proposals_map.find(gpid) != _balancer_proposals_map.end())
        {
            balancer_proposal_request& p = _balancer_proposals_map[gpid];
            if (p.type == BT_COPY_SECONDARY && request->node==p.from ||
                p.type == BT_MOVE_PRIMARY && request->node==p.to)
                _balancer_proposals_map.erase(gpid);
        }
        break;
    default:
        break;
    }
}

void greedy_load_balancer::run(global_partition_id gpid)
{
    zauto_read_lock l(_state->_lock);
    partition_configuration& pc = _state->_apps[gpid.app_id-1].partitions[gpid.pidx];
    run_lb(pc);
}

dsn::rpc_address greedy_load_balancer::find_minimal_load_machine(bool primaryOnly)
{
    std::vector<std::pair< ::dsn::rpc_address, int>> stats;

    for (auto it = _state->_nodes.begin(); it != _state->_nodes.end(); ++it)
    {
        if (it->second.is_alive)
        {
            stats.push_back(std::make_pair(it->first, static_cast<int>(primaryOnly ? it->second.primaries.size()
                : it->second.partitions.size())));
        }
    }

    if (stats.empty())
    {
        return ::dsn::rpc_address();
    }

    std::sort(stats.begin(), stats.end(), [](const std::pair< ::dsn::rpc_address, int>& l, const std::pair< ::dsn::rpc_address, int>& r)
    {
        return l.second < r.second || (l.second == r.second && l.first < r.first);
    });

    int candidate_count = 1;
    int val = stats[0].second;

    for (size_t i = 1; i < stats.size(); i++)
    {
        if (stats[i].second > val)
            break;
        candidate_count++;
    }

    return stats[dsn_random32(0, candidate_count - 1)].first;
}

dsn::rpc_address greedy_load_balancer::recommend_primary(partition_configuration& pc)
{
    auto find_machine_from_secondaries = [this](const std::vector<dsn::rpc_address>& addr_list)
    {
        if ( addr_list.empty() )
            return find_minimal_load_machine(true);
        int target = -1;
        int load = -1;
        for (int i=0; i!=addr_list.size(); ++i)
        {
            if (_state->_nodes[addr_list[i]].is_alive)
            {
                int l = _state->_nodes[addr_list[i]].primaries.size();
                if (target == -1 || load>l)
                {
                    target = i;
                    load = l;
                }
            }
        }
        if (target == -1)
            return dsn::rpc_address();
        return addr_list[target];
    };

    auto iter = _primary_recommender.find(pc.gpid);
    if (iter != _primary_recommender.end())
    {
        if ( _state->_nodes[iter->second].is_alive )
            return iter->second;
        _primary_recommender.erase(iter);
    }

    dsn::rpc_address result = find_machine_from_secondaries(pc.secondaries);
    if ( !result.is_invalid() )
        _primary_recommender.emplace(pc.gpid, result);
    return result;
}

bool greedy_load_balancer::run_lb(partition_configuration &pc)
{
    if (_state->freezed())
    {
        dinfo("state is freezed, node_alive count: %d, total: %d", _state->_node_live_count, _state->_nodes.size());
        return false;
    }
    dinfo("lb for gpid(%d.%d)", pc.gpid.app_id, pc.gpid.pidx);

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
        proposal.node = find_minimal_load_machine(false);
        if (proposal.node.is_invalid() == false &&
            proposal.node != pc.primary &&
            std::find(pc.secondaries.begin(), pc.secondaries.end(), proposal.node) == pc.secondaries.end())
        {
            send_proposal(pc.primary, proposal);
        }
        return false;
    }
    //we have too many secondaries, let's do remove
    else if (!pc.secondaries.empty() && static_cast<int>(pc.secondaries.size()) >= pc.max_replica_count)
    {
        auto iter = _balancer_proposals_map.find(pc.gpid);
        if (iter != _balancer_proposals_map.end() && iter->second.type == BT_MOVE_PRIMARY)
            return true;
        int target = 0;
        int load = _state->_nodes[pc.secondaries.front()].partitions.size();
        for (int i=1; i<pc.secondaries.size(); ++i)
        {
            int l = _state->_nodes[pc.secondaries[i]].partitions.size();
            if (l > load)
            {
                load = l;
                target = i;
            }
        }

        proposal.type = CT_REMOVE;
        proposal.node = pc.secondaries[target];
        send_proposal(pc.primary, proposal);
        return false;
    }
    else
        return true;
}
