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
#include "load_balancer.h"
#include <algorithm>

bool MachineLoadComp(const std::pair<end_point, int>& l, const std::pair<end_point, int>& r)
{
    return l.second < r.second;
}

load_balancer::load_balancer(server_state* state)
: _state(state), serverlet<load_balancer>("load_balancer")
{
}

load_balancer::~load_balancer()
{
}

void load_balancer::run()
{
    zauto_read_lock l(_state->_lock);

    for (size_t i = 0; i < _state->_apps.size(); i++)
    {
        app_state& app = _state->_apps[i];
        
        for (int j = 0; j < app.partition_count; j++)
        {
            partition_configuration& pc = app.partitions[j];
            run_lb(pc);
        }
    }
}

void load_balancer::run(global_partition_id gpid)
{
    zauto_read_lock l(_state->_lock);
    partition_configuration& pc = _state->_apps[gpid.app_id - 1].partitions[gpid.pidx];
    run_lb(pc);
}

end_point load_balancer::find_minimal_load_machine(bool primaryOnly)
{
    std::vector<std::pair<end_point, int>> stats;

    for (auto it = _state->_nodes.begin(); it != _state->_nodes.end(); it++)
    {
        if (it->second.is_alive)
        {
            stats.push_back(std::make_pair(it->first, static_cast<int>(primaryOnly ? it->second.primaries.size()
                : it->second.partitions.size())));
        }
    }

    
    std::sort(stats.begin(), stats.end(), [](const std::pair<end_point, int>& l, const std::pair<end_point, int>& r)
    {
        return l.second < r.second;
    });
    
    //std::sort(stats.begin(), stats.end(), MachineLoadComp);

    if (stats.empty())
    {
        return end_point::INVALID;
    }

    int candidateCount = 1;
    int val = stats[0].second;

    for (size_t i = 1; i < stats.size(); i++)
    {
        if (stats[i].second > val)
            break;
        candidateCount++;
    }

    return stats[env::random32(0, candidateCount - 1)].first;
}

void load_balancer::run_lb(partition_configuration& pc)
{
    if (_state->freezed())
        return;

    configuration_update_request proposal;
    proposal.config = pc;

    if (pc.primary == end_point::INVALID)
    {
        proposal.type = CT_ASSIGN_PRIMARY;
        if (pc.secondaries.size() > 0)
        {
            proposal.node = pc.secondaries[env::random32(0, static_cast<int>(pc.secondaries.size()) - 1)];
        }
        else
        {
            proposal.node = find_minimal_load_machine(true);
        }

        if (proposal.node != end_point::INVALID)
        {
            send_proposal(proposal.node, proposal);
        }
    }

    else if (static_cast<int>(pc.secondaries.size()) + 1 < pc.max_replica_count)
    {
        proposal.type = CT_ADD_SECONDARY;
        proposal.node = find_minimal_load_machine(false);
        if (proposal.node != end_point::INVALID)
        {
            send_proposal(pc.primary, proposal);
        }
    }
    else
    {
        // it is healthy, nothing to do
    }
}

// meta server => partition server
void load_balancer::send_proposal(const end_point& node, const configuration_update_request& proposal)
{
    rpc::call_one_way_typed(node, RPC_CONFIG_PROPOSAL, proposal, gpid_to_hash(proposal.config.gpid));
}

void load_balancer::query_decree(std::shared_ptr<query_replica_decree_request> query)
{
    rpc::call_typed(query->node, RPC_QUERY_PN_DECREE, query, this, &load_balancer::on_query_decree_ack, gpid_to_hash(query->gpid), 3000);
}

void load_balancer::on_query_decree_ack(error_code err, std::shared_ptr<query_replica_decree_request>& query, std::shared_ptr<query_replica_decree_response>& resp)
{
    if (err)
    {
        tasking::enqueue(LPC_QUERY_PN_DECREE, this, std::bind(&load_balancer::query_decree, this, query), 0, 1000);
    }
    else
    {
        zauto_write_lock l(_state->_lock);
        app_state& app = _state->_apps[query->gpid.app_id - 1];
        partition_configuration& ps = app.partitions[query->gpid.pidx];
        if (resp->last_decree > ps.last_committed_decree)
        {
            ps.last_committed_decree = resp->last_decree;
        }   
    }
}
