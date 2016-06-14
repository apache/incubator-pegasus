#include "server_load_balancer.h"

namespace dsn { namespace replication {

void simple_load_balancer::reconfig(const meta_view &view, const configuration_update_request &request)
{
    const dsn::gpid& gpid = request.config.pid;
    if (!((*view.apps)[gpid.get_app_id()]->is_stateful))
    {
        return;
    }

    config_context* cc = get_mutable_context(*(view.apps), gpid);
    //handle dropout history
    if (request.type==config_type::CT_DOWNGRADE_TO_INACTIVE || request.type==config_type::CT_REMOVE)
    {
        //erase duplicate entries
        for (auto it=cc->history.begin(); it!=cc->history.end(); )
        {
            if (it->node == request.node)
                it = cc->history.erase(it);
            else
                ++it;
        }
        //push to back
        cc->history.push_back( {request.node, dsn_now_ms()} );
        while (cc->history.size() > 10) cc->history.pop_front();
    }

    //handle balancer proposals
    if (!cc->empty_balancer_proposals())
    {
        if (cc->balancer_proposal->action_list.size() == 1)
            cc->balancer_proposal.reset();
        else
            cc->balancer_proposal->action_list.erase(cc->balancer_proposal->action_list.begin());
    }
}

bool simple_load_balancer::from_proposals(const meta_view &view, const dsn::gpid &gpid, configuration_proposal_action &action)
{
    const partition_configuration& pc = *get_config(*(view.apps), gpid);
    config_context& cc = *get_mutable_context(*(view.apps), gpid);
    bool is_action_valid;

    if (cc.balancer_proposal==nullptr || cc.balancer_proposal->action_list.empty())
    {
        action.type = config_type::CT_INVALID;
        return false;
    }
    action = cc.balancer_proposal->action_list.front();
    if (action.target.is_invalid() || action.node.is_invalid() || !is_node_alive(*(view.nodes), action.target))
        goto invalid_action;

    switch (action.type)
    {
    case config_type::CT_ASSIGN_PRIMARY:
        is_action_valid = (action.node==action.target && pc.primary.is_invalid() && !is_secondary(pc, action.node));
        break;
    case config_type::CT_UPGRADE_TO_PRIMARY:
        is_action_valid = (action.node==action.target && pc.primary.is_invalid() && is_secondary(pc, action.node));
        break;
    case config_type::CT_ADD_SECONDARY:
    case config_type::CT_ADD_SECONDARY_FOR_LB:
        is_action_valid = (is_primary(pc, action.target) && !is_secondary(pc, action.node));
        is_action_valid = (is_action_valid && is_node_alive(*(view.nodes), action.node));
        break;
    case config_type::CT_DOWNGRADE_TO_INACTIVE:
    case config_type::CT_REMOVE:
        is_action_valid = (is_primary(pc, action.target) && is_member(pc, action.node));
        break;
    case config_type::CT_DOWNGRADE_TO_SECONDARY:
        is_action_valid = (action.target==action.node && is_primary(pc, action.target));
        break;
    default:
        is_action_valid = false;
        break;
    }

    if (is_action_valid)
        return true;

invalid_action:
    action.type = config_type::CT_INVALID;
    cc.balancer_proposal.reset();
    return false;
}

pc_status simple_load_balancer::on_missing_primary(const meta_view& view, const dsn::gpid& gpid, configuration_proposal_action& action)
{
    const partition_configuration& pc = *get_config(*(view.apps), gpid);
    action.type = config_type::CT_INVALID;
    if (pc.secondaries.size() > 0)
    {
        action.node = pc.secondaries[dsn_random32(0, static_cast<int>(pc.secondaries.size()) - 1)];
        action.type = config_type::CT_UPGRADE_TO_PRIMARY;
        action.target = action.node;
        return pc_status::ill;
    }
    else if (pc.last_drops.size() == 0)
    {
        std::vector<rpc_address> sort_result;
        sort_alive_nodes(*view.nodes, primary_comparator(*view.nodes), sort_result);
        if (!sort_result.empty())
        {
            action.type = config_type::CT_ASSIGN_PRIMARY;
            int min_load = (*view.nodes)[sort_result[0]].partitions.size();
            int i;
            for (i=1; i!=sort_result.size(); ++i)
                if ((*view.nodes)[sort_result[i]].partitions.size() != min_load)
                    break;
            action.node = sort_result[dsn_random32(0, i-1)];
            action.target = action.node;
        }
        return pc_status::ill;
    }
    else
    {
        action.node = *pc.last_drops.rbegin();
        action.type = config_type::CT_ASSIGN_PRIMARY;
        action.target = action.node;
        derror("%d.%d enters DDD state, we are waiting for its last primary node %s to come back ...",
            pc.pid.get_app_id(),
            pc.pid.get_partition_index(),
            action.node.to_string()
            );
        return pc_status::dead;
    }
}

pc_status simple_load_balancer::on_missing_secondary(const meta_view& view, const dsn::gpid& gpid, configuration_proposal_action& action)
{
    const partition_configuration& pc = *get_config(*(view.apps), gpid);
    config_context& cc = *get_mutable_context(*(view.apps), gpid);

    action.node.set_invalid();
    bool is_emergency = (static_cast<int>(pc.secondaries.size()) + 1 < mutation_2pc_min_replica_count);
    bool can_delay = false;
    auto it = cc.history.begin();
    while (it != cc.history.end())
    {
        dropped_server& d = *it;
        if (is_member(pc, d.node))
        {
            // erase if already primary or secondary, or have duplicate entry
            it = cc.history.erase(it);
        }
        else if (is_node_alive(*view.nodes, d.node))
        {
            // if have alive node, the use the latest lost node
            action.node = d.node;
            it++;
        }
        else if (action.node.is_invalid() && !is_emergency && !can_delay &&
            d.time + replica_assign_delay_ms_for_dropouts > dsn_now_ms())
        {
            // if not emergency and some node is lost recently,
            // then delay some time to wait it come back
            can_delay = true;
            it++;
        }
        else
        {
            it++;
        }
    }

    if (action.node.is_invalid() && !can_delay)
    {
        std::vector<rpc_address> sort_result;
        sort_node(*view.nodes, partition_comparator(*view.nodes), [&pc](const node_state& ns){
            return ns.is_alive && !ns.address.is_invalid() && !is_member(pc, ns.address);
        }, sort_result);

        if (!sort_result.empty())
        {
            action.target = pc.primary;
            action.type = config_type::CT_ADD_SECONDARY;

            int min_load = (*view.nodes)[sort_result[0]].partitions.size();
            int i=1;
            for (; i<sort_result.size(); ++i)
                if ((*view.nodes)[sort_result[i]].partitions.size() != min_load)
                    break;
            action.node = sort_result[dsn_random32(0, i-1)];
        }
    }

    if (!action.node.is_invalid())
    {
        action.type = config_type::CT_ADD_SECONDARY;
        action.target = pc.primary;
    }
    return pc_status::ill;
}

pc_status simple_load_balancer::on_redundant_secondary(const meta_view& view, const dsn::gpid& gpid, configuration_proposal_action& action)
{
    const node_mapper& nodes = *(view.nodes);
    const partition_configuration& pc = *get_config(*(view.apps), gpid);
    int target = 0;
    int load = nodes.find(pc.secondaries.front())->second.partitions.size();
    for (int i=0; i!=pc.secondaries.size(); ++i)
    {
        int l = nodes.find(pc.secondaries[i])->second.partitions.size();
        if (l > load)
        {
            load = l;
            target = i;
        }
    }
    action.type = config_type::CT_REMOVE;
    action.node = pc.secondaries[target];
    action.target = pc.primary;
    return pc_status::ill;
}

pc_status simple_load_balancer::on_missing_worker(
    const meta_view& view,
    const dsn::gpid& gpid,
    const partition_configuration_stateless& pcs,
    /*out*/configuration_proposal_action& act)
{
    std::vector<rpc_address> sort_result;
    sort_node(*view.nodes, partition_comparator(*view.nodes), [&pcs](const node_state& ns){
        return ns.is_alive && !ns.address.is_invalid() && !pcs.is_member(ns.address);
    }, sort_result);

    if (!sort_result.empty())
    {
        int min_load = (*view.nodes)[sort_result[0]].partitions.size();
        int i=1;
        for (; i<sort_result.size(); ++i)
            if ((*view.nodes)[sort_result[i]].partitions.size() != min_load)
                break;
        act.node = sort_result[dsn_random32(0, i-1)];
        act.target = act.node;
        act.type = config_type::CT_ADD_SECONDARY;
    }
    return pc_status::ill;
}

pc_status simple_load_balancer::cure(
    const meta_view& view,
    const dsn::gpid& gpid,
    configuration_proposal_action& action)
{
    if (from_proposals(view, gpid, action))
        return pc_status::ill;

    std::shared_ptr<app_state>& app = (*view.apps)[gpid.get_app_id()];
    const partition_configuration& pc = *get_config(*(view.apps), gpid);
    if (app->is_stateful)
    {
        if (pc.primary.is_invalid())
            return on_missing_primary(view, gpid, action);
        if (static_cast<int>(pc.secondaries.size()) + 1 < pc.max_replica_count)
            return on_missing_secondary(view, gpid, action);
        if (static_cast<int>(pc.secondaries.size()) >= pc.max_replica_count)
            return on_redundant_secondary(view, gpid, action);
    }
    else
    {
        partition_configuration_stateless pcs(const_cast<partition_configuration&>(pc));
        if (static_cast<int>(pcs.workers().size()) < pc.max_replica_count)
            return on_missing_worker(view, gpid, pcs, action);
    }
    return pc_status::healthy;
}

}}
