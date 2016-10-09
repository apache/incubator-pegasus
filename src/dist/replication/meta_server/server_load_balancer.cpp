#include <dsn/utility/extensible_object.h>
#include "server_load_balancer.h"

# ifdef __TITLE__
# undef __TITLE__
# endif
# define __TITLE__ "server.load.balancer"

namespace dsn { namespace replication {

/// server load balancer extensions for node_state
class newly_partitions
{
public:
    newly_partitions();
    newly_partitions(node_state* ns);
    node_state* owner;
    std::map<int32_t, int32_t> primaries;
    std::map<int32_t, int32_t> partitions;

    int32_t primary_count(int32_t app_id)
    {
        return owner->primary_count(app_id) + primaries[app_id];
    }
    int32_t partition_count(int32_t app_id)
    {
        return owner->partition_count(app_id) + partitions[app_id];
    }

    bool less_primaries(newly_partitions& another, int32_t app_id);
    bool less_partitions(newly_partitions& another, int32_t app_id);
    void newly_add_primary(int32_t app_id);
    void newly_add_partition(int32_t app_id);

    void newly_remove_primary(int32_t app_id);
    void newly_remove_partition(int32_t app_id);
public:
    static void* s_create(void* related_ns);
    static void s_delete(void* _this);
};
typedef dsn::object_extension_helper<newly_partitions, node_state> newly_partitions_ext;

newly_partitions::newly_partitions(): newly_partitions(nullptr)
{
}

newly_partitions::newly_partitions(node_state *ns): owner(ns)
{
}

void* newly_partitions::s_create(void *related_ns)
{
    newly_partitions* result = new newly_partitions( reinterpret_cast<node_state*>(related_ns) );
    return result;
}

void newly_partitions::s_delete(void *_this)
{
    delete reinterpret_cast<newly_partitions*>(_this);
}

bool newly_partitions::less_primaries(newly_partitions &another, int32_t app_id)
{
    int newly_p1 = primary_count(app_id);
    int newly_p2 = another.primary_count(app_id);
    if (newly_p1 != newly_p2)
        return newly_p1 < newly_p2;

    newly_p1 = partition_count(app_id);
    newly_p2 = another.partition_count(app_id);
    return newly_p1 < newly_p2;
}

bool newly_partitions::less_partitions(newly_partitions &another, int32_t app_id)
{
    int newly_p1 = partition_count(app_id);
    int newly_p2 = another.partition_count(app_id);
    return newly_p1 < newly_p2;
}

void newly_partitions::newly_add_primary(int32_t app_id)
{
    ++primaries[app_id];
    ++partitions[app_id];
}

void newly_partitions::newly_add_partition(int32_t app_id)
{
    ++partitions[app_id];
}

void newly_partitions::newly_remove_primary(int32_t app_id)
{
    auto iter = primaries.find(app_id);
    dassert(iter!=primaries.end(), "");
    dassert(iter->second>0, "");
    if (0 == (--iter->second))
    {
        primaries.erase(iter);
    }

    auto iter2 = partitions.find(app_id);
    dassert(iter2!=primaries.end(), "");
    dassert(iter2->second>0, "");
    if (0 == (--iter2->second))
    {
        partitions.erase(iter2);
    }
}

void newly_partitions::newly_remove_partition(int32_t app_id)
{
    auto iter = partitions.find(app_id);
    dassert(iter != partitions.end(), "");
    dassert(iter->second>0, "");
    if ((--iter->second) == 0)
    {
        partitions.erase(iter);
    }
}

inline newly_partitions* get_newly_partitions(node_mapper& mapper, const dsn::rpc_address& addr)
{
    node_state* ns = get_node_state(mapper, addr, false);
    if (ns == nullptr)
        return nullptr;
    return newly_partitions_ext::get_inited(ns);
}

class local_module_initializer
{
private:
    local_module_initializer()
    {
        newly_partitions_ext::register_ext(newly_partitions::s_create, newly_partitions::s_delete);
    }
public:
    static local_module_initializer _instance;
};
local_module_initializer local_module_initializer::_instance;
//// end of server load balancer extensions for node state

void simple_load_balancer::reconfig(meta_view view, const configuration_update_request &request)
{
    const dsn::gpid& gpid = request.config.pid;
    if (!((*view.apps)[gpid.get_app_id()]->is_stateful))
    {
        return;
    }

    config_context* cc = get_config_context(*(view.apps), gpid);
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
        {
            reset_proposal(view, gpid);
        }
        else
        {
            cc->balancer_proposal->action_list.erase(cc->balancer_proposal->action_list.begin());
        }
    }
}

void simple_load_balancer::reset_proposal(meta_view &view, const dsn::gpid &gpid)
{
    config_context* cc = get_config_context(*(view.apps), gpid);
    if (cc->empty_balancer_proposals())
        return;
    newly_partitions* np = get_newly_partitions(*(view.nodes), cc->balancer_proposal->action_list.front().node);
    if (np==nullptr)
    {
        ddebug("can't get the newly_partitions extension structure for node(%s), the node may dead and may be removed",
            cc->balancer_proposal->action_list.front().node.to_string());
    }
    else if (cc->is_cure_proposal)
    {
        switch (cc->balancer_proposal->action_list.front().type)
        {
        case config_type::CT_ASSIGN_PRIMARY:
        case config_type::CT_UPGRADE_TO_PRIMARY:
        case config_type::CT_ADD_SECONDARY:
            np->newly_remove_partition(gpid.get_app_id());
            break;
        default:
            break;
        }
    }

    cc->balancer_proposal->action_list.clear();
    cc->is_cure_proposal = false;
}

bool simple_load_balancer::from_proposals(meta_view &view, const dsn::gpid &gpid, configuration_proposal_action &action)
{
    const partition_configuration& pc = *get_config(*(view.apps), gpid);
    config_context& cc = *get_config_context(*(view.apps), gpid);
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
    std::stringstream ss;
    ss << action;
    ddebug("proposal action(%s) for gpid(%d.%d) is invalid, clear all proposal actions", ss.str().c_str(), gpid.get_app_id(), gpid.get_partition_index());
    action.type = config_type::CT_INVALID;

    reset_proposal(view, gpid);
    return false;
}

pc_status simple_load_balancer::on_missing_primary(meta_view& view, const dsn::gpid& gpid, configuration_proposal_action& action)
{
    const partition_configuration& pc = *get_config(*(view.apps), gpid);
    action.type = config_type::CT_INVALID;
    if (pc.secondaries.size() > 0)
    {
        action.node.set_invalid();

        for (int i=0; i<pc.secondaries.size(); ++i)
        {
            node_state* ns = get_node_state(*(view.nodes), pc.secondaries[i], false);
            dassert(ns!=nullptr, "");
            if (!ns->alive())
                continue;

            newly_partitions* np = newly_partitions_ext::get_inited(ns);
            if (action.node.is_invalid() || np->less_primaries( *get_newly_partitions(*(view.nodes), action.node), gpid.get_app_id()) )
            {
                action.node = ns->addr();
            }
        }

        if (action.node.is_invalid())
        {
            derror("all nodes for gpid(%s.%s) are dead, waiting for some secondary to come back....", gpid.get_app_id(), gpid.get_partition_index());
            return pc_status::dead;
        }

        action.type = config_type::CT_UPGRADE_TO_PRIMARY;
        newly_partitions* np = get_newly_partitions( *(view.nodes), action.node);
        np->newly_add_primary(gpid.get_app_id());

        action.target = action.node;
        return pc_status::ill;
    }
    else if (pc.last_drops.size() == 0)
    {
        dsn::rpc_address min_primary_server;
        newly_partitions* min_primary_server_np = nullptr;

        for (auto& pairs: *view.nodes)
        {
            node_state& ns = pairs.second;
            if (!ns.alive())
                continue;
            newly_partitions* np = newly_partitions_ext::get_inited(&ns);
            if (min_primary_server_np==nullptr || np->less_primaries(*min_primary_server_np, gpid.get_app_id()))
            {
                min_primary_server = ns.addr();
                min_primary_server_np = np;
            }
        }

        if (min_primary_server_np != nullptr)
        {
            action.node = min_primary_server;
            action.target = action.node;
            action.type = config_type::CT_ASSIGN_PRIMARY;
            min_primary_server_np->newly_add_primary(gpid.get_app_id());
        }
        return pc_status::ill;
    }
    else
    {
        action.node = *pc.last_drops.rbegin();
        derror("%d.%d enters DDD state, we are waiting for its last primary node %s to come back ...",
            pc.pid.get_app_id(),
            pc.pid.get_partition_index(),
            action.node.to_string()
            );

        if (is_node_alive(*view.nodes, action.node))
        {
            action.type = config_type::CT_ASSIGN_PRIMARY;
            action.target = action.node;
            get_newly_partitions(*view.nodes, action.node)->newly_add_primary(gpid.get_app_id());
        }
        else
            action.node.set_invalid();
        return pc_status::dead;
    }
}

pc_status simple_load_balancer::on_missing_secondary(meta_view& view, const dsn::gpid& gpid, configuration_proposal_action& action)
{
    const partition_configuration& pc = *get_config(*(view.apps), gpid);
    config_context& cc = *get_config_context(*(view.apps), gpid);

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
        newly_partitions* min_server_np = nullptr;

        for (auto& pairs: *view.nodes)
        {
            node_state& ns = pairs.second;
            if (!ns.alive() || is_member(pc, ns.addr()))
                continue;
            newly_partitions* np = newly_partitions_ext::get_inited(&ns);
            if (min_server_np==nullptr || np->less_partitions(*min_server_np, gpid.get_app_id()))
            {
                action.node = ns.addr();
                min_server_np = np;
            }
        }
    }

    if (!action.node.is_invalid())
    {
        action.type = config_type::CT_ADD_SECONDARY;
        action.target = pc.primary;

        newly_partitions* np = get_newly_partitions(*(view.nodes), action.node);
        dassert(np!=nullptr, "");
        np->newly_add_partition(gpid.get_app_id());
    }
    return pc_status::ill;
}

pc_status simple_load_balancer::on_redundant_secondary(meta_view& view, const dsn::gpid& gpid, configuration_proposal_action& action)
{
    const node_mapper& nodes = *(view.nodes);
    const partition_configuration& pc = *get_config(*(view.apps), gpid);
    int target = 0;
    int load = nodes.find(pc.secondaries.front())->second.partition_count();
    for (int i=0; i!=pc.secondaries.size(); ++i)
    {
        int l = nodes.find(pc.secondaries[i])->second.partition_count();
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
    meta_view& view,
    const dsn::gpid& gpid,
    const partition_configuration_stateless& pcs,
    /*out*/configuration_proposal_action& act)
{
    std::vector<rpc_address> sort_result;
    sort_node(*view.nodes, partition_comparator(*view.nodes), [&pcs](const node_state& ns){
        return ns.alive() && !ns.addr().is_invalid() && !pcs.is_member(ns.addr());
    }, sort_result);

    if (!sort_result.empty())
    {
        int min_load = (*view.nodes)[sort_result[0]].partition_count();
        int i=1;
        for (; i<sort_result.size(); ++i)
            if ((*view.nodes)[sort_result[i]].partition_count() != min_load)
                break;
        act.node = sort_result[dsn_random32(0, i-1)];
        act.target = act.node;
        act.type = config_type::CT_ADD_SECONDARY;
    }
    return pc_status::ill;
}

pc_status simple_load_balancer::cure(
    meta_view view,
    const dsn::gpid& gpid,
    configuration_proposal_action& action)
{
    if (from_proposals(view, gpid, action))
        return pc_status::ill;

    std::shared_ptr<app_state>& app = (*view.apps)[gpid.get_app_id()];
    const partition_configuration& pc = *get_config(*(view.apps), gpid);

    if (app->is_stateful)
    {
        pc_status status;
        if (pc.primary.is_invalid())
            status = on_missing_primary(view, gpid, action);
        else if (static_cast<int>(pc.secondaries.size()) + 1 < pc.max_replica_count)
            status = on_missing_secondary(view, gpid, action);
        else if (static_cast<int>(pc.secondaries.size()) >= pc.max_replica_count)
            status = on_redundant_secondary(view, gpid, action);
        else
            status = pc_status::healthy;

        if (action.type != config_type::CT_INVALID)
        {
            config_context* cc = get_config_context(*(view.apps), gpid);
            dassert(cc->balancer_proposal.get()!=nullptr, "");
            dassert(cc->empty_balancer_proposals(), "");
            cc->balancer_proposal->action_list.push_back(action);
            cc->is_cure_proposal = true;
        }
        return status;
    }
    else
    {
        partition_configuration_stateless pcs(const_cast<partition_configuration&>(pc));
        if (static_cast<int>(pcs.workers().size()) < pc.max_replica_count)
            return on_missing_worker(view, gpid, pcs, action);
        return pc_status::healthy;
    }
}

}}
