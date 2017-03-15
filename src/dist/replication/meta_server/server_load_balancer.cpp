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

int server_load_balancer::suggest_alive_time(config_type::type t)
{
    switch (t)
    {
    case config_type::CT_ASSIGN_PRIMARY:
    case config_type::CT_UPGRADE_TO_PRIMARY:
    case config_type::CT_DOWNGRADE_TO_INACTIVE:
    case config_type::CT_REMOVE:
    case config_type::CT_DOWNGRADE_TO_SECONDARY:
        // this should be fast, 1 minutes is enough
        return 60;

    case config_type::CT_ADD_SECONDARY:
    case config_type::CT_ADD_SECONDARY_FOR_LB:
        // default 900 seconds for add secondary
        return 900;
    default:
        dassert(false, "");
        return 0;
    }
}

void server_load_balancer::register_proposals(meta_view view, const configuration_balancer_request &req, configuration_balancer_response &resp)
{
    config_context& cc = *get_config_context(*view.apps, req.gpid);
    partition_configuration& pc = *get_config(*view.apps, req.gpid);
    if (!cc.lb_actions.empty())
    {
        resp.err = ERR_INVALID_PARAMETERS;
        return;
    }

    cc.lb_actions.acts = req.action_list;
    cc.lb_actions.from_balancer = true;
    for (configuration_proposal_action& act: cc.lb_actions.acts)
    {
        if (act.target.is_invalid())
            act.target = pc.primary;

        if (act.period_ts == 0)
            act.period_ts = suggest_alive_time(act.type);
        act.period_ts += (dsn_now_ms()/1000);
    }
    resp.err = ERR_OK;
    return;
}

void server_load_balancer::apply_balancer(meta_view view, const migration_list &ml)
{
    if (!ml.empty())
    {
        configuration_balancer_response resp;
        for (auto& pairs: ml)
        {
            server_load_balancer::register_proposals(view, *pairs.second, resp);
            if (resp.err != dsn::ERR_OK)
            {
                const dsn::gpid& pid = pairs.first;
                dassert(false, "apply balancer for gpid(%d.%d) failed", pid.get_app_id(), pid.get_partition_index());
            }
        }
    }
}

void simple_load_balancer::reconfig(meta_view view, const configuration_update_request &request)
{
    const dsn::gpid& gpid = request.config.pid;
    if (!((*view.apps)[gpid.get_app_id()]->is_stateful))
    {
        return;
    }

    config_context* cc = get_config_context(*(view.apps), gpid);
    partition_configuration* pc = get_config(*(view.apps), gpid);

    if (!cc->lb_actions.empty())
    {
        if (cc->lb_actions.acts.size()==1)
        {
            reset_proposal(view, gpid);
        }
        else
        {
            cc->lb_actions.pop_front();
        }
    }

    //handle the dropped out servers
    if (request.type == config_type::CT_DROP_PARTITION)
    {
        const std::vector<rpc_address>& config_dropped = request.config.last_drops;
        for (const rpc_address& drop_node: config_dropped)
        {
            cc->record_drop_history(drop_node);
        }
    }
    else
    {
        when_update_replicas(request.type, [cc, pc, &request](bool is_adding)
        {
            if (is_adding)
            {
                cc->remove_from_dropped(request.node);
            }
            else
            {
                dassert(cc->record_drop_history(request.node), "node(%s) has been in the dropped", request.node.to_string());
            }
        });
    }
}

void simple_load_balancer::reset_proposal(meta_view &view, const dsn::gpid &gpid)
{
    config_context* cc = get_config_context(*(view.apps), gpid);
    dassert(!cc->lb_actions.empty(), "");

    configuration_proposal_action& act = cc->lb_actions.acts.front();
    newly_partitions* np = get_newly_partitions(*(view.nodes), act.node);
    if (np==nullptr)
    {
        ddebug("can't get the newly_partitions extension structure for node(%s), the node may dead and may be removed",
            act.node.to_string());
    }
    else if (!cc->lb_actions.from_balancer)
    {
        when_update_replicas(act.type, [np, &gpid](bool is_adding)
        {
            if (is_adding)
                np->newly_remove_partition(gpid.get_app_id());
        });
    }

    cc->lb_actions.clear();
    cc->lb_actions.from_balancer = false;
}

bool simple_load_balancer::from_proposals(meta_view &view, const dsn::gpid &gpid, configuration_proposal_action &action)
{
    const partition_configuration& pc = *get_config(*(view.apps), gpid);
    config_context& cc = *get_config_context(*(view.apps), gpid);
    bool is_action_valid;

    if (cc.lb_actions.empty())
    {
        action.type = config_type::CT_INVALID;
        return false;
    }
    action = cc.lb_actions.acts.front();
    if (action.target.is_invalid() || action.node.is_invalid() || !is_node_alive(*(view.nodes), action.target))
        goto invalid_action;
    if (has_seconds_expired(action.period_ts))
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

pc_status simple_load_balancer::on_missing_primary(meta_view& view, const dsn::gpid& gpid)
{
    const partition_configuration& pc = *get_config(*(view.apps), gpid);
    proposal_actions& acts = get_config_context(*view.apps, gpid)->lb_actions;

    configuration_proposal_action action;
    pc_status result = pc_status::invalid;

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
            if (action.node.is_invalid() || np->less_primaries( *get_newly_partitions(*(view.nodes), action.node), gpid.get_app_id()))
            {
                action.node = ns->addr();
            }
        }

        if (action.node.is_invalid())
        {
            derror("all nodes for gpid(%d.%d) are dead, waiting for some secondary to come back....", gpid.get_app_id(), gpid.get_partition_index());
            result = pc_status::dead;
        }
        else
        {
            action.type = config_type::CT_UPGRADE_TO_PRIMARY;
            newly_partitions* np = get_newly_partitions( *(view.nodes), action.node);
            np->newly_add_primary(gpid.get_app_id());

            action.target = action.node;
            action.period_ts = suggest_alive_time(action.type);
            result = pc_status::ill;
        }
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
            action.period_ts = suggest_alive_time(action.type);
        }

        result = pc_status::ill;
    }
    else
    {
        action.node = *pc.last_drops.rbegin();
        derror("%d.%d enters DDD state, we are waiting for all replicas to come back, and select primary according to informations collected",
            pc.pid.get_app_id(),
            pc.pid.get_partition_index()
            );
        // when considering how to handle the DDD state, we must keep in mind that our shared-private/log data only write to OS-cache.
        // so the last removed replica can't act as primary directly.
        // We can only make choice by collecting all the messages of a group and have a comparision
        bool ready_for_decision = true;
        const config_context& cc = *get_config_context(*view.apps, gpid);

        // firstly check if all informations has been collected for node in last_drop
        for (const dsn::rpc_address& node: pc.last_drops)
        {
            if (ready_for_decision && !is_node_alive(*view.nodes, node))
            {
                dwarn("%s is not alive for %d.%d, don't make any decision", node.to_string(), pc.pid.get_app_id(), pc.pid.get_partition_index());
                ready_for_decision = false;
            }
            if (ready_for_decision)
            {
                auto iter = std::find_if(cc.dropped.begin(), cc.dropped.end(), [&node](const dropped_replica& r) { return r.node == node; });
                // A non-invalid time stamp means that the node hasn't report anything
                if (iter == cc.dropped.end() || iter->time != dropped_replica::INVALID_TIMESTAMP)
                {
                    dwarn("haven't yet collected information from %s of %d.%d for reason %s, don't make any decision",
                        node.to_string(), pc.pid.get_app_id(), pc.pid.get_partition_index(),
                        iter == cc.dropped.end()?"not exist in dropped":"no replica state");
                    ready_for_decision = false;
                }
            }
        }

        // then let's select a node with most information
        if (ready_for_decision)
        {
            auto iter = cc.dropped.rbegin();
            for (; iter != cc.dropped.rend(); ++iter)
            {
                if (std::find(pc.last_drops.begin(), pc.last_drops.end(), iter->node) != pc.last_drops.end())
                {
                    break;
                }
            }
            dassert(iter != cc.dropped.rend(), "can't find from confing_context::dropped for node (%s)", iter->node.to_string());
            const dropped_replica& r = *iter;
            action.node = r.node;
            action.target = r.node;
            action.type = config_type::CT_ASSIGN_PRIMARY;
            // well, we will give a very large alive time for the primary to come back
            // as this is our only choice
            action.period_ts = 86400;
            ddebug("select primary for DDD of %d.%d, node(%s), "
               "ballot(%" PRId64 "), committed_decree(%" PRId64 "), prepared_decree(%" PRId64")",
               gpid.get_app_id(),
               gpid.get_partition_index(),
               action.node.to_string(),
               r.ballot,
               r.last_committed_decree,
               r.last_prepared_decree);

            get_newly_partitions(*view.nodes, action.node)->newly_add_primary(gpid.get_app_id());
        }
        else
        {
            derror("we don't take any decision for %d.%d, administrator can assign a pirmary manually by shell", gpid.get_app_id(), gpid.get_partition_index());
            action.node.set_invalid();
        }
        result = pc_status::dead;
    }

    if (action.type != config_type::CT_INVALID)
    {
        action.period_ts += (dsn_now_ms()/1000);
        acts.assign_cure_proposal(action);
    }
    return result;
}

pc_status simple_load_balancer::on_missing_secondary(meta_view& view, const dsn::gpid& gpid)
{
    partition_configuration& pc = *get_config(*(view.apps), gpid);
    config_context& cc = *get_config_context(*(view.apps), gpid);
    proposal_actions& prop_acts = get_config_context(*(view.apps), gpid)->lb_actions;

    configuration_proposal_action action;
    bool is_emergency = false;
    if (replica_count(pc) < mutation_2pc_min_replica_count)
    {
        is_emergency = true;
        ddebug("gpid(%d.%d) in emergency due to too few replicas", gpid.get_app_id(), gpid.get_partition_index());
    }
    else if (cc.dropped.empty())
    {
        is_emergency = true;
        ddebug("gpid(%d.%d) in emergency due to no dropped candidate", gpid.get_app_id(), gpid.get_partition_index());
    }
    else if (has_milliseconds_expired(cc.dropped.back().time+replica_assign_delay_ms_for_dropouts))
    {
        is_emergency = true;
        ddebug("gpid(%d.%d) in emergency due to lose secondary for a long time", gpid.get_app_id(), gpid.get_partition_index());
    }
    action.node.set_invalid();

    if (is_emergency)
    {
        dassert(cc.prefered_dropped < (int)(cc.dropped.size()), "prefered_dropped(%d) may not updated when drop_list(size %d) update", cc.prefered_dropped, cc.dropped.size());
        while (cc.prefered_dropped >= 0)
        {
            dropped_replica& server = cc.dropped[cc.prefered_dropped--];
            if (is_node_alive(*view.nodes, server.node))
            {
                action.node = server.node;
                action.period_ts = 900;
                break;
            }
        }
    }
    else
    {
        for (auto iter=cc.dropped.rbegin(); iter!=cc.dropped.rend(); ++iter)
        {
            dropped_replica& server = *iter;
            if (is_node_alive(*view.nodes, server.node))
            {
                dassert(!server.node.is_invalid(), "");
                action.node = server.node;
                action.period_ts = 90;
                break;
            }
        }
        if (action.node.is_invalid())
        {
            ddebug("can't find valid node to add as secondary for gpid(%d.%d), ignore this as not in emergency", gpid.get_app_id(), gpid.get_partition_index());
            return pc_status::ill;
        }
    }

    if (action.node.is_invalid())
    {
        action.period_ts = 1500;
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

        action.period_ts += (dsn_now_ms()/1000);
        prop_acts.assign_cure_proposal(action);
    }
    return pc_status::ill;
}

pc_status simple_load_balancer::on_redundant_secondary(meta_view& view, const dsn::gpid& gpid)
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

    configuration_proposal_action action;
    action.type = config_type::CT_REMOVE;
    action.node = pc.secondaries[target];
    action.target = pc.primary;
    action.period_ts = dsn_now_ms()/1000 + suggest_alive_time(action.type);
    get_config_context(*view.apps, gpid)->lb_actions.assign_cure_proposal(action);
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
    const proposal_actions& acts = get_config_context(*view.apps, gpid)->lb_actions;

    dassert(app->is_stateful, "");
    dassert(acts.empty(), "");

    pc_status status;
    if (pc.primary.is_invalid())
        status = on_missing_primary(view, gpid);
    else if (static_cast<int>(pc.secondaries.size()) + 1 < pc.max_replica_count)
        status = on_missing_secondary(view, gpid);
    else if (static_cast<int>(pc.secondaries.size()) >= pc.max_replica_count)
        status = on_redundant_secondary(view, gpid);
    else
        status = pc_status::healthy;

    if (!acts.empty())
    {
        action = acts.acts.front();
    }
    return status;
}

bool simple_load_balancer::collect_replica(meta_view view, const rpc_address &node, const replica_info &info)
{
    partition_configuration& pc = *get_config(*view.apps, info.pid);
    if (is_member(pc, node))
        return true;

    config_context& cc = *get_config_context(*view.apps, info.pid);
    int ans = cc.collect_drop_replica(node, info);
    dassert(cc.check_order(), "");
    return ans != -1;
}

bool simple_load_balancer::construct_replica(meta_view view, const gpid &pid, int max_replica_count)
{
    partition_configuration& pc = *get_config(*view.apps, pid);
    config_context& cc = *get_config_context(*view.apps, pid);

    dassert(replica_count(pc)==0, "");
    dassert(max_replica_count>0, "max replica count is %d, should be at lease 1", max_replica_count);

    std::vector<dropped_replica>& drop_list = cc.dropped;
    if (drop_list.empty())
    {
        dwarn("construct for (%d.%d) failed, coz no replicas collected", pid.get_app_id(), pid.get_partition_index());
        return false;
    }

    //treat last server in drop_list as the primary
    dropped_replica& server = drop_list.back();
    dassert(server.time == dropped_replica::INVALID_TIMESTAMP, "");
    pc.primary = server.node;
    pc.ballot = server.ballot;
    pc.partition_flags = 0;
    pc.max_replica_count = max_replica_count;

    ddebug("construct for (%d.%d), select %s as primary, ballot(%" PRId64 "), committed_decree(%" PRId64 "), prepare_decree(%" PRId64 ")",
        pid.get_app_id(),
        pid.get_partition_index(),
        server.node.to_string(),
        server.ballot,
        server.last_committed_decree,
        server.last_prepared_decree);

    drop_list.pop_back();

    // we put max_replica_count-1 recent replicas to last_drops, in case of the DDD-state when the only primary dead
    // when add node to pc.last_drops, we don't remove it from our cc.drop_list
    dassert(pc.last_drops.empty(), "");
    for (auto iter=drop_list.rbegin(); iter!=drop_list.rend(); ++iter)
    {
        if (pc.last_drops.size()+1 >= max_replica_count)
            break;
        // similar to cc.drop_list, pc.last_drop is also a stack structure
        pc.last_drops.insert(pc.last_drops.begin(), iter->node);
        ddebug("construct for (%d.%d), select %s into last_drops, ballot(%" PRId64 "), committed_decree(%" PRId64 "), prepare_decree(%" PRId64 ")",
            pid.get_app_id(),
            pid.get_partition_index(),
            iter->node.to_string(),
            iter->ballot,
            iter->last_committed_decree,
            iter->last_prepared_decree);
    }

    cc.prefered_dropped = drop_list.size() - 1;
    return true;
}

}}
