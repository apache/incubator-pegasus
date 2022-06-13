#include "server_load_balancer.h"
#include <dsn/utility/extensible_object.h>
#include <dsn/utility/string_conv.h>
#include <dsn/tool-api/command_manager.h>
#include <boost/lexical_cast.hpp>
#include <dsn/utils/time_utils.h>

namespace dsn {
namespace replication {
newly_partitions::newly_partitions() : newly_partitions(nullptr) {}

newly_partitions::newly_partitions(node_state *ns)
    : owner(ns), total_primaries(0), total_partitions(0)
{
}

void *newly_partitions::s_create(void *related_ns)
{
    newly_partitions *result = new newly_partitions(reinterpret_cast<node_state *>(related_ns));
    return result;
}

void newly_partitions::s_delete(void *_this) { delete reinterpret_cast<newly_partitions *>(_this); }

bool newly_partitions::less_primaries(newly_partitions &another, int32_t app_id)
{
    int newly_p1 = primary_count(app_id);
    int newly_p2 = another.primary_count(app_id);
    if (newly_p1 != newly_p2)
        return newly_p1 < newly_p2;

    newly_p1 = partition_count(app_id);
    newly_p2 = another.partition_count(app_id);
    if (newly_p1 != newly_p2)
        return newly_p1 < newly_p2;

    newly_p1 = primary_count();
    newly_p2 = another.primary_count();
    if (newly_p1 != newly_p2)
        return newly_p1 < newly_p2;

    return partition_count() < another.partition_count();
}

bool newly_partitions::less_partitions(newly_partitions &another, int32_t app_id)
{
    int newly_p1 = partition_count(app_id);
    int newly_p2 = another.partition_count(app_id);
    if (newly_p1 != newly_p2)
        return newly_p1 < newly_p2;

    return partition_count() < another.partition_count();
}

void newly_partitions::newly_add_primary(int32_t app_id, bool only_primary)
{
    ++primaries[app_id];
    ++total_primaries;
    if (!only_primary) {
        ++partitions[app_id];
        ++total_partitions;
    }
}

void newly_partitions::newly_add_partition(int32_t app_id)
{
    ++partitions[app_id];
    ++total_partitions;
}

void newly_partitions::newly_remove_primary(int32_t app_id, bool only_primary)
{
    auto iter = primaries.find(app_id);
    dassert(iter != primaries.end(), "invalid app_id, app_id = %d", app_id);
    dassert(iter->second > 0, "invalid primary count, cnt = %d", iter->second);
    if (0 == (--iter->second)) {
        primaries.erase(iter);
    }

    dassert(total_primaries > 0, "invalid total primaires = %d", total_primaries);
    --total_primaries;

    if (!only_primary) {
        newly_remove_partition(app_id);
    }
}

void newly_partitions::newly_remove_partition(int32_t app_id)
{
    auto iter = partitions.find(app_id);
    dassert(iter != partitions.end(), "invalid app_id, app_id = %d", app_id);
    dassert(iter->second > 0, "invalid partition count, cnt = %d", iter->second);
    if ((--iter->second) == 0) {
        partitions.erase(iter);
    }

    dassert(total_partitions > 0, "invalid total partitions = ", total_partitions);
    --total_partitions;
}

newly_partitions *get_newly_partitions(node_mapper &mapper, const dsn::rpc_address &addr)
{
    node_state *ns = get_node_state(mapper, addr, false);
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

server_load_balancer::server_load_balancer(meta_service *svc) : _svc(svc) {}

void server_load_balancer::register_proposals(meta_view view,
                                              const configuration_balancer_request &req,
                                              configuration_balancer_response &resp)
{
    config_context &cc = *get_config_context(*view.apps, req.gpid);
    partition_configuration &pc = *get_config(*view.apps, req.gpid);
    if (!cc.lb_actions.empty()) {
        resp.err = ERR_INVALID_PARAMETERS;
        return;
    }

    std::vector<configuration_proposal_action> acts = req.action_list;
    for (configuration_proposal_action &act : acts) {
        // for some client generated proposals, the sender may not know the primary address.
        // e.g: "copy_secondary from a to b".
        // the client only knows the secondary a and secondary b, it doesn't know which target
        // to send the proposal to.
        // for these proposals, they should keep the target empty and
        // the meta-server will fill primary as target.
        if (act.target.is_invalid()) {
            if (!pc.primary.is_invalid())
                act.target = pc.primary;
            else {
                resp.err = ERR_INVALID_PARAMETERS;
                return;
            }
        }
    }

    resp.err = ERR_OK;
    cc.lb_actions.assign_balancer_proposals(acts);
    return;
}

void server_load_balancer::apply_balancer(meta_view view, const migration_list &ml)
{
    if (!ml.empty()) {
        configuration_balancer_response resp;
        for (auto &pairs : ml) {
            register_proposals(view, *pairs.second, resp);
            if (resp.err != dsn::ERR_OK) {
                const dsn::gpid &pid = pairs.first;
                dassert(false,
                        "apply balancer for gpid(%d.%d) failed",
                        pid.get_app_id(),
                        pid.get_partition_index());
            }
        }
    }
}
} // namespace replication
} // namespace dsn
