#include "replica.h"
#include "replication_app_base.h"
#include "mutation.h"
#include "mutation_log.h"
#include "replica_stub.h"

#define __TITLE__ "FailOver"

namespace rdsn { namespace replication {

void replica::handle_local_failure(int error)
{
    rdsn_debug(
        "%s: handle local failure error %x, status = %s",
        name(),
        error,
        enum_to_string(status())
        );
    
    if (status() == PS_PRIMARY)
    {
        _stub->RemoveReplicaOnCoordinator(_primary_states.membership);
    }

    update_local_configuration_with_no_ballot_change(PS_ERROR);
}

void replica::handle_remote_failure(partition_status status, const end_point& node, int error)
{    
    rdsn_debug(
        "%s: handle remote failure error %u, status = %s, node = %s:%u",
        name(),
        error,
        enum_to_string(status),
        node.name.c_str(), (int)node.port
        );

    rdsn_assert (status == PS_PRIMARY, "");
    rdsn_assert (node != address(), "");

    switch (status)
    {
    case PS_SECONDARY:
        rdsn_assert (_primary_states.CheckExist(node, PS_SECONDARY), "");
        {
            configuration_update_request request;
            request.node = node;
            request.type = CT_DOWNGRADE_TO_INACTIVE;
            request.config = _primary_states.membership;
            downgrade_to_inactive_on_primary(request);
        }
        break;
    case PS_POTENTIAL_SECONDARY:
        // potential secondary failure does not lead to ballot change
        // therefore, it is possible to have multiple exec here
        if (_primary_states.Learners.erase(node) > 0)
        {
            if (_primary_states.CheckExist(node, PS_INACTIVE))
                _primary_states.Statuses[node] = PS_INACTIVE;
            else
                _primary_states.Statuses.erase(node);
        }
        
        break;
    case PS_INACTIVE:
    case PS_ERROR:
        break;
    default:
        rdsn_assert (false, "");
        break;
    }
}

void replica::on_meta_server_disconnected()
{
    rdsn_debug( "%s: coordinator disconnected", name());

    update_local_configuration_with_no_ballot_change(PS_INACTIVE);
}

}} // namespace
