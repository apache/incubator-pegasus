
#include "replica.h"
#include "mutation.h"
#include "replication_app_base.h"

#define __TITLE__ "TwoPhaseCommit"

namespace rdsn { namespace replication {

replication_app_base::replication_app_base(replica* replica, const replication_app_config* config)
{
    _dir = replica->dir();
    _replica = replica;
}

int replication_app_base::WriteInternal(mutation_ptr& mu, bool ackClient)
{
    rdsn_assert (mu->data.header.decree == last_committed_decree() + 1, "");

    int err = write(mu->client_requests, mu->data.header.decree, ackClient);

    //rdsn_assert(mu->data.header.decree == last_committed_decree(), "");

    return err;
}

void replication_app_base::WriteReplicationResponse(message_ptr& response)
{
    int err = ERR_SUCCESS;
    response->write(err);
}

}} // end namespace
