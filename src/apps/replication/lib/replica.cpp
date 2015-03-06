#include "replica.h"
#include "replication_app_base.h"
#include "mutation.h"
#include "mutation_log.h"
#include "replica_stub.h"

#define __TITLE__ "replica"

namespace rdsn { namespace replication {

// for replica::load(..) only
replica::replica(replica_stub* stub, replication_options& options)
: serviceletex<replica>("replica")
{
    rassert (stub, "");
    _stub = stub;
    _app = nullptr;
        
    _options = options;

    init_state();
}

// for create new replica only used in replica_stub::OnConfigProposal
replica::replica(replica_stub* stub, global_partition_id gpid, replication_options& options)
: serviceletex<replica>("replica")
{
    rassert (stub, "");
    _stub = stub;
    _app = nullptr;    
    _options = options;

    init_state();
    _config.gpid = gpid;
}

void replica::init_state()
{
    _prepare_list = new prepare_list(
        0, 
        _options.StalenessForStartPrepareForPotentialSecondary,
        std::bind(
            &replica::execute_mutation,
            this,
            std::placeholders::_1
            )
    );

    memset((void*)&_config, 0, sizeof(_config));
    _config.status = PS_INACTIVE;
    _primary_states.membership.ballot = 0;
    _last_config_change_time_ms =now_ms();
}

replica::~replica(void)
{
    close();

    if (nullptr != _prepare_list)
    {
        delete _prepare_list;
        _prepare_list = nullptr;
    }

    if (nullptr != _app)
    {
        delete _app;
        _app = nullptr;
    }
}

void replica::on_client_read(const client_read_request& meta, message_ptr& request)
{
    if (status() == PS_INACTIVE || status() == PS_POTENTIAL_SECONDARY)
    {
        response_client_message(request, ERR_INVALID_STATE);
        return;
    }

    rassert (_app != nullptr, "");
    _app->read(meta, request);
}

void replica::response_client_message(message_ptr& request, int error, decree d/* = invalid_decree*/)
{
    message_ptr resp = request->create_response();
    resp->write(error);

    rpc_response(resp);
}

void replica::execute_mutation(mutation_ptr& mu)
{
    rassert (nullptr != _app, "");

    int err = ERR_SUCCESS;
    switch (status())
    {
    case PS_INACTIVE:
        if (_app->last_committed_decree() + 1 == mu->data.header.decree)
            err = _app->WriteInternal(mu, false);
        break;
    case PS_PRIMARY:
    case PS_SECONDARY:
        {
        rassert (_app->last_committed_decree() + 1 == mu->data.header.decree, "");
        bool ackClient = (status() == PS_PRIMARY);
        if (ackClient)
        {
            if (mu->client_requests.size() == 0)
                ackClient = false;
            else if ((*mu->client_requests.begin())->header().from_address.ip == 0)
                ackClient = false;
        }
        err = _app->WriteInternal(mu, ackClient); 

        //PerformanceCounters::Increment(PerfCounters_LocalCommitQps, nullptr);
        }
        break;
    case PS_POTENTIAL_SECONDARY:
        if (LearningSucceeded == _potential_secondary_states.LearningState)
        {
            if (mu->data.header.decree == _app->last_committed_decree() + 1)
            {
                err = _app->WriteInternal(mu, false); 
            }
            else
            {
                rassert (mu->data.header.decree <= _app->last_committed_decree(), "");
            }
        }
        else
        {
            // drop mutations as learning will catch up
            rdebug("%s: mutation %s skipped coz learing buffer overflow", name(), mu->name());
        }
        break;
    case PS_ERROR:
        break;
    }
     
    rdebug("TwoPhaseCommit, %s: mutation %s committed, err = %x", name(), mu->name(), err);

    if (err != ERR_SUCCESS)
    {
        handle_local_failure(err);
    }
}

mutation_ptr replica::new_mutation(decree decree)
{
    mutation_ptr mu(new mutation());
    mu->data.header.gpid = get_gpid();
    mu->data.header.ballot = get_ballot();
    mu->data.header.decree = decree;
    mu->data.header.logOffset = invalid_offset;
    return mu;
}

bool replica::group_configuration(__out partition_configuration& config) const
{
    if (PS_PRIMARY != status())
        return false;

    config = _primary_states.membership;
    return true;
}

decree replica::last_durable_decree() const { return _app->last_durable_decree(); }

decree replica::LastPreparedDecree() const
{
    ballot lastBallot = 0;
    decree start = last_committed_decree();
    while (true)
    {
        auto mu = _prepare_list->get_mutation_by_decree(start + 1);
        if (mu == nullptr || mu->data.header.ballot < lastBallot || !mu->is_prepared())
            break;
        start++;
        lastBallot = mu->data.header.ballot;
    }
    return start;
}

void replica::close()
{
    if (status() != PS_INACTIVE && status() != PS_ERROR)
    {
        update_local_configuration_with_no_ballot_change(PS_INACTIVE);
    }

    cleanup_preparing_mutations(true);
    _primary_states.Cleanup();
    _potential_secondary_states.Cleanup(true);

    if (_app != nullptr)
    {
        _app->close(false);
    }
}

}} // namespace
