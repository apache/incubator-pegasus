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
#include "replica.h"
#include "mutation.h"
#include "mutation_log.h"
#include "replica_stub.h"

# ifdef __TITLE__
# undef __TITLE__
# endif
# define __TITLE__ "replica"

namespace dsn { namespace replication {

// for replica::load(..) only
replica::replica(replica_stub* stub, replication_options& options)
: serverlet<replica>("replica")
{
    dassert (stub, "");
    _stub = stub;
    _app = nullptr;
        
    _options = options;

    init_state();
}

// for create new replica only used in replica_stub::on_config_proposal
replica::replica(replica_stub* stub, global_partition_id gpid, replication_options& options)
: serverlet<replica>("replica")
{
    dassert (stub, "");
    _stub = stub;
    _app = nullptr;    
    _options = options;

    init_state();
    _config.gpid = gpid;
}

void replica::init_state()
{
    _inactive_is_transient = false;
    _log = nullptr;
    _prepare_list = new prepare_list(
        0, 
        _options.staleness_for_start_prepare_for_potential_secondary,
        std::bind(
            &replica::execute_mutation,
            this,
            std::placeholders::_1
            ),
        _options.prepare_ack_on_secondary_before_logging_allowed
    );

    _config.ballot = 0;
    _config.gpid.pidx = 0;
    _config.gpid.app_id = 0;
    _config.status = PS_INACTIVE;
    _primary_states.membership.ballot = 0;
    _last_config_change_time_ms = now_ms();
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

void replica::on_client_read(const read_request_header& meta, message_ptr& request)
{
    if (status() == PS_INACTIVE || status() == PS_POTENTIAL_SECONDARY)
    {
        response_client_message(request, ERR_INVALID_STATE);
        return;
    }

    if (meta.semantic == read_semantic_t::ReadLastUpdate)
    {
        if (status() != PS_PRIMARY || 
            last_committed_decree() < _primary_states.last_prepare_decree_on_new_primary)
        {
            response_client_message(request, ERR_INVALID_STATE);
            return;
        }
    }

    dassert (_app != nullptr, "");
    _app->dispatch_rpc_call(meta.code, request, true);
}

void replica::response_client_message(message_ptr& request, error_code error, decree d/* = invalid_decree*/)
{
    if (nullptr == request)
        return;

    message_ptr resp = request->create_response();
    resp->writer().write(error);

    dassert(error != ERR_OK, "");
    dinfo("handle replication request with rpc_id = %016llx failed, err = %s",
        request->header().rpc_id, error.to_string());

    rpc::reply(resp);
}

void replica::execute_mutation(mutation_ptr& mu)
{
    dassert (nullptr != _app, "");

    error_code err = ERR_OK;
    switch (status())
    {
    case PS_INACTIVE:
        if (_app->last_committed_decree() + 1 == mu->data.header.decree)
            err = _app->write_internal(mu, false);
        break;
    case PS_PRIMARY:
    case PS_SECONDARY:
        {
        dassert (_app->last_committed_decree() + 1 == mu->data.header.decree, "");
        bool ack_client = (status() == PS_PRIMARY);
        if (ack_client)
        {
            if (mu->client_request == nullptr)
                ack_client = false;
            else if (mu->client_request->header().from_address.ip == 0)
                ack_client = false;
        }
        err = _app->write_internal(mu, ack_client); 
        }
        break;
    case PS_POTENTIAL_SECONDARY:
        if (LearningSucceeded == _potential_secondary_states.learning_status)
        {
            if (mu->data.header.decree == _app->last_committed_decree() + 1)
            {
                err = _app->write_internal(mu, false); 
            }
            else
            {
                dassert (mu->data.header.decree <= _app->last_committed_decree(), "");
            }
        }
        else
        {
            // drop mutations as learning will catch up
            ddebug("%s: mutation %s skipped coz learing buffer overflow", name(), mu->name());
        }
        break;
    case PS_ERROR:
        break;
    }
    
    ddebug("TwoPhaseCommit, %s: mutation %s committed, err = %s", name(), mu->name(), err.to_string());

    if (err != ERR_OK)
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
    mu->data.header.log_offset = invalid_offset;
    return mu;
}

bool replica::group_configuration(__out_param partition_configuration& config) const
{
    if (PS_PRIMARY != status())
        return false;

    config = _primary_states.membership;
    return true;
}

decree replica::last_durable_decree() const { return _app->last_durable_decree(); }

decree replica::last_prepared_decree() const
{
    ballot lastBallot = 0;
    decree start = last_committed_decree();
    while (true)
    {
        auto mu = _prepare_list->get_mutation_by_decree(start + 1);
        if (mu == nullptr 
            || mu->data.header.ballot < lastBallot 
            || (!mu->is_logged() && !_options.prepare_ack_on_secondary_before_logging_allowed)
            )
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
    _primary_states.cleanup();
    _potential_secondary_states.cleanup(true);

    if (_app != nullptr)
    {
        _app->close(false);
    }
}

}} // namespace
