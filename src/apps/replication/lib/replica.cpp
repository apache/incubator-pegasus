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
replica::replica(replica_stub* stub, const char* path)
: serverlet<replica>("replica")
{
    dassert (stub, "");
    _stub = stub;
    _app = nullptr;
    _dir = path;
    _primary_address = primary_address();
    _options = &stub->options();

    init_state();
}

// for replica::newr(...) only
replica::replica(replica_stub* stub, global_partition_id gpid, const char* app_type)
: serverlet<replica>("replica")
{
    dassert (stub, "");
    _stub = stub;
    _app = nullptr;

    char buffer[256];
    sprintf(buffer, "%u.%u.%s", gpid.app_id, gpid.pidx, app_type);
    _dir = _stub->dir() + "/" + buffer;
    _primary_address = primary_address();
    _options = &stub->options();

    init_state();
    _config.gpid = gpid;
}

void replica::init_state()
{
    _inactive_is_transient = false;
    _prepare_list = new prepare_list(
        0, 
        _options->staleness_for_start_prepare_for_potential_secondary,
        std::bind(
            &replica::execute_mutation,
            this,
            std::placeholders::_1
            ),
        _options->prepare_ack_on_secondary_before_logging_allowed
    );

    _config.ballot = 0;
    _config.gpid.pidx = 0;
    _config.gpid.app_id = 0;
    _config.status = PS_INACTIVE;
    _primary_states.membership.ballot = 0;
    _last_config_change_time_ms = now_ms();
    _2pc_logger = nullptr;
    _log = nullptr;

    error_code err = ERR_OK;
    if (_options->log_private)
    {
        _log = new mutation_log(
            _options->log_buffer_size_mb,
            _options->log_pending_max_ms,
            _options->log_file_size_mb,
            _options->log_batch_write
            );

        std::string log_dir = dir() + "/log";
        err = _log->initialize(log_dir.c_str());
        dassert(err == ERR_OK, "");        
    }

    // setup 2pc logger
    if (_options->log_shared)
    { 
        _2pc_logger = _stub->_log;
    }
    else if (_options->log_private)
    {
        _2pc_logger = _log;
    }
    else
    {
        _2pc_logger = nullptr;
    }
}

replica::~replica(void)
{
    close();

    if (nullptr != _prepare_list)
    {
        delete _prepare_list;
        _prepare_list = nullptr;
    }
}

void replica::on_client_read(const read_request_header& meta, dsn_message_t request)
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

    rpc_read_stream reader(request);
    _app->dispatch_rpc_call(dsn_task_code_from_string(meta.code.c_str(), TASK_CODE_INVALID),
                            reader, dsn_msg_create_response(request));
}

void replica::response_client_message(dsn_message_t request, error_code error, decree d/* = invalid_decree*/)
{
    if (nullptr == request)
    {
        error.end_tracking();
        return;
    }   

    reply(request, error);
}

void replica::execute_mutation(mutation_ptr& mu)
{
    dassert (nullptr != _app, "");

    error_code err = ERR_OK;
    switch (status())
    {
    case PS_INACTIVE:
        if (_app->last_committed_decree() + 1 == mu->data.header.decree)
            err = _app->write_internal(mu);
        break;
    case PS_PRIMARY:
    case PS_SECONDARY:
        {
        dassert (_app->last_committed_decree() + 1 == mu->data.header.decree, "");
        err = _app->write_internal(mu);
        }
        break;
    case PS_POTENTIAL_SECONDARY:
        if (LearningSucceeded == _potential_secondary_states.learning_status)
        {
            if (mu->data.header.decree == _app->last_committed_decree() + 1)
            {
                err = _app->write_internal(mu); 
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

bool replica::group_configuration(/*out*/ partition_configuration& config) const
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
            || (!mu->is_logged() && !_options->prepare_ack_on_secondary_before_logging_allowed)
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

    if (_log != nullptr)
    {
        _log->close();
        delete _log;
        _log = nullptr;
    }
    _2pc_logger = nullptr;

    if (_app != nullptr)
    {
        _app->close(false);
        delete _app;
        _app = nullptr;
    }
}

}} // namespace
