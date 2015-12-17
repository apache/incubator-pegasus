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

/*
 * Description:
 *     helper functions in replica object
 *
 * Revision history:
 *     Mar., 2015, @imzhenyu (Zhenyu Guo), first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

#include "replica.h"
#include "mutation.h"
#include "mutation_log.h"
#include "replica_stub.h"
#include <dsn/cpp/json_helper.h>

# ifdef __TITLE__
# undef __TITLE__
# endif
# define __TITLE__ "replica"

namespace dsn { namespace replication {

replica::replica(replica_stub* stub, global_partition_id gpid, const char* app_type, const char* dir)
    : serverlet<replica>("replica"), 
    _primary_states(gpid, stub->options().staleness_for_commit, stub->options().batch_write_disabled)
{
    dassert(stub != nullptr, "");
    _stub = stub;
    _app_type = app_type;
    _dir = dir;
    sprintf(_name, "%u.%u@%s", gpid.app_id, gpid.pidx, stub->_primary_address.to_string());
    _options = &stub->options();
    init_state();
    _config.gpid = gpid;
}

void replica::json_state(std::stringstream& out) const
{
    JSON_DICT_ENTRIES(out, *this, name(), _config, _app->last_committed_decree(), _app->last_durable_decree());
}

void replica::update_commit_statistics(int count)
{
    _stub->_counter_replicas_total_commit_throught.add((uint64_t)count);
}

void replica::init_state()
{
    _inactive_is_transient = false;
    _prepare_list = new prepare_list(
        0, 
        _options->max_mutation_count_in_prepare_list,
        std::bind(
            &replica::execute_mutation,
            this,
            std::placeholders::_1
            )
    );

    _config.ballot = 0;
    _config.gpid.pidx = 0;
    _config.gpid.app_id = 0;
    _config.status = PS_INACTIVE;
    _primary_states.membership.ballot = 0;
    _last_config_change_time_ms = now_ms();
    _private_log = nullptr;
}

replica::~replica(void)
{
    close();

    if (nullptr != _prepare_list)
    {
        delete _prepare_list;
        _prepare_list = nullptr;
    }

    dinfo("%s: replica destroyed", name());
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

    ddebug("%s: reply client write, err = %s", name(), error.to_string());
    reply(request, error);
}

//error_code replica::check_and_fix_private_log_completeness()
//{
//    error_code err = ERR_OK;
//
//    auto mind = _private_log->max_gced_decree(get_gpid());
//    if (_prepare_list->max_decree())
//
//    if (!(mind <= last_durable_decree()))
//    {
//        err = ERR_INCOMPLETE_DATA;
//        derror("%s: private log is incomplete (gced/durable): %" PRId64 " vs %" PRId64,
//            name(),
//            mind,
//            last_durable_decree()
//            );
//    }
//    else
//    {
//        mind = _private_log->max_decree(get_gpid());
//        if (!(mind >= _app->last_committed_decree()))
//        {
//            err = ERR_INCOMPLETE_DATA;
//            derror("%s: private log is incomplete (max/commit): %" PRId64 " vs %" PRId64,
//                name(),
//                mind,
//                _app->last_committed_decree()
//                );
//        }
//    }
//    
//    if (ERR_INCOMPLETE_DATA == err)
//    {
//        _private_log->close(true);
//        _private_log->open(nullptr);
//        _private_log->set_private(get_gpid(), _app->last_durable_decree());
//    }
//    
//    return err;
//}

void replica::check_state_completeness()
{
    /* prepare commit durable */
    dassert(max_prepared_decree() >= last_committed_decree(), "");
    dassert(last_committed_decree() >= last_durable_decree(), "");

    auto mind = _stub->_log->max_gced_decree(get_gpid(), _app->log_info().init_offset_in_shared_log);
    dassert(mind <= last_durable_decree(), "");
    _stub->_log->check_log_start_offset(get_gpid(), _app->log_info().init_offset_in_shared_log);

    if (_private_log != nullptr)
    {   
        auto mind = _private_log->max_gced_decree(get_gpid(), _app->log_info().init_offset_in_private_log);
        dassert(mind <= last_durable_decree(), "");

        _private_log->check_log_start_offset(get_gpid(), _app->log_info().init_offset_in_private_log);
    }
}

void replica::execute_mutation(mutation_ptr& mu)
{
    dinfo("%s: execute mutation %s: request_count = %u",
        name(), 
        mu->name(), 
        static_cast<int>(mu->client_requests.size())
        );

    error_code err = ERR_OK;
    decree d = mu->data.header.decree;

    switch (status())
    {
    case PS_INACTIVE:
        if (_app->last_committed_decree() + 1 == d)
        {
            err = _app->write_internal(mu);
        }
        else
        {
            ddebug(
                "%s: mutation %s commit to %s skipped, app.last_committed_decree = %" PRId64,
                name(), mu->name(),
                enum_to_string(status()),
                _app->last_committed_decree()
                );
        }
        break;
    case PS_PRIMARY:
        {
            check_state_completeness();
            dassert(_app->last_committed_decree() + 1 == d, "");
            err = _app->write_internal(mu);
        }
        break;

    case PS_SECONDARY:
        if (_secondary_states.checkpoint_task == nullptr)
        {
            check_state_completeness();
            dassert (_app->last_committed_decree() + 1 == d, "");
            err = _app->write_internal(mu);
        }
        else
        {
            ddebug(
                "%s: mutation %s commit to %s skipped, app.last_committed_decree = %" PRId64,
                name(), mu->name(),
                enum_to_string(status()),
                _app->last_committed_decree()
                );

            // make sure private log saves the state
            // catch-up will be done later after checkpoint task is fininished
            dassert(_private_log != nullptr, "");            
        }
        break;
    case PS_POTENTIAL_SECONDARY:
        if (_potential_secondary_states.learning_status == LearningSucceeded ||
            _potential_secondary_states.learning_status == LearningWithPrepareTransient)
        {
            dassert(_app->last_committed_decree() + 1 == d, "");
            err = _app->write_internal(mu);
        }
        else
        {
            // prepare also happens with LearningWithPrepare, in this case
            // make sure private log saves the state,
            // catch-up will be done later after the checkpoint task is finished

            ddebug(
                "%s: mutation %s commit to %s skipped, app.last_committed_decree = %" PRId64,
                name(), mu->name(),
                enum_to_string(status()),
                _app->last_committed_decree()
                );
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

    if (status() == PS_PRIMARY)
    {
        mutation_ptr next = _primary_states.write_queue.check_possible_work(
            static_cast<int>(_prepare_list->max_decree() - d)
            );

        if (next)
        {
            init_prepare(next);
        }
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
            || !mu->is_logged()
            )
            break;

        start++;
        lastBallot = mu->data.header.ballot;
    }
    return start;
}

void replica::close()
{
    if (nullptr != _check_timer)
    {
        _check_timer->cancel(true);
        _check_timer = nullptr;
    }

    if (status() != PS_INACTIVE && status() != PS_ERROR)
    {
        update_local_configuration_with_no_ballot_change(PS_INACTIVE);
    }

    cleanup_preparing_mutations(true);
    _primary_states.cleanup();
    _secondary_states.cleanup();
    _potential_secondary_states.cleanup(true);

    if (_private_log != nullptr)
    {
        _private_log->close();
        _private_log = nullptr;
    }

    if (_app != nullptr)
    {
        _app->close(false);
    }
}

}} // namespace
