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
 *     two-phase commit in replication
 *
 * Revision history:
 *     Mar., 2015, @imzhenyu (Zhenyu Guo), first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

#include "replica.h"
#include "mutation.h"
#include "mutation_log.h"
#include "replica_stub.h"
#include "replication_app_base.h"

# ifdef __TITLE__
# undef __TITLE__
# endif
# define __TITLE__ "replica.2pc"

namespace dsn { namespace replication {


void replica::on_client_write(task_code code, dsn_message_t request)
{
    check_hashed_access();

    if (partition_status::PS_PRIMARY != status())
    {
        response_client_message(request, ERR_INVALID_STATE);
        return;
    }

    if (static_cast<int>(_primary_states.membership.secondaries.size()) + 1 < _options->mutation_2pc_min_replica_count)
    {
        response_client_message(request, ERR_NOT_ENOUGH_MEMBER);
        return;
    }

    auto mu = _primary_states.write_queue.add_work(code, request, this);
    if (mu)
    {
        init_prepare(mu);
    }
}

void replica::init_prepare(mutation_ptr& mu)
{
    dassert (partition_status::PS_PRIMARY == status(), "");

    error_code err = ERR_OK;
    uint8_t count = 0;
            
    mu->data.header.last_committed_decree = last_committed_decree();
    if (mu->data.header.decree == invalid_decree)
    {
        mu->set_id(get_ballot(), _prepare_list->max_decree() + 1);
    }
    else
    {
        mu->set_id(get_ballot(), mu->data.header.decree);
    }
    
    dinfo("%s: mutation %s init_prepare, mutation_tid=%" PRIu64, name(), mu->name(), mu->tid());

    // check bounded staleness
    if (mu->data.header.decree > last_committed_decree() + _options->staleness_for_commit)
    {
        err = ERR_CAPACITY_EXCEEDED;
        goto ErrOut;
    }
 
    dassert (mu->data.header.decree > last_committed_decree(), "");

    // local prepare
    err = _prepare_list->prepare(mu, partition_status::PS_PRIMARY);
    if (err != ERR_OK)
    {
        goto ErrOut;
    }
    
    // remote prepare
    mu->set_prepare_ts();
    mu->set_left_secondary_ack_count((unsigned int)_primary_states.membership.secondaries.size());
    for (auto it = _primary_states.membership.secondaries.begin(); it != _primary_states.membership.secondaries.end(); ++it)
    {
        send_prepare_message(*it, partition_status::PS_SECONDARY, mu, _options->prepare_timeout_ms_for_secondaries);
    }

    count = 0;
    for (auto it = _primary_states.learners.begin(); it != _primary_states.learners.end(); ++it)
    {
        if (it->second.prepare_start_decree != invalid_decree && mu->data.header.decree >= it->second.prepare_start_decree)
        {
            send_prepare_message(it->first, partition_status::PS_POTENTIAL_SECONDARY, mu, _options->prepare_timeout_ms_for_potential_secondaries, it->second.signature);
            count++;
        }
    }    
    mu->set_left_potential_secondary_ack_count(count);

    if (mu->is_logged())
    {
        do_possible_commit_on_primary(mu);
    }
    else
    {
        dassert(mu->data.header.log_offset == invalid_offset, "");
        dassert(mu->log_task() == nullptr, "");

        mu->log_task() = _stub->_log->append(mu,
            LPC_WRITE_REPLICATION_LOG,
            this,
            std::bind(&replica::on_append_log_completed, this, mu,
                      std::placeholders::_1,
                      std::placeholders::_2),
                      gpid_to_hash(get_gpid())
            );

        dassert(nullptr != mu->log_task(), "");
    }
    return;

ErrOut:
    for (auto& r : mu->client_requests)
    {
        response_client_message(r, err);
    }
    return;
}

void replica::send_prepare_message(
    ::dsn::rpc_address addr, 
    partition_status::type status,
    mutation_ptr& mu, 
    int timeout_milliseconds,
    int64_t learn_signature)
{
    dsn_message_t msg = dsn_msg_create_request(RPC_PREPARE, timeout_milliseconds, gpid_to_hash(get_gpid()));
    replica_configuration rconfig;
    _primary_states.get_replica_config(status, rconfig, learn_signature);

    {
        rpc_write_stream writer(msg);
        marshall(writer, get_gpid(), DSF_THRIFT_BINARY);
        marshall(writer, rconfig, DSF_THRIFT_BINARY);
        mu->write_to(writer);
    }
    
    mu->remote_tasks()[addr] = rpc::call(addr, msg,
        this,
        [=](error_code err, dsn_message_t request, dsn_message_t reply)
        {
            on_prepare_reply(std::make_pair(mu, rconfig.status), err, request, reply);
        },
        gpid_to_hash(get_gpid())
        );

    ddebug( 
        "%s: mutation %s send_prepare_message to %s as %s",  
        name(), mu->name(),
        addr.to_string(),
        enum_to_string(rconfig.status)
        );
}

void replica::do_possible_commit_on_primary(mutation_ptr& mu)
{
    dassert (_config.ballot == mu->data.header.ballot, "");
    dassert (partition_status::PS_PRIMARY == status(), "");

    if (mu->is_ready_for_commit())
    {
        _prepare_list->commit(mu->data.header.decree, COMMIT_ALL_READY);
    }
}

void replica::on_prepare(dsn_message_t request)
{
    check_hashed_access();

    replica_configuration rconfig;
    mutation_ptr mu;

    {
        rpc_read_stream reader(request);
        unmarshall(reader, rconfig, DSF_THRIFT_BINARY);
        mu = mutation::read_from(reader, request);
    }

    decree decree = mu->data.header.decree;

    dinfo("%s: mutation %s on_prepare", name(), mu->name());

    dassert(mu->data.header.ballot == rconfig.ballot, "");

    if (mu->data.header.ballot < get_ballot())
    {
        derror("%s: mutation %s on_prepare skipped due to old view", name(), mu->name());
        // no need response because the rpc should have been cancelled on primary in this case
        return;
    }

    // update configuration when necessary
    else if (rconfig.ballot > get_ballot())
    {
        if (!update_local_configuration(rconfig))
        {
            derror(
                "%s: mutation %s on_prepare failed as update local configuration failed, state = %s",
                name(), mu->name(),
                enum_to_string(status())
                );
            ack_prepare_message(ERR_INVALID_STATE, mu);
            return;
        }
    }

    if (partition_status::PS_INACTIVE == status() || partition_status::PS_ERROR == status())
    {
        derror(
            "%s: mutation %s on_prepare failed as invalid replica state, state = %s",
            name(), mu->name(),
            enum_to_string(status())
            );
        ack_prepare_message(
            (partition_status::PS_INACTIVE == status() && _inactive_is_transient) ? ERR_INACTIVE_STATE : ERR_INVALID_STATE,
            mu
            );
        return;
    }
    else if (partition_status::PS_POTENTIAL_SECONDARY == status())
    {
        // new learning process
        if (rconfig.learner_signature != _potential_secondary_states.learning_signature)
        {
            init_learn(rconfig.learner_signature);
            // no need response as rpc is already gone
            return;
        }

        if (!(_potential_secondary_states.learning_status == learner_status::LearningWithPrepare
            || _potential_secondary_states.learning_status == learner_status::LearningSucceeded))
        {
            derror(
                "%s: mutation %s on_prepare skipped as invalid learning status, state = %s, learning_status = %s",
                name(), mu->name(),
                enum_to_string(status()),
                enum_to_string(_potential_secondary_states.learning_status)
                );

            // no need response as rpc is already gone
            return;
        }
    }

    dassert (rconfig.status == status(), "");    
    if (decree <= last_committed_decree())
    {
        ack_prepare_message(ERR_OK, mu);
        return;
    }
    
    // real prepare start
    auto mu2 = _prepare_list->get_mutation_by_decree(decree);
    if (mu2 != nullptr && mu2->data.header.ballot == mu->data.header.ballot)
    {
        if (mu2->is_logged())
        {
            ack_prepare_message(ERR_OK, mu);
        }
        else
        {
            derror("%s: mutation %s on_prepare skipped as it is duplicate", name(), mu->name());
            // response will be unnecessary when we add retry logic in rpc engine.
            // the retried rpc will use the same id therefore it will be considered responsed
            // even the response is for a previous try.
        }
        return;
    }

    error_code err = _prepare_list->prepare(mu, status());
    dassert (err == ERR_OK, "");

    if (partition_status::PS_POTENTIAL_SECONDARY == status())
    {
        dassert (mu->data.header.decree <= last_committed_decree() + _options->max_mutation_count_in_prepare_list, "");
    }
    else
    {
        dassert (partition_status::PS_SECONDARY == status(), "");
        dassert (mu->data.header.decree <= last_committed_decree() + _options->staleness_for_commit, "");
    }

    dassert(mu->log_task() == nullptr, "");
    mu->log_task() = _stub->_log->append(mu,
        LPC_WRITE_REPLICATION_LOG,
        this,
        std::bind(&replica::on_append_log_completed, this, mu,
                  std::placeholders::_1,
                  std::placeholders::_2),
        gpid_to_hash(get_gpid())
        );
}

void replica::on_append_log_completed(mutation_ptr& mu, error_code err, size_t size)
{
    check_hashed_access();

    dinfo("%s: append shared log completed for mutation %s, size = %u, err = %s",
          name(), mu->name(), size, err.to_string());

    if (err == ERR_OK)
    {
        mu->set_logged();
    }
    else
    {
        derror("%s: append shared log failed for mutation %s, err = %s",
               name(), mu->name(), err.to_string());
    }

    // skip old mutations
    if (mu->data.header.ballot >= get_ballot() && status() != partition_status::PS_INACTIVE)
    {
        switch (status())
        {
        case partition_status::PS_PRIMARY:
            if (err == ERR_OK)
            {
                do_possible_commit_on_primary(mu);
            }
            else
            {
                handle_local_failure(err);
            }
            break;
        case partition_status::PS_SECONDARY:
        case partition_status::PS_POTENTIAL_SECONDARY:
            if (err != ERR_OK)
            {
                handle_local_failure(err);
            }
            // always ack
            ack_prepare_message(err, mu);
            break;
        case partition_status::PS_ERROR:
            break;
        default:
            dassert(false, "");
            break;
        }
    }

    if (err != ERR_OK)
    {
        // mutation log failure, propagate to all replicas
        _stub->handle_log_failure(err);
    }
   
    // write local private log if necessary
    if (err == ERR_OK && _private_log && status() != partition_status::PS_ERROR)
    {
        _private_log->append(mu,
            LPC_WRITE_REPLICATION_LOG,
            nullptr,
            [this, mu](error_code err, size_t size)
            {
                //
                // DO NOT CHANGE THIS CALLBACK HERE UNLESS
                // YOU FULLY UNDERSTAND WHAT WE DO HERE
                // 
                // AS PRIVATE LOG IS BATCHED, WE ONLY EXECUTE
                // THE FIRST CALLBACK IF THERE IS FAILURE TO
                // NOTIFY FAILURE. ALL OTHER TASKS ARE SIMPLY
                // CANCELLED!!!
                //
                // TODO: we do not need so many callbacks
                //

                dinfo("%s: append private log completed for mutation %s, size = %u, err = %s",
                      name(), mu->name(), size, err.to_string());

                if (err != ERR_OK)
                {
                    derror("%s: append private log failed for mutation %s, err = %s",
                           name(), mu->name(), err.to_string());
                    handle_local_failure(err);
                }
            },
            gpid_to_hash(get_gpid())
            );
    }
}

void replica::on_prepare_reply(std::pair<mutation_ptr, partition_status::type> pr, error_code err, dsn_message_t request, dsn_message_t reply)
{
    check_hashed_access();

    mutation_ptr mu = pr.first;
    partition_status::type targetStatus = pr.second;

    // skip callback for old mutations
    if (mu->data.header.ballot < get_ballot() || partition_status::PS_PRIMARY != status())
        return;
    
    dassert (mu->data.header.ballot == get_ballot(), "");

    ::dsn::rpc_address node = dsn_msg_to_address(request);
    partition_status::type st = _primary_states.get_node_status(node);

    // handle reply
    prepare_ack resp;

    // handle error
    if (err != ERR_OK)
    {
        resp.err = err;
    }
    else
    {
        ::dsn::unmarshall(reply, resp);
    }
    
    ddebug(
        "%s: mutation %s on_prepare_reply from %s, err = %s",
        name(), mu->name(),
        node.to_string(),
        resp.err.to_string()
        );
       
    if (resp.err == ERR_OK)
    {
        dassert (resp.ballot == get_ballot(), "");
        dassert (resp.decree == mu->data.header.decree, "");

        switch (targetStatus)
        {
        case partition_status::PS_SECONDARY:
            dassert (_primary_states.check_exist(node, partition_status::PS_SECONDARY), "");
            dassert (mu->left_secondary_ack_count() > 0, "");
            if (0 == mu->decrease_left_secondary_ack_count())
            {
                do_possible_commit_on_primary(mu);
            }
            break;
        case partition_status::PS_POTENTIAL_SECONDARY:
            dassert (mu->left_potential_secondary_ack_count() > 0, "");
            if (0 == mu->decrease_left_potential_secondary_ack_count())
            {
                do_possible_commit_on_primary(mu);
            }
            break;
        default:
            dwarn(
                "%s: mutation %s prepare ack skipped coz the node is now inactive", name(), mu->name()
                );
            break;
        }
    }

    // failure handling
    else
    {
        // retry for INACTIVE state when there are still time
        if (resp.err == ERR_INACTIVE_STATE
            && !mu->is_prepare_close_to_timeout(2, targetStatus == partition_status::PS_SECONDARY ?
            _options->prepare_timeout_ms_for_secondaries :
            _options->prepare_timeout_ms_for_potential_secondaries)
            )
        {
            send_prepare_message(node, targetStatus, mu, targetStatus == partition_status::PS_SECONDARY ?
                _options->prepare_timeout_ms_for_secondaries :
                _options->prepare_timeout_ms_for_potential_secondaries);
            return;
        }

        // make sure this is before any later commit ops
        // because now commit ops may lead to new prepare ops
        // due to replication throttling
        handle_remote_failure(st, node, resp.err);

        // note targetStatus and (curent) status may diff
        if (targetStatus == partition_status::PS_POTENTIAL_SECONDARY)
        {
            dassert (mu->left_potential_secondary_ack_count() > 0, "");
            if (0 == mu->decrease_left_potential_secondary_ack_count())
            {
                do_possible_commit_on_primary(mu);
            }
        }
    }
}

void replica::ack_prepare_message(error_code err, mutation_ptr& mu)
{
    prepare_ack resp;
    resp.pid = get_gpid();
    resp.err = err;
    resp.ballot = get_ballot();
    resp.decree = mu->data.header.decree;

    // for partition_status::PS_POTENTIAL_SECONDARY ONLY
    resp.last_committed_decree_in_app = _app->last_committed_decree(); 
    resp.last_committed_decree_in_prepare_list = last_committed_decree();

    dassert(nullptr != mu->prepare_msg(), "");
    reply(mu->prepare_msg(), resp);

    ddebug("%s: mutation %s ack_prepare_message, err = %s", name(), mu->name(), err.to_string());
}

void replica::cleanup_preparing_mutations(bool wait)
{
    decree start = last_committed_decree() + 1;
    decree end = _prepare_list->max_decree();

    for (decree decree = start; decree <= end; decree++)
    {
        mutation_ptr mu = _prepare_list->get_mutation_by_decree(decree);
        if (mu != nullptr)
        {
            mu->clear_prepare_or_commit_tasks();

            //
            // make sure the buffers from mutations are valid for underlying aio
            //
            if (wait) {
                _stub->_log->flush();
                mu->wait_log_task();
            }
        }
    }
}

}} // namespace
