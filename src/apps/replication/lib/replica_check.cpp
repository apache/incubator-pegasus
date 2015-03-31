/*
 * The MIT License (MIT)

 * Copyright (c) 2015 Microsoft Corporation, Robust Distributed System Nucleus(rDSN)

 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:

 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.

 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
#include "replica.h"
#include "replication_app_base.h"
#include "mutation.h"
#include "mutation_log.h"
#include "replica_stub.h"

#define __TITLE__ "GroupCheck"

namespace dsn { namespace replication {

void replica::init_group_check()
{
    if (PS_PRIMARY != status() || _options.GroupCheckDisabled)
        return;

    dassert (nullptr == _primary_states.GroupCheckTask, "");
    _primary_states.GroupCheckTask = tasking::enqueue(
            LPC_GROUP_CHECK,
            this,
            &replica::broadcast_group_check,
            gpid_to_hash(get_gpid()),
            0,
            _options.GroupCheckIntervalMs
            );
}

void replica::broadcast_group_check()
{
    dassert (nullptr != _primary_states.GroupCheckTask, "");
    if (_primary_states.GroupCheckPendingReplies.size() > 0)
    {
        dwarn(
            "%s: %u group check replies are still pending when doing next round check",
            name(), static_cast<int>(_primary_states.GroupCheckPendingReplies.size())
            );

        for (auto it = _primary_states.GroupCheckPendingReplies.begin(); it != _primary_states.GroupCheckPendingReplies.end(); it++)
        {
            it->second->cancel(true);
        }
        _primary_states.GroupCheckPendingReplies.clear();
    }

    for (auto it = _primary_states.Statuses.begin(); it != _primary_states.Statuses.end(); it++)
    {
        if (it->first == address())
            continue;

        end_point addr = it->first;
        std::shared_ptr<group_check_request> request(new group_check_request);

        request->app_type = _primary_states.membership.app_type;
        request->node = addr;
        _primary_states.GetReplicaConfig(addr, request->config);
        request->lastCommittedDecree = last_committed_decree();
        request->learnerSignature = 0;
        if (it->second == PS_POTENTIAL_SECONDARY)
        {
            auto it2 = _primary_states.Learners.find(it->first);
            dassert (it2 != _primary_states.Learners.end(), "");
            request->learnerSignature = it2->second.signature;
        }

        task_ptr caller_tsk = rpc::call_typed(
            addr,
            RPC_GROUP_CHECK,
            request,            
            this,
            &replica::on_group_check_reply,
            gpid_to_hash(get_gpid()),
            _options.GroupCheckTimeoutMs
            );

        _primary_states.GroupCheckPendingReplies[addr] = caller_tsk;

        ddebug(
            "%s: init_group_check for %s:%u", name(), addr.name.c_str(), addr.port
        );
    }
}

void replica::on_group_check(const group_check_request& request, __out_param group_check_response& response)
{
    ddebug(
        "%s: on_group_check from %s:%u",
        name(), request.config.primary.name.c_str(), request.config.primary.port
        );
    
    if (request.config.ballot < get_ballot())
    {
        response.err = ERR_VERSION_OUTDATED;
        return;
    }
    else if (request.config.ballot > get_ballot())
    {
        update_local_configuration(request.config);
    }
    else if (is_same_ballot_status_change_allowed(status(), request.config.status))
    {
        update_local_configuration(request.config, true);
    }
    
    switch (status())
    {
    case PS_INACTIVE:
        break;
    case PS_SECONDARY:
        if (request.lastCommittedDecree > last_committed_decree())
        {
            _prepare_list->commit(request.lastCommittedDecree, true);
        }
        break;
    case PS_POTENTIAL_SECONDARY:
        init_learn(request.learnerSignature);
        break;
    case PS_ERROR:
        break;
    default:
        dassert (false, "");
    }
    
    response.gpid = get_gpid();
    response.node = address();
    response.err = ERR_SUCCESS;
    if (status() == PS_ERROR)
    {
        response.err = ERR_INVALID_STATE;
    }

    response.lastCommittedDecreeInApp = _app->last_committed_decree();
    response.lastCommittedDecreeInPrepareList = last_committed_decree();
    response.learnerState = _potential_secondary_states.LearningState;
    response.learnerSignature = _potential_secondary_states.LearningSignature;
}

void replica::on_group_check_reply(error_code err, std::shared_ptr<group_check_request> req, std::shared_ptr<group_check_response> resp)
{
    if (PS_PRIMARY != status() || req->config.ballot < get_ballot())
    {
        return;
    }

    auto r = _primary_states.GroupCheckPendingReplies.erase(req->node);
    dassert (r == 1, "");

    if (err)
    {
        handle_remote_failure(req->config.status, req->node, err);
    }
    else
    {
        if (resp->err == ERR_SUCCESS)
        {
            if (resp->learnerState == LearningSucceeded && req->config.status == PS_POTENTIAL_SECONDARY)
            {
                handle_learning_succeeded_on_primary(req->node, resp->learnerSignature);
            }
        }
        else
        {
            handle_remote_failure(req->config.status, req->node, resp->err);
        }
    }
}

// for testing purpose only
void replica::send_group_check_once_for_test(int delay_milliseconds)
{
    dassert (_options.GroupCheckDisabled, "");

    _primary_states.GroupCheckTask = tasking::enqueue(
            LPC_GROUP_CHECK,
            this,
            &replica::broadcast_group_check,
            gpid_to_hash(get_gpid()),
            delay_milliseconds
            );
}

}} // end namepspace
