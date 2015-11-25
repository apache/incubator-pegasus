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
 *     What is this file about?
 *
 * Revision history:
 *     xxxx-xx-xx, author, first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

#include "replica_context.h"

# ifdef __TITLE__
# undef __TITLE__
# endif
# define __TITLE__ "replica.context"

namespace dsn { namespace replication {

void primary_context::cleanup(bool clean_pending_mutations)
{
    do_cleanup_pending_mutations(clean_pending_mutations);

    // clean up group check
    if (nullptr != group_check_task)
    {
        group_check_task->cancel(true);
        group_check_task = nullptr;
    }

    for (auto it = group_check_pending_replies.begin(); it != group_check_pending_replies.end(); it++)
    {
        it->second->cancel(true);
    }
    group_check_pending_replies.clear();

    // clean up reconfiguration
    if (nullptr != reconfiguration_task)
    {
        reconfiguration_task->cancel(true);
        reconfiguration_task = nullptr;
    }
}

void primary_context::do_cleanup_pending_mutations(bool clean_pending_mutations)
{
    if (clean_pending_mutations)
    {
        write_queue.clear();
    }
}

void primary_context::reset_membership(const partition_configuration& config, bool clear_learners)
{
    statuses.clear();
    if (clear_learners)
    {
        learners.clear();
    }

    membership = config;

    if (membership.primary.is_invalid() == false)
    {
        statuses[membership.primary] = PS_PRIMARY;
    }

    for (auto it = config.secondaries.begin(); it != config.secondaries.end(); it++)
    {
        statuses[*it] = PS_SECONDARY;
        learners.erase(*it);
    }

    for (auto it = learners.begin(); it != learners.end(); it++)
    {
        statuses[it->first] = PS_POTENTIAL_SECONDARY;
    }
}

bool primary_context::get_replica_config(::dsn::rpc_address node, /*out*/ replica_configuration& config)
{
    config.gpid = membership.gpid;
    config.primary = membership.primary;  
    config.ballot = membership.ballot;

    auto it = statuses.find(node);
    if (it != statuses.end())
    {
        config.status = it->second;
        return true;
    }
    else
    {
        config.status = PS_INACTIVE;
        return false;
    }
}


void primary_context::get_replica_config(partition_status st, /*out*/ replica_configuration& config)
{
    config.gpid = membership.gpid;
    config.primary = membership.primary;  
    config.ballot = membership.ballot;
    config.status = st;
}

bool primary_context::check_exist(::dsn::rpc_address node, partition_status st)
{
    switch (st)
    {
    case PS_PRIMARY:
        return membership.primary == node;
    case PS_SECONDARY:
        return std::find(membership.secondaries.begin(), membership.secondaries.end(), node) != membership.secondaries.end();
    case PS_POTENTIAL_SECONDARY:
        return learners.find(node) != learners.end();
    default:
        dassert (false, "");
        return false;
    }
}

void secondary_context::cleanup()
{
    if (nullptr != checkpoint_task)
    {
        checkpoint_task->cancel(true);
        checkpoint_task = nullptr;
    }
}

bool potential_secondary_context::cleanup(bool force)
{
    if (learn_remote_files_task != nullptr)
    {
        bool clean_remote_learning;
        learn_remote_files_task->cancel(false, &clean_remote_learning);
        if (force)
        {
            learn_remote_files_task->cancel(true);
        }
        else if (!clean_remote_learning)
        {
            return false;
        }
    }

    if (learning_task != nullptr)
    {
        learning_task->cancel(true);
    }

    if (learn_remote_files_completed_task != nullptr)
    {
        learn_remote_files_completed_task->cancel(true);
    }

    learning_signature = 0;
    learning_round_is_running = false;
    learning_start_prepare_decree = invalid_decree;
    return true;
}

}} // end namespace
